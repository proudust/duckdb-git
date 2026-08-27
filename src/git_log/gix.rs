use crate::git::backend::gix::{
    collect_refs, emit_inspected_commit, emit_prefetch_item, prepare_walk,
    run_prefetch_commit_walk, start_commit_date_walk, walk_next_oid, CachedRepo, InspectedCommit,
    PrefetchItem, WalkPrepared,
};
use crate::git::date_walk::{CommitDateWalk, OidBytes};
use crate::git::meta_proj::MetaProjection;
use crate::git::sink::{oid_hex, CommitSink};
use crate::git_log::params::GitLogParameter;
use crate::git_log::prefetch::{
    fixed_max_threads, PrefetchBuffer, READ_BATCH_SIZE, RING_CAPACITY,
};
use crate::git_log::schema;
use crate::git_log::vector::VectorInserter;
use duckdb::core::DataChunkHandle;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
#[cfg(feature = "git-log-stats")]
use std::time::Instant;

enum ScanEngine {
    Prefetch { buffer: PrefetchBuffer<PrefetchItem> },
    Inline {
        state: Mutex<InlineState>,
        first_parent: bool,
        batch_size: usize,
        proj: MetaProjection,
    },
}

enum InlineState {
    Pending(WalkPrepared),
    Active {
        walk: CommitDateWalk,
        cache: Option<HashMap<OidBytes, InspectedCommit>>,
    },
    Done,
}

struct GixLogScannerInner {
    engine: ScanEngine,
    decorations: HashMap<gix::ObjectId, Vec<String>>,
    max_threads: u64,
    repo_path: String,
}

pub struct GixLogScanner {
    inner: Arc<GixLogScannerInner>,
}

fn bytes_to_oid(bytes: OidBytes) -> gix::ObjectId {
    gix::ObjectId::from(bytes)
}

impl GixLogScanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        if params.ignore_all_space {
            return Err("ignore_all_space=true is not supported with the gix backend".into());
        }

        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();

        let prep = prepare_walk(
            repo,
            params.revision.as_deref(),
            params.max_count,
            params.first_parent,
            params.all_refs,
        )?;
        let decorations = if schema::needs_refs(column_indices) {
            collect_refs(repo, params.decorate)?
        } else {
            HashMap::new()
        };

        let needs_fc = schema::needs_file_changes(column_indices);
        let max_threads = fixed_max_threads(false, needs_fc);
        let proj = schema::meta_projection(column_indices);

        // Inline skips emit_commit / file_changes; only use it for metadata-only scans.
        // When file_changes is projected, keep Prefetch even if max_threads == 1 (e.g. 1 core).
        let engine = if !needs_fc {
            #[cfg(feature = "git-log-stats")]
            crate::git::diag::reset_git_log_stats();
            let first_parent = prep.first_parent;
            ScanEngine::Inline {
                state: Mutex::new(InlineState::Pending(prep)),
                first_parent,
                batch_size: READ_BATCH_SIZE,
                proj,
            }
        } else {
            let buffer = PrefetchBuffer::new(RING_CAPACITY);
            let producer = buffer.producer();
            let repo_path_owned = repo_path.to_string();
            let diff_merges = params.diff_merges;

            let walker = std::thread::spawn(move || {
                let result = (|| -> Result<(), String> {
                    let handle = CachedRepo::open(&repo_path_owned).map_err(|e| e.to_string())?;
                    let repo = handle.repo();
                    #[cfg(feature = "git-log-stats")]
                    {
                        crate::git::diag::record_walker_identity(
                            crate::git::diag::thread_id_bits(),
                            repo as *const _ as usize,
                        );
                    }
                    #[cfg(feature = "git-log-stats")]
                    let walk_t = Instant::now();
                    let walk_result = run_prefetch_commit_walk(
                        repo,
                        prep,
                        proj,
                        true,
                        diff_merges,
                        |item| {
                            if !producer.push(item) {
                                return Ok(false);
                            }
                            Ok(true)
                        },
                    );
                    #[cfg(feature = "git-log-stats")]
                    crate::git::diag::record_walk(walk_t.elapsed());
                    walk_result
                })();
                match result {
                    Ok(()) => producer.mark_finished(),
                    Err(e) => producer.set_error(e),
                }
            });
            buffer.attach_walker(walker);
            ScanEngine::Prefetch { buffer }
        };

        Ok(GixLogScanner {
            inner: Arc::new(GixLogScannerInner {
                engine,
                decorations,
                max_threads,
                repo_path: repo_path.to_string(),
            }),
        })
    }

    pub fn max_threads(&self) -> u64 {
        self.inner.max_threads
    }

    pub fn read(
        &self,
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        match &self.inner.engine {
            ScanEngine::Prefetch { buffer } => {
                self.read_prefetch(buffer, params, output, column_indices)
            }
            ScanEngine::Inline {
                state,
                first_parent,
                batch_size,
                proj,
            } => self.read_inline(
                state,
                *first_parent,
                *batch_size,
                *proj,
                params,
                output,
                column_indices,
            ),
        }
    }

    fn read_prefetch(
        &self,
        buffer: &PrefetchBuffer<PrefetchItem>,
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        #[cfg(feature = "git-log-stats")]
        let read_t = Instant::now();
        let batch = buffer
            .take_batch(READ_BATCH_SIZE)
            .map_err(|e| -> Box<dyn Error> { e.into() })?;
        if batch.is_empty() {
            #[cfg(feature = "git-log-stats")]
            crate::git::diag::record_read(read_t.elapsed());
            return Ok(0);
        }

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();
        #[cfg(feature = "git-log-stats")]
        {
            crate::git::diag::record_read_identity(
                crate::git::diag::thread_id_bits(),
                repo as *const _ as usize,
            );
        }

        let count = self.emit_batch(repo, &batch, params, output, column_indices)?;
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::record_read(read_t.elapsed());
        Ok(count)
    }

    fn read_inline(
        &self,
        state: &Mutex<InlineState>,
        first_parent: bool,
        batch_size: usize,
        proj: MetaProjection,
        _params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        #[cfg(feature = "git-log-stats")]
        let read_t = Instant::now();

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();
        #[cfg(feature = "git-log-stats")]
        {
            crate::git::diag::record_walker_identity(
                crate::git::diag::thread_id_bits(),
                repo as *const _ as usize,
            );
            crate::git::diag::record_read_identity(
                crate::git::diag::thread_id_bits(),
                repo as *const _ as usize,
            );
        }

        let mut guard = state.lock().unwrap();
        match &mut *guard {
            InlineState::Done => {
                #[cfg(feature = "git-log-stats")]
                crate::git::diag::record_read(read_t.elapsed());
                return Ok(0);
            }
            InlineState::Pending(_) => {
                let pending = std::mem::replace(&mut *guard, InlineState::Done);
                let InlineState::Pending(prep) = pending else {
                    unreachable!("Pending branch");
                };
                let mut cache = proj.needs_emit_cache().then(HashMap::new);
                #[cfg(feature = "git-log-stats")]
                let walk_t = Instant::now();
                match start_commit_date_walk(repo, prep.clone(), proj, cache.as_mut()) {
                    Ok(walk) => {
                        #[cfg(feature = "git-log-stats")]
                        crate::git::diag::record_walk(walk_t.elapsed());
                        *guard = InlineState::Active { walk, cache };
                    }
                    Err(e) => {
                        *guard = InlineState::Pending(prep);
                        return Err(e);
                    }
                }
            }
            InlineState::Active { .. } => {}
        }

        let InlineState::Active { walk, cache } = &mut *guard else {
            unreachable!("Active after Pending start or prior Active");
        };

        let mut writer = VectorInserter::new(output, column_indices);
        let empty_refs: Vec<String> = Vec::new();
        let mut count = 0u32;
        let mut exhausted = false;
        while (count as usize) < batch_size {
            #[cfg(feature = "git-log-stats")]
            let walk_step_t = Instant::now();
            match walk_next_oid(repo, first_parent, proj, walk, cache.as_mut())? {
                Some(oid_bytes) => {
                    #[cfg(feature = "git-log-stats")]
                    crate::git::diag::record_walk(walk_step_t.elapsed());
                    let oid = bytes_to_oid(oid_bytes);
                    writer.begin_row(count as usize);
                    #[cfg(feature = "git-log-stats")]
                    let emit_step_t = Instant::now();
                    if let Some(cache) = cache.as_mut() {
                        let meta = cache.remove(&oid_bytes).ok_or_else(|| {
                            format!("inline emit cache miss for {oid}")
                        })?;
                        emit_inspected_commit(oid, &meta, &mut writer);
                    } else if proj.commit_id {
                        let hex = oid_hex(oid.as_bytes());
                        writer.commit_id(&hex);
                    }

                    let refs = self.inner.decorations.get(&oid).unwrap_or(&empty_refs);
                    writer.begin_decorate(refs.len());
                    for name in refs {
                        writer.decorate_name(name);
                    }
                    writer.finish_row();
                    #[cfg(feature = "git-log-stats")]
                    crate::git::diag::record_emit(emit_step_t.elapsed());
                    count += 1;
                }
                None => {
                    #[cfg(feature = "git-log-stats")]
                    crate::git::diag::record_walk(walk_step_t.elapsed());
                    exhausted = true;
                    break;
                }
            }
        }
        if exhausted {
            *guard = InlineState::Done;
        }
        drop(guard);

        if count == 0 {
            #[cfg(feature = "git-log-stats")]
            crate::git::diag::record_read(read_t.elapsed());
            #[cfg(feature = "git-log-stats")]
            if exhausted {
                crate::git::diag::dump_git_log_stats_if_env();
            }
            return Ok(0);
        }

        writer.finish();
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::record_read(read_t.elapsed());
        #[cfg(feature = "git-log-stats")]
        if exhausted {
            crate::git::diag::dump_git_log_stats_if_env();
        }
        Ok(count)
    }

    fn emit_batch(
        &self,
        repo: &gix::Repository,
        batch: &[PrefetchItem],
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let mut writer = VectorInserter::new(output, column_indices);
        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        #[cfg(feature = "git-log-stats")]
        let emit_t = Instant::now();
        for (batch_idx, item) in batch.iter().enumerate() {
            let oid = bytes_to_oid(item.oid);
            writer.begin_row(batch_idx);
            emit_prefetch_item(
                repo,
                item,
                skip_file_changes,
                params.diff_merges,
                &mut writer,
            )?;

            let refs = self.inner.decorations.get(&oid).unwrap_or(&empty_refs);
            writer.begin_decorate(refs.len());
            for name in refs {
                writer.decorate_name(name);
            }

            writer.finish_row();
        }

        writer.finish();
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::record_emit(emit_t.elapsed());
        Ok(batch.len() as u32)
    }
}

use crate::git::backend::libgit::{
    collect_refs, emit_inspected_commit, emit_prefetch_item, prepare_walk,
    run_prefetch_commit_walk, start_commit_date_walk, walk_next_oid, BlobRing, CachedRepo,
    EmitOpts, InspectedCommit, PrefetchItem, WalkPrepared,
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
use git2::Oid;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
#[cfg(feature = "prefetch-stats")]
use std::time::Instant;

enum ScanEngine {
    /// Parallel emit: walker thread feeds a bounded PrefetchItem ring.
    Prefetch { buffer: PrefetchBuffer<PrefetchItem> },
    /// Single DuckDB worker: walk + emit share one thread-local Repository.
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
        /// Present only when projected columns need retained inspect metadata.
        cache: Option<HashMap<OidBytes, InspectedCommit>>,
    },
    Done,
}

struct LibGitLogScannerInner {
    engine: ScanEngine,
    decorations: HashMap<Oid, Vec<String>>,
    max_threads: u64,
    repo_path: String,
}

pub struct LibGitLogScanner {
    inner: Arc<LibGitLogScannerInner>,
}

fn bytes_to_oid(bytes: OidBytes) -> Oid {
    Oid::from_bytes(&bytes).expect("20-byte sha1")
}

impl LibGitLogScanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
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
        let max_threads = fixed_max_threads(true, needs_fc);
        let proj = schema::meta_projection(column_indices);

        // Inline skips emit_commit / file_changes; only use it for metadata-only scans.
        // When file_changes is projected, keep Prefetch even if max_threads == 1 (e.g. 1 core).
        let engine = if !needs_fc {
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
                    #[cfg(feature = "prefetch-stats")]
                    {
                        crate::git::diag::record_walker_identity(
                            crate::git::diag::thread_id_bits(),
                            repo as *const _ as usize,
                        );
                    }
                    #[cfg(feature = "prefetch-stats")]
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
                    #[cfg(feature = "prefetch-stats")]
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

        Ok(LibGitLogScanner {
            inner: Arc::new(LibGitLogScannerInner {
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
        #[cfg(feature = "prefetch-stats")]
        let read_t = Instant::now();
        let batch = buffer
            .take_batch(READ_BATCH_SIZE)
            .map_err(|e| -> Box<dyn Error> { e.into() })?;
        if batch.is_empty() {
            #[cfg(feature = "prefetch-stats")]
            crate::git::diag::record_read(read_t.elapsed());
            return Ok(0);
        }

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();
        #[cfg(feature = "prefetch-stats")]
        {
            crate::git::diag::record_read_identity(
                crate::git::diag::thread_id_bits(),
                repo as *const _ as usize,
            );
        }

        let count = self.emit_batch(repo, &batch, params, output, column_indices)?;
        #[cfg(feature = "prefetch-stats")]
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
        #[cfg(feature = "prefetch-stats")]
        let read_t = Instant::now();

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();
        #[cfg(feature = "prefetch-stats")]
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
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_read(read_t.elapsed());
                return Ok(0);
            }
            InlineState::Pending(_) => {
                let pending = std::mem::replace(&mut *guard, InlineState::Done);
                let InlineState::Pending(prep) = pending else {
                    unreachable!("Pending branch");
                };
                let mut cache = proj.needs_emit_cache().then(HashMap::new);
                #[cfg(feature = "prefetch-stats")]
                let walk_t = Instant::now();
                match start_commit_date_walk(repo, prep.clone(), proj, cache.as_mut()) {
                    Ok(walk) => {
                        #[cfg(feature = "prefetch-stats")]
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
        #[cfg(feature = "prefetch-stats")]
        let walk_t = Instant::now();
        #[cfg(feature = "prefetch-stats")]
        let emit_t = Instant::now();
        while (count as usize) < batch_size {
            match walk_next_oid(repo, first_parent, proj, walk, cache.as_mut())? {
                Some(oid_bytes) => {
                    let oid = bytes_to_oid(oid_bytes);
                    writer.begin_row(count as usize);
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
                    count += 1;
                }
                None => {
                    exhausted = true;
                    break;
                }
            }
        }
        #[cfg(feature = "prefetch-stats")]
        {
            crate::git::diag::record_walk(walk_t.elapsed());
            crate::git::diag::record_emit(emit_t.elapsed());
        }
        if exhausted {
            *guard = InlineState::Done;
        }
        drop(guard);

        if count == 0 {
            #[cfg(feature = "prefetch-stats")]
            crate::git::diag::record_read(read_t.elapsed());
            return Ok(0);
        }

        writer.finish();
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_read(read_t.elapsed());
        Ok(count)
    }

    fn emit_batch(
        &self,
        repo: &git2::Repository,
        batch: &[PrefetchItem],
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let mut writer = VectorInserter::new(output, column_indices);
        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let mut ring = BlobRing::new();
        #[cfg(feature = "prefetch-stats")]
        let emit_t = Instant::now();
        let result = (|| {
            for (batch_idx, item) in batch.iter().enumerate() {
                let oid = bytes_to_oid(item.oid);
                writer.begin_row(batch_idx);
                emit_prefetch_item(
                    repo,
                    item,
                    &EmitOpts {
                        ignore_all_space: params.ignore_all_space,
                        skip_file_changes,
                        diff_merges: params.diff_merges,
                        rename_threshold: params.rename_threshold,
                    },
                    &mut writer,
                    &mut ring,
                )?;

                let refs = self.inner.decorations.get(&oid).unwrap_or(&empty_refs);
                writer.begin_decorate(refs.len());
                for name in refs {
                    writer.decorate_name(name);
                }

                writer.finish_row();
            }

            writer.finish();
            Ok(batch.len() as u32)
        })();
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_emit(emit_t.elapsed());
        crate::git::backend::libgit::flush_blob_ring_stats();
        result
    }
}

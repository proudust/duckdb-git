use crate::git::backend::gix::{
    collect_refs, emit_commit, prepare_walk, run_commit_date_walk, start_commit_date_walk,
    walk_next_oid, CachedRepo, WalkPrepared,
};
use crate::git::date_walk::{CommitDateWalk, OidBytes};
use crate::git::sink::CommitSink;
use crate::git_log::params::GitLogParameter;
use crate::git_log::prefetch::{
    fixed_max_threads, OidPrefetchBuffer, READ_BATCH_SIZE, RING_CAPACITY,
};
use crate::git_log::schema;
use crate::git_log::vector::VectorInserter;
use duckdb::core::DataChunkHandle;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
#[cfg(feature = "prefetch-stats")]
use std::time::Instant;

enum ScanEngine {
    Prefetch { buffer: OidPrefetchBuffer },
    Inline {
        state: Mutex<InlineState>,
        first_parent: bool,
        batch_size: usize,
    },
}

enum InlineState {
    Pending(WalkPrepared),
    Active(CommitDateWalk),
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

        let engine = if max_threads == 1 {
            let first_parent = prep.first_parent;
            ScanEngine::Inline {
                state: Mutex::new(InlineState::Pending(prep)),
                first_parent,
                batch_size: READ_BATCH_SIZE,
            }
        } else {
            let buffer = OidPrefetchBuffer::new(RING_CAPACITY);
            let producer = buffer.producer();
            let repo_path_owned = repo_path.to_string();

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
                    let walk_result = run_commit_date_walk(repo, prep, |oid| {
                        if !producer.push(oid) {
                            return Ok(false);
                        }
                        Ok(true)
                    });
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
            } => self.read_inline(state, *first_parent, *batch_size, params, output, column_indices),
        }
    }

    fn read_prefetch(
        &self,
        buffer: &OidPrefetchBuffer,
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
        params: &GitLogParameter,
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
        let walk = match &mut *guard {
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
                #[cfg(feature = "prefetch-stats")]
                let walk_t = Instant::now();
                let started = start_commit_date_walk(repo, prep)?;
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_walk(walk_t.elapsed());
                *guard = InlineState::Active(started);
                let InlineState::Active(walk) = &mut *guard else {
                    unreachable!("just set Active");
                };
                walk
            }
            InlineState::Active(walk) => walk,
        };

        let mut batch = Vec::with_capacity(batch_size.min(256));
        #[cfg(feature = "prefetch-stats")]
        let walk_t = Instant::now();
        let mut exhausted = false;
        while batch.len() < batch_size {
            match walk_next_oid(repo, first_parent, walk)? {
                Some(oid) => batch.push(oid),
                None => {
                    exhausted = true;
                    break;
                }
            }
        }
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_walk(walk_t.elapsed());
        if exhausted {
            *guard = InlineState::Done;
        }
        drop(guard);

        if batch.is_empty() {
            #[cfg(feature = "prefetch-stats")]
            crate::git::diag::record_read(read_t.elapsed());
            return Ok(0);
        }

        let count = self.emit_batch(repo, &batch, params, output, column_indices)?;
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_read(read_t.elapsed());
        Ok(count)
    }

    fn emit_batch(
        &self,
        repo: &gix::Repository,
        batch: &[OidBytes],
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let mut writer = VectorInserter::new(output, column_indices);
        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        #[cfg(feature = "prefetch-stats")]
        let emit_t = Instant::now();
        for (batch_idx, oid_bytes) in batch.iter().enumerate() {
            let oid = bytes_to_oid(*oid_bytes);
            writer.begin_row(batch_idx);
            emit_commit(
                repo,
                oid,
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
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_emit(emit_t.elapsed());
        Ok(batch.len() as u32)
    }
}

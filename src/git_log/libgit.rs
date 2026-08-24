use crate::git::backend::libgit::{
    collect_refs, emit_commit, prepare_walk, run_commit_date_walk, BlobRing, CachedRepo, EmitOpts,
};
use crate::git::date_walk::OidBytes;
use crate::git::sink::CommitSink;
use crate::git_log::params::GitLogParameter;
use crate::git_log::prefetch::{
    fixed_max_threads, OidPrefetchBuffer, READ_BATCH_SIZE, RING_CAPACITY,
};
use crate::git_log::schema;
use crate::git_log::vector::VectorInserter;
use duckdb::core::DataChunkHandle;
use git2::Oid;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
#[cfg(feature = "prefetch-stats")]
use std::time::Instant;

struct LibGitLogScannerInner {
    buffer: OidPrefetchBuffer,
    decorations: HashMap<Oid, Vec<String>>,
    batch_size: usize,
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

        Ok(LibGitLogScanner {
            inner: Arc::new(LibGitLogScannerInner {
                buffer,
                decorations,
                batch_size: READ_BATCH_SIZE,
                max_threads: fixed_max_threads(true, schema::needs_file_changes(column_indices)),
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
        #[cfg(feature = "prefetch-stats")]
        let read_t = Instant::now();
        let batch = self
            .inner
            .buffer
            .take_batch(self.inner.batch_size)
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

        let mut writer = VectorInserter::new(output, column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let mut ring = BlobRing::new();
        #[cfg(feature = "prefetch-stats")]
        let emit_t = Instant::now();
        let result = (|| {
            for (batch_idx, oid_bytes) in batch.iter().enumerate() {
                let oid = bytes_to_oid(*oid_bytes);
                writer.begin_row(batch_idx);
                emit_commit(
                    repo,
                    oid,
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
        {
            crate::git::diag::record_emit(emit_t.elapsed());
            crate::git::diag::record_read(read_t.elapsed());
        }
        crate::git::backend::libgit::flush_blob_ring_stats();
        result
    }
}

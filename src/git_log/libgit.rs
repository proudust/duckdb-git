use crate::git::backend::libgit::{
    build_contained_index, collect_refs, emit_commit, walk_commit_oids, BlobRing, CachedRepo,
};
use crate::git::ref_index::ContainedIndex;
use crate::git::sink::CommitSink;
use crate::git_log::params::GitLogParameter;
use crate::git_log::schema;
use crate::git_log::vector::VectorInserter;
use duckdb::core::DataChunkHandle;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct LibGitLogScannerInner {
    commit_oids: Vec<git2::Oid>,
    decorations: HashMap<git2::Oid, Vec<String>>,
    contained: ContainedIndex<git2::Oid>,
    current_index: AtomicUsize,
    batch_size: usize,
    max_threads: u64,
    repo_path: String,
}

pub struct LibGitLogScanner {
    inner: Arc<LibGitLogScannerInner>,
}

impl LibGitLogScanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();

        let commit_oids = walk_commit_oids(
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
        let need_branches = schema::needs_contained_branches(column_indices);
        let need_tags = schema::needs_contained_tags(column_indices);
        let contained = if need_branches || need_tags {
            let wanted: HashSet<git2::Oid> = commit_oids.iter().copied().collect();
            build_contained_index(repo, params.decorate, need_branches, need_tags, &wanted)?
        } else {
            ContainedIndex::empty()
        };

        let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

        Ok(LibGitLogScanner {
            inner: Arc::new(LibGitLogScannerInner {
                commit_oids,
                decorations,
                contained,
                current_index: AtomicUsize::new(0),
                batch_size,
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
        let start_index = self
            .inner
            .current_index
            .fetch_add(self.inner.batch_size, Ordering::Relaxed);

        if start_index >= self.inner.commit_oids.len() {
            return Ok(0);
        }

        let end_index = std::cmp::min(
            start_index + self.inner.batch_size,
            self.inner.commit_oids.len(),
        );

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();

        let mut writer = VectorInserter::new(output, column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let mut ring = BlobRing::new();
        let oids = &self.inner.commit_oids[start_index..end_index];
        let result = (|| {
            for (batch_idx, oid) in oids.iter().enumerate() {
                writer.begin_row(batch_idx);
                emit_commit(
                    repo,
                    *oid,
                    params.ignore_all_space,
                    skip_file_changes,
                    params.diff_merges,
                    params.rename_threshold,
                    &mut writer,
                    &mut ring,
                )?;

                let refs = self.inner.decorations.get(oid).unwrap_or(&empty_refs);
                writer.begin_decorate(refs.len());
                for name in refs {
                    writer.decorate_name(name);
                }

                let n = self.inner.contained.branches_of(oid).count();
                writer.begin_contained_branches(n);
                for name in self.inner.contained.branches_of(oid) {
                    writer.contained_branch(name);
                }

                let n = self.inner.contained.tags_of(oid).count();
                writer.begin_contained_tags(n);
                for name in self.inner.contained.tags_of(oid) {
                    writer.contained_tag(name);
                }

                writer.finish_row();
            }

            writer.finish();
            Ok(oids.len() as u32)
        })();
        crate::git::backend::libgit::flush_blob_ring_stats();
        result
    }
}

/// Soft cap on DuckDB worker threads for libgit scans. Matches the "up to 4
/// parallel `read()` calls" assumption in BlobRing's `DEFAULT_CAP` RSS notes;
/// gix has no equivalent cap. Revisit if thread-scaling benches move the cliff.
const MAX_LIBGIT_THREADS: usize = 4;

fn compute_parallelism(commit_count: usize) -> (u64, usize) {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_LIBGIT_THREADS);
    let max_threads = std::cmp::min(commit_count, cpu_cores) as u64;
    let batch_size = (commit_count / cpu_cores).clamp(1, 2048);
    (max_threads, batch_size)
}

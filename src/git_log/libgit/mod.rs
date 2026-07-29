mod commits;
mod diff;
mod refs;
mod repo;
mod xdiff;

use crate::git_log::params::{DiffMerges, GitLogParameter};
use crate::git_log::ref_index::ContainedIndex;
use crate::git_log::schema;
use crate::git_log::vector::VectorInserter;
use crate::git_log::{GitLogReadPlanner, GitLogReader};
use commits::{read_commit, walk_commit_oids};
use duckdb::core::DataChunkHandle;
use refs::{build_contained_index, collect_refs};
use repo::CachedRepo;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
#[cfg(test)]
const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

struct LibGitLogReadPlannerInner {
    commit_oids: Vec<git2::Oid>,
    decorations: HashMap<git2::Oid, Vec<String>>,
    contained: ContainedIndex<git2::Oid>,
    current_index: AtomicUsize,
    batch_size: usize,
    max_threads: u64,
    repo_path: String,
}

pub struct LibGitLogReadPlanner {
    inner: Arc<LibGitLogReadPlannerInner>,
}

impl LibGitLogReadPlanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();

        let commit_oids = walk_commit_oids(repo, params.revision.as_deref(), params.max_count)?;
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

        Ok(LibGitLogReadPlanner {
            inner: Arc::new(LibGitLogReadPlannerInner {
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
}

impl GitLogReadPlanner for LibGitLogReadPlanner {
    fn max_threads(&self) -> u64 {
        self.inner.max_threads
    }

    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader> {
        Box::new(LibGitLogReader {
            inner: Arc::clone(&self.inner),
            ignore_all_space: params.ignore_all_space,
            diff_merges: params.diff_merges,
        })
    }
}

struct LibGitLogReader {
    inner: Arc<LibGitLogReadPlannerInner>,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
}

impl GitLogReader for LibGitLogReader {
    fn read(
        &mut self,
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
        let oids = &self.inner.commit_oids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = read_commit(
                repo,
                *oid,
                self.ignore_all_space,
                skip_file_changes,
                self.diff_merges,
            )?;
            let refs = self.inner.decorations.get(oid).unwrap_or(&empty_refs);
            let branches: Vec<&str> = self.inner.contained.branches_of(oid).collect();
            let tags: Vec<&str> = self.inner.contained.tags_of(oid).collect();
            writer.push(batch_idx, &oid.to_string(), &commit, refs, &branches, &tags);
        }

        writer.finish();
        Ok(oids.len() as u32)
    }
}

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

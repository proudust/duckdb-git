use crate::params::GitLogParameter;
use crate::schema;
use crate::vector::VectorInserter;
use duckdb::core::DataChunkHandle;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{open, BackendKind, GitBackend};

/// Created at VTab init, shared across parallel workers.
pub trait GitLogReadPlanner: Send + Sync {
    fn max_threads(&self) -> u64;
    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader>;
}

pub trait GitLogReader {
    fn read<'a>(
        &mut self,
        output: &'a mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>>;
}

struct ChunkedOidListPlannerInner {
    commit_oids: Vec<String>,
    decorations: HashMap<String, Vec<String>>,
    current_index: AtomicUsize,
    batch_size: usize,
    max_threads: u64,
    repo_path: String,
    backend: BackendKind,
}

struct ChunkedOidListPlanner {
    inner: Arc<ChunkedOidListPlannerInner>,
}

impl GitLogReadPlanner for ChunkedOidListPlanner {
    fn max_threads(&self) -> u64 {
        self.inner.max_threads
    }

    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader> {
        Box::new(ChunkedOidListReader {
            inner: Arc::clone(&self.inner),
            ignore_all_space: params.ignore_all_space,
        })
    }
}

struct ChunkedOidListReader {
    inner: Arc<ChunkedOidListPlannerInner>,
    ignore_all_space: bool,
}

impl GitLogReader for ChunkedOidListReader {
    fn read<'a>(
        &mut self,
        output: &'a mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let start_index = self
            .inner
            .current_index
            .fetch_add(self.inner.batch_size, Ordering::Relaxed);

        if start_index >= self.inner.commit_oids.len() {
            return Ok(0);
        }

        let end_index =
            std::cmp::min(start_index + self.inner.batch_size, self.inner.commit_oids.len());

        let backend = open(&self.inner.repo_path, self.inner.backend)?;

        let mut writer = VectorInserter::new(output, column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let oids = &self.inner.commit_oids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = backend.get_commit(oid, self.ignore_all_space, skip_file_changes)?;
            let refs = self.inner.decorations.get(oid).unwrap_or(&empty_refs);
            writer.push(batch_idx, oid, &commit, refs);
        }

        writer.finish();
        Ok(oids.len() as u32)
    }
}

fn compute_parallelism(commit_count: usize) -> (u64, usize) {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_threads = std::cmp::min(commit_count, cpu_cores) as u64;
    let batch_size = (commit_count / cpu_cores).clamp(1, 2048);
    (max_threads, batch_size)
}

pub fn open_planner(
    repo_path: &str,
    kind: BackendKind,
    params: &GitLogParameter,
    column_indices: &[u64],
) -> Result<Box<dyn GitLogReadPlanner>, Box<dyn Error>> {
    let backend = open(repo_path, kind)?;

    let commit_oids = backend.get_commit_oids(params.revision.as_deref(), params.max_count)?;
    let decorations = if schema::needs_refs(column_indices) {
        backend.get_refs(params.decorate)?
    } else {
        HashMap::new()
    };

    let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

    Ok(Box::new(ChunkedOidListPlanner {
        inner: Arc::new(ChunkedOidListPlannerInner {
            commit_oids,
            decorations,
            current_index: AtomicUsize::new(0),
            batch_size,
            max_threads,
            repo_path: repo_path.to_string(),
            backend: kind,
        }),
    }))
}

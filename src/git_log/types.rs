use crate::git_log::params::GitLogParameter;

use duckdb::core::DataChunkHandle;
use std::error::Error;

/// git diff --numstat convention for gitlink (submodule) entries.
pub fn gitlink_numstat(status: &str) -> (i32, i32) {
    match status {
        "A" => (1, 0),
        "D" => (0, 1),
        "M" | "R" | "T" | "C" => (1, 1),
        _ => (0, 0),
    }
}

#[derive(Clone)]
pub struct FileChange {
    pub path: String,
    pub old_path: Option<String>,
    pub status: &'static str,
    pub blob_id: String,
    pub file_size: Option<i64>,
    pub add_lines: i32,
    pub del_lines: i32,
}

pub struct CommitData {
    pub author_name: Vec<u8>,
    pub author_email: Vec<u8>,
    pub author_timestamp: i64,
    pub committer_name: Vec<u8>,
    pub committer_email: Vec<u8>,
    pub committer_timestamp: i64,
    pub message: Vec<u8>,
    pub parents: Vec<String>,
    pub file_changes: Vec<FileChange>,
}

pub trait GitLogReader {
    fn read<'a>(
        &mut self,
        output: &'a mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>>;
}

/// Created at VTab init, shared across parallel workers.
pub trait GitLogReadPlanner: Send + Sync {
    fn max_threads(&self) -> u64;
    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader>;
}

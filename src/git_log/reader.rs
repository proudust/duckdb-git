use crate::git_log::params::GitLogParameter;

use duckdb::core::DataChunkHandle;
use std::error::Error;

pub trait GitLogReader {
    fn read(
        &mut self,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>>;
}

/// Created at VTab init, shared across parallel workers.
pub trait GitLogReadPlanner: Send + Sync {
    fn max_threads(&self) -> u64;
    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader>;
}

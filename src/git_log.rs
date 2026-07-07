use crate::backend;
use crate::params::GitLogParameter;
use crate::schema;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::error::Error;
use std::sync::Arc;

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

struct GitLogInitData {
    planner: Arc<dyn GitLogReadPlanner>,
    column_indices: Vec<u64>,
}

struct GitLogVTab;

impl VTab for GitLogVTab {
    type InitData = GitLogInitData;
    type BindData = GitLogParameter;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        schema::bind_columns(bind)?;
        crate::params::bind(bind)
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let params = info.get_bind_data::<GitLogParameter>();
        let params = unsafe { &*params };

        let column_indices = info.get_column_indices();

        let planner: Arc<dyn GitLogReadPlanner> = Arc::from(backend::open_planner(
            &params.repo_path,
            params.backend,
            params,
            &column_indices,
        )?);
        info.set_max_threads(planner.max_threads());

        Ok(GitLogInitData {
            planner,
            column_indices,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let mut reader = init_data.planner.new_reader(bind_data);
        let row_count = reader.read(output, &init_data.column_indices)?;
        output.set_len(row_count as usize);
        Ok(())
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(crate::params::parameters())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(crate::params::named_parameters())
    }
}

pub fn register(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")?;
    Ok(())
}

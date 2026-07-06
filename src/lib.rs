mod backend;
mod params;
mod schema;
mod types;
mod vector;

use backend::GitBackend;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use params::GitLogBindData;
use vector::VectorInserter;
use std::{
    collections::HashMap,
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C)]
struct GitLogInitData {
    commit_ids: Vec<String>,
    decorations: HashMap<String, Vec<String>>,
    current_index: AtomicUsize,
    batch_size: usize,
    column_indices: Vec<u64>,
}

struct GitLogVTab;

impl VTab for GitLogVTab {
    type InitData = GitLogInitData;
    type BindData = GitLogBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        schema::bind_columns(bind)?;
        params::bind(bind)
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<GitLogBindData>();
        let bind_data = unsafe { &*bind_data };

        let column_indices = info.get_column_indices();

        let backend = backend::open(&bind_data.repo_path, bind_data.backend)?;

        let commit_oids =
            backend.get_commit_oids(bind_data.revision.as_deref(), bind_data.max_count)?;
        let decorations = if schema::needs_refs(&column_indices) {
            backend.get_refs(bind_data.decorate)?
        } else {
            HashMap::new()
        };

        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let max_threads = std::cmp::min(commit_oids.len(), cpu_cores) as u64;
        info.set_max_threads(max_threads);

        let batch_size = (commit_oids.len() / cpu_cores).clamp(1, 2048);

        Ok(GitLogInitData {
            commit_ids: commit_oids,
            decorations,
            current_index: AtomicUsize::new(0),
            batch_size,
            column_indices,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let batch_size = init_data.batch_size;

        let start_index = init_data
            .current_index
            .fetch_add(batch_size, Ordering::Relaxed);

        if start_index >= init_data.commit_ids.len() {
            output.set_len(0);
            return Ok(());
        }

        let end_index = std::cmp::min(start_index + batch_size, init_data.commit_ids.len());

        let backend = backend::open(&bind_data.repo_path, bind_data.backend)?;

        let mut writer = VectorInserter::new(output, &init_data.column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(&init_data.column_indices);
        let oids = &init_data.commit_ids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = backend.get_commit(oid, bind_data.ignore_all_space, skip_file_changes)?;
            let refs = init_data.decorations.get(oid).unwrap_or(&empty_refs);
            writer.push(batch_idx, oid, &commit, refs);
        }

        writer.finish();
        output.set_len(oids.len());
        Ok(())
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(params::parameters())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(params::named_parameters())
    }
}

pub fn register(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")?;
    Ok(())
}

#[cfg(feature = "loadable-extension")]
#[duckdb::duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register(&con)
}

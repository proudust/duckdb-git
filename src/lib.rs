mod backend;
mod types;
mod vector;

use backend::GitBackend;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use types::DiffMerges;
use vector::VectorInserter;
use std::{
    error::Error,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C)]
struct GitLogBindData {
    repo_path: String,
    revision: Option<String>,
    max_count: Option<usize>,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
}

#[repr(C)]
struct GitLogInitData {
    commit_ids: Vec<String>,
    current_index: AtomicUsize,
    batch_size: usize,
}

struct GitLogVTab;

impl VTab for GitLogVTab {
    type InitData = GitLogInitData;
    type BindData = GitLogBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("commit_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("author", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "author_email",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "author_timestamp",
            LogicalTypeHandle::from(LogicalTypeId::TimestampTZ),
        );
        bind.add_result_column("committer", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "committer_email",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "committer_timestamp",
            LogicalTypeHandle::from(LogicalTypeId::TimestampTZ),
        );
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        // parents: VARCHAR[]
        let parents_array_type =
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("parents", parents_array_type);

        let diff_merges = bind
            .get_named_parameter("diff_merges")
            .map(|value| DiffMerges::from_str(value.to_string().as_str()).unwrap())
            .unwrap_or_else(|| DiffMerges::Off);

        if !diff_merges.should_skip_file_changes() {
            let file_change_struct = LogicalTypeHandle::struct_type(&[
                ("path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("status", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("blob_id", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("file_size", LogicalTypeHandle::from(LogicalTypeId::Bigint)),
                ("add_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                ("del_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
            ]);
            let file_changes_array_type = LogicalTypeHandle::list(&file_change_struct);
            bind.add_result_column("file_changes", file_changes_array_type);
        }

        let repo_path = bind.get_parameter(0).to_string();

        let revision = bind
            .get_named_parameter("revision")
            .map(|value| value.to_string());

        let max_count = bind
            .get_named_parameter("max_count")
            .and_then(|value| value.to_string().parse::<usize>().ok());

        let ignore_all_space = bind
            .get_named_parameter("ignore_all_space")
            .map(|value| value.to_string().to_lowercase() == "true")
            .unwrap_or(false);

        Ok(GitLogBindData {
            repo_path,
            revision,
            max_count,
            ignore_all_space,
            diff_merges,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<GitLogBindData>();
        let bind_data = unsafe { &*bind_data };

        let backend = backend::open(
            &bind_data.repo_path,
            bind_data.ignore_all_space,
            bind_data.diff_merges.clone(),
        )?;

        let commit_oids =
            backend.get_commit_oids(bind_data.revision.as_deref(), bind_data.max_count)?;

        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let max_threads = std::cmp::min(commit_oids.len(), cpu_cores) as u64;
        info.set_max_threads(max_threads);

        let batch_size = (commit_oids.len() / cpu_cores).clamp(1, 2048);

        Ok(GitLogInitData {
            commit_ids: commit_oids,
            current_index: AtomicUsize::new(0),
            batch_size,
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

        let backend = backend::open(
            &bind_data.repo_path,
            bind_data.ignore_all_space,
            bind_data.diff_merges.clone(),
        )?;

        let mut writer =
            VectorInserter::new(output, !bind_data.diff_merges.should_skip_file_changes());

        let oids = &init_data.commit_ids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = backend.get_commit(oid)?;
            writer.push(batch_idx, oid, &commit);
        }

        writer.finish();
        output.set_len(oids.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // repo_path
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                "revision".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "max_count".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                "ignore_all_space".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "diff_merges".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ])
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

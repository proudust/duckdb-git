extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate git2;
extern crate libduckdb_sys;

mod git_log;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use git_log::{DiffMerges, GitContext};
use libduckdb_sys as ffi;
use std::{
    error::Error,
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
    commit_ids: Vec<git2::Oid>,
    current_index: AtomicUsize,
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

        // 名前付きパラメータ "diff_merges" を取得（列定義前に必要）
        let diff_merges = bind
            .get_named_parameter("diff_merges")
            .map(|value| DiffMerges::from_str(value.to_string().as_str()))
            .unwrap_or_else(|| DiffMerges::Off);

        // file_changes列はdiff_merges=offの場合は省略
        if !diff_merges.should_skip_file_changes() {
            // file_changes: STRUCT(path VARCHAR, status VARCHAR, blob_id VARCHAR, file_size BIGINT, add_lines INTEGER, del_lines INTEGER)[]
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

        // 名前付きパラメータ "revision" を取得
        let revision = bind
            .get_named_parameter("revision")
            .map(|value| value.to_string());

        // 名前付きパラメータ "max_count" を取得
        let max_count = bind
            .get_named_parameter("max_count")
            .and_then(|value| value.to_string().parse::<usize>().ok());

        // 名前付きパラメータ "ignore_all_space" を取得
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

        // GitContext を作成
        let ctx = GitContext::new(
            &bind_data.repo_path,
            bind_data.ignore_all_space,
            bind_data.diff_merges.clone(),
        )?;

        // 全てのコミットOIDを収集
        let commit_oids = ctx.get_commit_oids(bind_data.revision.as_ref(), bind_data.max_count)?;

        // 並行処理のためのスレッド数を設定（CPUコア数を基準とする）
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1); // フォールバックとして1コアを想定
        let max_threads = std::cmp::min(commit_oids.len(), cpu_cores) as u64;
        info.set_max_threads(max_threads);

        Ok(GitLogInitData {
            commit_ids: commit_oids,
            current_index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let current_index = init_data.current_index.load(Ordering::Relaxed);

        if current_index >= init_data.commit_ids.len() {
            output.set_len(0);
            return Ok(());
        }

        let oid = init_data.commit_ids[current_index];

        // GitContext を作成
        let ctx = GitContext::new(
            &bind_data.repo_path,
            bind_data.ignore_all_space,
            bind_data.diff_merges.clone(),
        )?;
        let commit = ctx.get_commit(oid)?;

        // commit_id column
        let commit_id_vector = output.flat_vector(0);
        commit_id_vector.insert(0, &oid.to_string());

        // author column
        let author_vector = output.flat_vector(1);
        author_vector.insert(0, commit.author_name());

        // author_email column
        let email_vector = output.flat_vector(2);
        email_vector.insert(0, commit.author_email());

        // author_timestamp column - convert to microseconds for DuckDB TIMESTAMP
        let mut author_timestamp_vector = output.flat_vector(3);
        let author_timestamp_micros = commit.author_timestamp() * 1_000_000; // Convert seconds to microseconds
        author_timestamp_vector.as_mut_slice::<i64>()[0] = author_timestamp_micros;

        // committer column
        let committer_vector = output.flat_vector(4);
        committer_vector.insert(0, commit.committer_name());

        // committer_email column
        let committer_email_vector = output.flat_vector(5);
        committer_email_vector.insert(0, commit.committer_email());

        // committer_timestamp column - convert to microseconds for DuckDB TIMESTAMP
        let mut committer_timestamp_vector = output.flat_vector(6);
        let committer_timestamp_micros = commit.committer_timestamp() * 1_000_000; // Convert seconds to microseconds
        committer_timestamp_vector.as_mut_slice::<i64>()[0] = committer_timestamp_micros;

        // message column
        let message_vector = output.flat_vector(7);
        message_vector.insert(0, commit.message());

        // parents column (string array)
        let parents = commit.parents();
        let mut parents_vector = output.list_vector(8);
        let parents_child = parents_vector.child(parents.len());
        for (i, parent) in parents.iter().enumerate() {
            parents_child.insert(i, parent.as_str());
        }
        parents_vector.set_entry(0, 0, parents.len());
        parents_vector.set_len(parents.len());

        // file_changes列の処理（diff_merges=offの場合はスキップ）
        if !bind_data.diff_merges.should_skip_file_changes() {
            let file_changes = commit.file_changes()?;
            // file_changes column (struct array) - インデックス9
            let mut file_changes_vector = output.list_vector(9);
            let file_changes_struct_child = file_changes_vector.struct_child(file_changes.len());

            // pathフィールド (struct内の0番目のフィールド)
            let path_child = file_changes_struct_child.child(0, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                path_child.insert(i, file_change.path.as_str());
            }

            // statusフィールド (struct内の1番目のフィールド)
            let status_child = file_changes_struct_child.child(1, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                status_child.insert(i, file_change.status.as_str());
            }

            // blob_idフィールド (struct内の2番目のフィールド)
            let blob_id_child = file_changes_struct_child.child(2, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                blob_id_child.insert(i, file_change.blob_id.as_str());
            }

            // file_sizeフィールド (struct内の3番目のフィールド)
            let mut file_size_child = file_changes_struct_child.child(3, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                file_size_child.as_mut_slice::<i64>()[i] = file_change.file_size;
            }

            // add_linesフィールド (struct内の4番目のフィールド)
            let mut add_lines_child = file_changes_struct_child.child(4, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                add_lines_child.as_mut_slice::<i32>()[i] = file_change.add_lines;
            }

            // del_linesフィールド (struct内の5番目のフィールド)
            let mut del_lines_child = file_changes_struct_child.child(5, file_changes.len());
            for (i, file_change) in file_changes.iter().enumerate() {
                del_lines_child.as_mut_slice::<i32>()[i] = file_change.del_lines;
            }

            file_changes_vector.set_entry(0, 0, file_changes.len());
            file_changes_vector.set_len(file_changes.len());
        }

        // Increment index for next call
        init_data
            .current_index
            .store(current_index + 1, Ordering::Relaxed);

        output.set_len(1);
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

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")
        .expect("Failed to register git_log table function");
    Ok(())
}

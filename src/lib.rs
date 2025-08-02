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

        // バッチサイズを事前計算（DuckDBのチャンクサイズ制限を考慮）
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

        // 事前計算されたバッチサイズを使用
        let batch_size = init_data.batch_size;

        let start_index = init_data
            .current_index
            .fetch_add(batch_size, Ordering::Relaxed);

        if start_index >= init_data.commit_ids.len() {
            output.set_len(0);
            return Ok(());
        }

        // 処理する範囲を計算
        let end_index = std::cmp::min(start_index + batch_size, init_data.commit_ids.len());

        // GitContext を作成
        let ctx = GitContext::new(
            &bind_data.repo_path,
            bind_data.ignore_all_space,
            bind_data.diff_merges.clone(),
        )?;

        // 各列のベクターを取得
        let commit_id_vector = output.flat_vector(0);
        let author_vector = output.flat_vector(1);
        let email_vector = output.flat_vector(2);
        let mut author_timestamp_vector = output.flat_vector(3);
        let committer_vector = output.flat_vector(4);
        let committer_email_vector = output.flat_vector(5);
        let mut committer_timestamp_vector = output.flat_vector(6);
        let message_vector = output.flat_vector(7);
        let mut parents_vector = output.list_vector(8);

        // file_changes列のベクターを条件付きで取得
        let mut file_changes_vector = if !bind_data.diff_merges.should_skip_file_changes() {
            Some(output.list_vector(9))
        } else {
            None
        };

        // バッチ内の各コミットを処理
        let oids = &init_data.commit_ids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = ctx.get_commit(*oid)?;

            // commit_id column
            commit_id_vector.insert(batch_idx, &oid.to_string());

            // author column
            author_vector.insert(batch_idx, commit.author_name());

            // author_email column
            email_vector.insert(batch_idx, commit.author_email());

            // author_timestamp column - convert to microseconds for DuckDB TIMESTAMP
            let author_timestamp_micros = commit.author_timestamp() * 1_000_000;
            author_timestamp_vector.as_mut_slice::<i64>()[batch_idx] = author_timestamp_micros;

            // committer column
            committer_vector.insert(batch_idx, commit.committer_name());

            // committer_email column
            committer_email_vector.insert(batch_idx, commit.committer_email());

            // committer_timestamp column - convert to microseconds for DuckDB TIMESTAMP
            let committer_timestamp_micros = commit.committer_timestamp() * 1_000_000;
            committer_timestamp_vector.as_mut_slice::<i64>()[batch_idx] =
                committer_timestamp_micros;

            // message column
            message_vector.insert(batch_idx, commit.message());

            // parents column (string array)
            let parents = commit.parents();
            let parents_child = parents_vector.child(parents.len());
            for (i, parent) in parents.iter().enumerate() {
                parents_child.insert(i, parent.as_str());
            }
            parents_vector.set_entry(batch_idx, 0, parents.len());

            // file_changes列の処理（diff_merges=offの場合はスキップ）
            if !bind_data.diff_merges.should_skip_file_changes() {
                let file_changes = commit.file_changes()?;
                let file_changes_struct_child = file_changes_vector
                    .as_mut()
                    .unwrap()
                    .struct_child(file_changes.len());

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

                file_changes_vector
                    .as_mut()
                    .unwrap()
                    .set_entry(batch_idx, 0, file_changes.len());
            }
        }

        // parentsとfile_changesベクターの長さを設定
        parents_vector.set_len(oids.len());
        if file_changes_vector.is_some() {
            file_changes_vector.as_mut().unwrap().set_len(oids.len());
        }

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

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")
        .expect("Failed to register git_log table function");
    Ok(())
}

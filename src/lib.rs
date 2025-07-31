extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate git2;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use git2::Repository;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(C)]
struct GitLogBindData {
    repo_path: String,
    revision: Option<String>,
    max_count: Option<usize>,
}

#[repr(C)]
struct GitLogInitData {
    commits: Vec<CommitInfo>,
    current_index: AtomicUsize,
}

#[derive(Clone)]
struct CommitInfo {
    commit_id: String,
    author: String,
    author_email: String,
    committer: String,
    committer_email: String,
    message: String,
    author_timestamp: i64,
    committer_timestamp: i64,
    parents: Vec<String>,
    file_changes: Vec<FileChange>,
}

#[derive(Clone)]
struct FileChange {
    path: String,
    status: String,
    blob_id: String,
    file_size: i64,
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

        // file_changes: STRUCT(path VARCHAR, status VARCHAR, blob_id VARCHAR, file_size BIGINT)[]
        let file_change_struct = LogicalTypeHandle::struct_type(&[
            ("path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("status", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("blob_id", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("file_size", LogicalTypeHandle::from(LogicalTypeId::Bigint)),
        ]);
        let file_changes_array_type = LogicalTypeHandle::list(&file_change_struct);
        bind.add_result_column("file_changes", file_changes_array_type);

        let repo_path = bind.get_parameter(0).to_string();

        // 名前付きパラメータ "revision" を取得
        let revision = bind
            .get_named_parameter("revision")
            .map(|value| value.to_string());

        // 名前付きパラメータ "max_count" を取得
        let max_count = bind
            .get_named_parameter("max_count")
            .and_then(|value| value.to_string().parse::<usize>().ok());

        Ok(GitLogBindData {
            repo_path,
            revision,
            max_count,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<GitLogBindData>();
        let bind_data = unsafe { &*bind_data };

        let commits = get_git_commits(
            &bind_data.repo_path,
            bind_data.revision.as_deref(),
            bind_data.max_count,
        )?;
        Ok(GitLogInitData {
            commits,
            current_index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let current_index = init_data.current_index.load(Ordering::Relaxed);

        if current_index >= init_data.commits.len() {
            output.set_len(0);
            return Ok(());
        }

        let commit = &init_data.commits[current_index]; // commit_id column
        let commit_id_vector = output.flat_vector(0);
        let commit_id_cstring = CString::new(commit.commit_id.as_str())?;
        commit_id_vector.insert(0, commit_id_cstring);

        // author column
        let author_vector = output.flat_vector(1);
        let author_cstring = CString::new(commit.author.as_str())?;
        author_vector.insert(0, author_cstring);

        // author_email column
        let email_vector = output.flat_vector(2);
        let email_cstring = CString::new(commit.author_email.as_str())?;
        email_vector.insert(0, email_cstring);

        // author_timestamp column - convert to microseconds for DuckDB TIMESTAMP
        let mut author_timestamp_vector = output.flat_vector(3);
        let author_timestamp_micros = commit.author_timestamp * 1_000_000; // Convert seconds to microseconds
        author_timestamp_vector.as_mut_slice::<i64>()[0] = author_timestamp_micros;

        // committer column
        let committer_vector = output.flat_vector(4);
        let committer_cstring = CString::new(commit.committer.as_str())?;
        committer_vector.insert(0, committer_cstring);

        // committer_email column
        let committer_email_vector = output.flat_vector(5);
        let committer_email_cstring = CString::new(commit.committer_email.as_str())?;
        committer_email_vector.insert(0, committer_email_cstring);

        // committer_timestamp column - convert to microseconds for DuckDB TIMESTAMP
        let mut committer_timestamp_vector = output.flat_vector(6);
        let committer_timestamp_micros = commit.committer_timestamp * 1_000_000; // Convert seconds to microseconds
        committer_timestamp_vector.as_mut_slice::<i64>()[0] = committer_timestamp_micros;

        // message column
        let message_vector = output.flat_vector(7);
        let message_cstring = CString::new(commit.message.as_str())?;
        message_vector.insert(0, message_cstring);

        // parents column (string array)
        let mut parents_vector = output.list_vector(8);
        let parents_child = parents_vector.child(commit.parents.len());
        for (i, parent) in commit.parents.iter().enumerate() {
            parents_child.insert(i, parent.as_str());
        }
        parents_vector.set_entry(0, 0, commit.parents.len());
        parents_vector.set_len(commit.parents.len());

        // file_changes column (struct array)
        let mut file_changes_vector = output.list_vector(9);
        let file_changes_struct_child = file_changes_vector.struct_child(commit.file_changes.len());

        // pathフィールド (struct内の0番目のフィールド)
        let path_child = file_changes_struct_child.child(0, commit.file_changes.len());
        for (i, file_change) in commit.file_changes.iter().enumerate() {
            path_child.insert(i, file_change.path.as_str());
        }

        // statusフィールド (struct内の1番目のフィールド)
        let status_child = file_changes_struct_child.child(1, commit.file_changes.len());
        for (i, file_change) in commit.file_changes.iter().enumerate() {
            status_child.insert(i, file_change.status.as_str());
        }

        // blob_idフィールド (struct内の2番目のフィールド)
        let blob_id_child = file_changes_struct_child.child(2, commit.file_changes.len());
        for (i, file_change) in commit.file_changes.iter().enumerate() {
            blob_id_child.insert(i, file_change.blob_id.as_str());
        }

        // file_sizeフィールド (struct内の3番目のフィールド)
        let mut file_size_child = file_changes_struct_child.child(3, commit.file_changes.len());
        for (i, file_change) in commit.file_changes.iter().enumerate() {
            file_size_child.as_mut_slice::<i64>()[i] = file_change.file_size;
        }

        file_changes_vector.set_entry(0, 0, commit.file_changes.len());
        file_changes_vector.set_len(commit.file_changes.len());

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
        ])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")
        .expect("Failed to register git_log table function");
    Ok(())
}

fn get_git_commits(
    repo_path: &str,
    revision: Option<&str>,
    max_count: Option<usize>,
) -> Result<Vec<CommitInfo>, git2::Error> {
    let repo = Repository::open(repo_path)?;
    let mut revwalk = repo.revwalk()?;

    // リビジョンが指定されている場合はそれを使用、そうでなければHEADを使用
    match revision {
        Some(rev) => {
            // ブランチ名やコミットハッシュを解決
            let obj = repo.revparse_single(rev)?;
            revwalk.push(obj.id())?;
        }
        None => {
            revwalk.push_head()?;
        }
    }

    let mut commits = Vec::new();

    // max_countが指定されていればその値を使用、そうでなければデフォルトで全件を取得
    let revwalk_iter: Box<dyn Iterator<Item = _>> = match max_count {
        Some(count) => Box::new(revwalk.take(count)),
        None => Box::new(revwalk),
    };

    for oid in revwalk_iter {
        let oid = oid?;
        let commit = repo.find_commit(oid)?;

        let mut file_changes = Vec::new();

        // 各コミットについて変更されたファイルを取得
        let parent_count = commit.parent_count();

        if parent_count == 0 {
            // 初回コミットの場合、全てのファイルを新規追加として扱う
            let tree = commit.tree()?;
            tree.walk(git2::TreeWalkMode::PreOrder, |_root, entry| {
                if let Some(name) = entry.name() {
                    let oid = entry.id();
                    let file_size = if let Ok(blob) = repo.find_blob(oid) {
                        blob.size() as i64
                    } else {
                        0 // ディレクトリや取得できない場合は0
                    };

                    file_changes.push(FileChange {
                        path: name.to_string(),
                        status: "A".to_string(), // Added
                        blob_id: oid.to_string(),
                        file_size,
                    });
                }
                git2::TreeWalkResult::Ok
            })?;
        } else {
            // 親コミットとの差分を取得
            let parent = commit.parent(0)?;
            let parent_tree = parent.tree()?;
            let current_tree = commit.tree()?;

            let diff = repo.diff_tree_to_tree(Some(&parent_tree), Some(&current_tree), None)?;

            diff.foreach(
                &mut |delta, _progress| {
                    let status = match delta.status() {
                        git2::Delta::Added => "A",
                        git2::Delta::Deleted => "D",
                        git2::Delta::Modified => "M",
                        git2::Delta::Renamed => "R",
                        git2::Delta::Copied => "C",
                        git2::Delta::Ignored => "I",
                        git2::Delta::Untracked => "?",
                        git2::Delta::Typechange => "T",
                        _ => "U", // Unknown/Unmodified
                    };

                    let file_path = if let Some(new_file) = delta.new_file().path() {
                        new_file.to_string_lossy().to_string()
                    } else if let Some(old_file) = delta.old_file().path() {
                        old_file.to_string_lossy().to_string()
                    } else {
                        "unknown".to_string()
                    };

                    // blob_idとファイルサイズの取得
                    let (blob_id, file_size) = if delta.new_file().path().is_some() {
                        let new_oid = delta.new_file().id();
                        let size = if let Ok(blob) = repo.find_blob(new_oid) {
                            blob.size() as i64
                        } else {
                            0 // ディレクトリや削除されたファイルは0
                        };
                        (new_oid.to_string(), size)
                    } else if delta.old_file().path().is_some() {
                        let old_oid = delta.old_file().id();
                        let size = if let Ok(blob) = repo.find_blob(old_oid) {
                            blob.size() as i64
                        } else {
                            0
                        };
                        (old_oid.to_string(), size)
                    } else {
                        ("unknown".to_string(), 0)
                    };

                    file_changes.push(FileChange {
                        path: file_path,
                        status: status.to_string(),
                        blob_id,
                        file_size,
                    });

                    true
                },
                None,
                None,
                None,
            )?;
        }

        // 親コミットIDを取得
        let parents: Vec<String> = (0..commit.parent_count())
            .map(|i| commit.parent_id(i).unwrap().to_string())
            .collect();

        // 1つのコミットにつき1つのCommitInfoを作成
        commits.push(CommitInfo {
            commit_id: oid.to_string(),
            author: commit.author().name().unwrap_or("Unknown").to_string(),
            author_email: commit.author().email().unwrap_or("Unknown").to_string(),
            committer: commit.committer().name().unwrap_or("Unknown").to_string(),
            committer_email: commit.committer().email().unwrap_or("Unknown").to_string(),
            message: commit.message().unwrap_or("No message").to_string(),
            author_timestamp: commit.time().seconds(),
            committer_timestamp: commit.committer().when().seconds(),
            parents,
            file_changes,
        });
    }

    Ok(commits)
}

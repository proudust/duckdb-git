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
}

#[repr(C)]
struct GitLogInitData {
    commits: Vec<CommitInfo>,
    current_index: AtomicUsize,
}

#[derive(Clone)]
struct CommitInfo {
    hash: String,
    author: String,
    email: String,
    message: String,
    timestamp: i64,
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
        bind.add_result_column("hash", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("author", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("email", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "timestamp",
            LogicalTypeHandle::from(LogicalTypeId::TimestampTZ),
        );

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

        Ok(GitLogBindData {
            repo_path,
            revision,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<GitLogBindData>();
        let bind_data = unsafe { &*bind_data };

        let commits = get_git_commits(&bind_data.repo_path, bind_data.revision.as_deref())?;
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

        let commit = &init_data.commits[current_index]; // hash column
        let hash_vector = output.flat_vector(0);
        let hash_cstring = CString::new(commit.hash.as_str())?;
        hash_vector.insert(0, hash_cstring);

        // author column
        let author_vector = output.flat_vector(1);
        let author_cstring = CString::new(commit.author.as_str())?;
        author_vector.insert(0, author_cstring);

        // email column
        let email_vector = output.flat_vector(2);
        let email_cstring = CString::new(commit.email.as_str())?;
        email_vector.insert(0, email_cstring);

        // message column
        let message_vector = output.flat_vector(3);
        let message_cstring = CString::new(commit.message.as_str())?;
        message_vector.insert(0, message_cstring);

        // timestamp column - convert to microseconds for DuckDB TIMESTAMP
        let mut timestamp_vector = output.flat_vector(4);
        let timestamp_micros = commit.timestamp * 1_000_000; // Convert seconds to microseconds
        timestamp_vector.as_mut_slice::<i64>()[0] = timestamp_micros;

        // file_changes column (struct array)
        let mut file_changes_vector = output.list_vector(5);
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
        Some(vec![(
            "revision".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )])
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

    for oid in revwalk.take(100) {
        // 最新100件のコミットを取得
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

        // 1つのコミットにつき1つのCommitInfoを作成
        commits.push(CommitInfo {
            hash: oid.to_string(),
            author: commit.author().name().unwrap_or("Unknown").to_string(),
            email: commit.author().email().unwrap_or("Unknown").to_string(),
            message: commit.message().unwrap_or("No message").to_string(),
            timestamp: commit.time().seconds(),
            file_changes,
        });
    }

    Ok(commits)
}

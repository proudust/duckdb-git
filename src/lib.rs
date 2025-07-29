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
        bind.add_result_column("timestamp", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let repo_path = bind.get_parameter(0).to_string();
        Ok(GitLogBindData { repo_path })
    }

    fn init(_info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        // バインドデータから直接リポジトリパスを取得するのは困難なため、
        // カレントディレクトリを使用します
        let commits = get_git_commits(".")?;
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

        // timestamp column - convert to readable format
        let timestamp_vector = output.flat_vector(4);
        let timestamp_string = format!("{}", commit.timestamp);
        let timestamp_cstring = CString::new(timestamp_string)?;
        timestamp_vector.insert(0, timestamp_cstring);

        // Increment index for next call
        init_data
            .current_index
            .store(current_index + 1, Ordering::Relaxed);

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitLogVTab>("git_log")
        .expect("Failed to register git_log table function");
    Ok(())
}

fn get_git_commits(repo_path: &str) -> Result<Vec<CommitInfo>, git2::Error> {
    let repo = Repository::open(repo_path)?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;

    let mut commits = Vec::new();

    for oid in revwalk.take(100) {
        // 最新100件のコミットを取得
        let oid = oid?;
        let commit = repo.find_commit(oid)?;

        commits.push(CommitInfo {
            hash: oid.to_string(),
            author: commit.author().name().unwrap_or("Unknown").to_string(),
            email: commit.author().email().unwrap_or("Unknown").to_string(),
            message: commit.message().unwrap_or("No message").to_string(),
            timestamp: commit.time().seconds(),
        });
    }

    Ok(commits)
}

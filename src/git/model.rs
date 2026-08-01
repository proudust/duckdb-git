/// git diff --numstat convention for gitlink (submodule) entries.
pub fn gitlink_numstat(status: &str) -> (i32, i32) {
    match status {
        "A" => (1, 0),
        "D" => (0, 1),
        "M" | "R" | "T" | "C" => (1, 1),
        _ => (0, 0),
    }
}

/// Match `git log --numstat` when a blob object is missing (`diff.c`: `unable to read %s`).
pub fn unable_to_read_object(oid: impl std::fmt::Display) -> String {
    format!("unable to read {oid}")
}

#[derive(Clone)]
pub struct FileChange {
    pub path: String,
    pub old_path: Option<String>,
    pub status: &'static str,
    pub blob_id: String,
    pub file_size: Option<i64>,
    pub add_lines: Option<i32>,
    pub del_lines: Option<i32>,
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

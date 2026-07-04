#[derive(Clone)]
pub struct FileChange {
    pub path: String,
    pub status: &'static str,
    pub blob_id: String,
    pub file_size: i64,
    pub add_lines: i32,
    pub del_lines: i32,
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

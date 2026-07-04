#[derive(Debug, Clone, PartialEq)]
pub enum DiffMerges {
    Off,
    FirstParent,
}

impl DiffMerges {
    pub fn should_skip_file_changes(&self) -> bool {
        matches!(self, DiffMerges::Off)
    }
}

impl std::str::FromStr for DiffMerges {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "off" | "none" => DiffMerges::Off,
            _ => DiffMerges::FirstParent,
        })
    }
}

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

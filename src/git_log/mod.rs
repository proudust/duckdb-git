#[cfg(feature = "git2-backend")]
mod git2;
#[cfg(feature = "gix-backend")]
mod gix;

#[cfg(feature = "git2-backend")]
pub use self::git2::{Commit, GitContext};
#[cfg(feature = "gix-backend")]
pub use self::gix::{Commit, GitContext};

#[derive(Debug, Clone, PartialEq)]
pub enum DiffMerges {
    Off,
    FirstParent,
}

impl DiffMerges {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "off" => DiffMerges::Off,
            "none" => DiffMerges::Off,
            "first-parent" => DiffMerges::FirstParent,
            _ => DiffMerges::FirstParent,
        }
    }

    pub fn should_skip_file_changes(&self) -> bool {
        matches!(self, DiffMerges::Off)
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

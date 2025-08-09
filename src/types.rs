pub struct GitLogParameter {
    pub repo_path: String,
    pub revision: Option<String>,

    pub decorate: DecorateMode,
    pub max_count: Option<usize>,
    pub stat: bool,
    pub name_only: bool,
    pub name_status: bool,
    pub ignore_all_space: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecorateMode {
    No,
    Short,
    Full,
}

impl DecorateMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "no" => DecorateMode::No,
            "short" => DecorateMode::Short,
            "full" => DecorateMode::Full,
            _ => DecorateMode::Short, // デフォルトは short
        }
    }
}

pub trait GitContext {
    type Commit: GitCommit;

    fn get_commit(&self) -> &Self::Commit;
}

pub trait GitCommit {
    type FileChange: GitFileChange;

    fn commit_id(&self) -> &[u8];
    fn refs(&self) -> Vec<&[u8]>;
    fn parents(&self) -> Vec<&[u8]>;
    fn author_name(&self) -> &[u8];
    fn author_email(&self) -> &[u8];
    fn author_timestamp(&self) -> i64;
    fn committer_name(&self) -> &[u8];
    fn committer_email(&self) -> &[u8];
    fn committer_timestamp(&self) -> i64;
    fn message(&self) -> &[u8];
    fn file_changes(&self) -> Vec<Self::FileChange>;
}

pub trait GitFileChange {
    fn blob_id(&self) -> &[u8];
    fn path(&self) -> &[u8];
    fn status(&self) -> FileStatus;
    fn add_lines(&self) -> i32;
    fn del_lines(&self) -> i32;
    fn file_size(&self) -> i64;
}

#[derive(Clone, Debug)]
pub enum FileStatus {
    Added,
    Deleted,
    Modified,
    Renamed,
    Copied,
    Ignored,
    Untracked,
    Typechange,
    Unmodified,
    Unknown,
}

impl FileStatus {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            FileStatus::Added => b"A",
            FileStatus::Deleted => b"D",
            FileStatus::Modified => b"M",
            FileStatus::Renamed => b"R",
            FileStatus::Copied => b"C",
            FileStatus::Ignored => b"I",
            FileStatus::Untracked => b"?",
            FileStatus::Typechange => b"T",
            FileStatus::Unmodified => b" ",
            FileStatus::Unknown => b"U",
        }
    }

    pub fn from_delta(delta: git2::Delta) -> Self {
        match delta {
            git2::Delta::Added => FileStatus::Added,
            git2::Delta::Deleted => FileStatus::Deleted,
            git2::Delta::Modified => FileStatus::Modified,
            git2::Delta::Renamed => FileStatus::Renamed,
            git2::Delta::Copied => FileStatus::Copied,
            git2::Delta::Ignored => FileStatus::Ignored,
            git2::Delta::Untracked => FileStatus::Untracked,
            git2::Delta::Typechange => FileStatus::Typechange,
            git2::Delta::Unmodified => FileStatus::Unmodified,
            _ => FileStatus::Unknown,
        }
    }
}

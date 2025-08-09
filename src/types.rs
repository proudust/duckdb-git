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

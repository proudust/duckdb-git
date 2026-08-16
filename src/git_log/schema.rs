use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum GitLogColumn {
    CommitId = 0,
    Author = 1,
    AuthorEmail = 2,
    AuthorTimestamp = 3,
    Committer = 4,
    CommitterEmail = 5,
    CommitterTimestamp = 6,
    Message = 7,
    Parents = 8,
    Decorate = 9,
    ContainedBranches = 10,
    ContainedTags = 11,
    FileChanges = 12,
}

impl GitLogColumn {
    pub fn index(self) -> u64 {
        self as u64
    }
}

impl TryFrom<u64> for GitLogColumn {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::CommitId),
            1 => Ok(Self::Author),
            2 => Ok(Self::AuthorEmail),
            3 => Ok(Self::AuthorTimestamp),
            4 => Ok(Self::Committer),
            5 => Ok(Self::CommitterEmail),
            6 => Ok(Self::CommitterTimestamp),
            7 => Ok(Self::Message),
            8 => Ok(Self::Parents),
            9 => Ok(Self::Decorate),
            10 => Ok(Self::ContainedBranches),
            11 => Ok(Self::ContainedTags),
            12 => Ok(Self::FileChanges),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FileChangeField {
    Path = 0,
    OldPath = 1,
    Status = 2,
    BlobId = 3,
    FileSize = 4,
    AddLines = 5,
    DelLines = 6,
}

impl FileChangeField {
    pub fn index(self) -> usize {
        self as usize
    }
}

fn file_change_struct_type() -> LogicalTypeHandle {
    LogicalTypeHandle::struct_type(&[
        ("path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("old_path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("status", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("blob_id", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("file_size", LogicalTypeHandle::from(LogicalTypeId::Bigint)),
        ("add_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
        ("del_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
    ])
}

/// LIST(STRUCT(...)) type for the `file_changes` result column.
pub(crate) fn file_changes_list_type() -> LogicalTypeHandle {
    LogicalTypeHandle::list(&file_change_struct_type())
}

pub fn bind_columns(bind: &BindInfo) -> Result<(), Box<dyn std::error::Error>> {
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

    let parents_array_type =
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("parents", parents_array_type);

    let decorate_array_type =
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("decorate", decorate_array_type);

    let contained_branches_array_type =
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("contained_branches", contained_branches_array_type);

    let contained_tags_array_type =
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("contained_tags", contained_tags_array_type);

    bind.add_result_column("file_changes", file_changes_list_type());

    Ok(())
}

pub fn needs_refs(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitLogColumn::Decorate.index())
}

pub fn needs_contained_branches(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitLogColumn::ContainedBranches.index())
}

pub fn needs_contained_tags(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitLogColumn::ContainedTags.index())
}

pub fn needs_file_changes(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitLogColumn::FileChanges.index())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_indices_are_contiguous() {
        let last = GitLogColumn::FileChanges.index();
        for i in 0..=last {
            assert!(
                GitLogColumn::try_from(i).is_ok(),
                "missing column index {i}"
            );
        }
        assert!(GitLogColumn::try_from(last + 1).is_err());
    }

    #[test]
    fn projection_helpers() {
        let unrelated = [GitLogColumn::CommitId.index(), GitLogColumn::Author.index()];
        for (needs, column) in [
            (needs_refs as fn(&[u64]) -> bool, GitLogColumn::Decorate),
            (needs_contained_branches, GitLogColumn::ContainedBranches),
            (needs_contained_tags, GitLogColumn::ContainedTags),
            (needs_file_changes, GitLogColumn::FileChanges),
        ] {
            assert!(needs(&[column.index()]), "{column:?} should be required");
            assert!(!needs(&unrelated), "{column:?} should not be required");
        }
    }
}

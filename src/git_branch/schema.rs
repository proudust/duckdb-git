use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum GitBranchColumn {
    Name = 0,
    Refname = 1,
    IsHead = 2,
    CommitId = 3,
    Subject = 4,
    Author = 5,
    AuthorEmail = 6,
    AuthorTimestamp = 7,
    Committer = 8,
    CommitterEmail = 9,
    CommitterTimestamp = 10,
    Upstream = 11,
    UpstreamAhead = 12,
    UpstreamBehind = 13,
    UpstreamGone = 14,
    Push = 15,
    SymrefTarget = 16,
}

impl GitBranchColumn {
    pub fn index(self) -> u64 {
        self as u64
    }
}

impl TryFrom<u64> for GitBranchColumn {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Name),
            1 => Ok(Self::Refname),
            2 => Ok(Self::IsHead),
            3 => Ok(Self::CommitId),
            4 => Ok(Self::Subject),
            5 => Ok(Self::Author),
            6 => Ok(Self::AuthorEmail),
            7 => Ok(Self::AuthorTimestamp),
            8 => Ok(Self::Committer),
            9 => Ok(Self::CommitterEmail),
            10 => Ok(Self::CommitterTimestamp),
            11 => Ok(Self::Upstream),
            12 => Ok(Self::UpstreamAhead),
            13 => Ok(Self::UpstreamBehind),
            14 => Ok(Self::UpstreamGone),
            15 => Ok(Self::Push),
            16 => Ok(Self::SymrefTarget),
            _ => Err(()),
        }
    }
}

pub fn bind_columns(bind: &BindInfo) -> Result<(), Box<dyn std::error::Error>> {
    bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("refname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("is_head", LogicalTypeHandle::from(LogicalTypeId::Boolean));
    bind.add_result_column("commit_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("subject", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("author", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("author_email", LogicalTypeHandle::from(LogicalTypeId::Varchar));
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
    bind.add_result_column("upstream", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("upstream_ahead", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    bind.add_result_column("upstream_behind", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    bind.add_result_column("upstream_gone", LogicalTypeHandle::from(LogicalTypeId::Boolean));
    bind.add_result_column("push", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("symref_target", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    Ok(())
}

pub fn needs_tip_meta(column_indices: &[u64]) -> bool {
    [
        GitBranchColumn::Subject,
        GitBranchColumn::Author,
        GitBranchColumn::AuthorEmail,
        GitBranchColumn::AuthorTimestamp,
        GitBranchColumn::Committer,
        GitBranchColumn::CommitterEmail,
        GitBranchColumn::CommitterTimestamp,
    ]
    .iter()
    .any(|c| column_indices.contains(&c.index()))
}

pub fn needs_upstream(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitBranchColumn::Upstream.index())
        || needs_ahead_behind(column_indices)
        || column_indices.contains(&GitBranchColumn::UpstreamGone.index())
}

pub fn needs_push(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitBranchColumn::Push.index())
}

pub fn needs_symref(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitBranchColumn::SymrefTarget.index())
}

pub fn needs_ahead_behind(column_indices: &[u64]) -> bool {
    column_indices.contains(&GitBranchColumn::UpstreamAhead.index())
        || column_indices.contains(&GitBranchColumn::UpstreamBehind.index())
}

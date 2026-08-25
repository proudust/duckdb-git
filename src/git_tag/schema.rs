use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum GitTagColumn {
    Name = 0,
    Refname = 1,
    ObjectId = 2,
    ObjectType = 3,
    CommitId = 4,
    IsAnnotated = 5,
    Tagger = 6,
    TaggerEmail = 7,
    TaggerTimestamp = 8,
    Message = 9,
    Signature = 10,
}

impl GitTagColumn {
    pub fn index(self) -> u64 {
        self as u64
    }
}

impl TryFrom<u64> for GitTagColumn {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Name),
            1 => Ok(Self::Refname),
            2 => Ok(Self::ObjectId),
            3 => Ok(Self::ObjectType),
            4 => Ok(Self::CommitId),
            5 => Ok(Self::IsAnnotated),
            6 => Ok(Self::Tagger),
            7 => Ok(Self::TaggerEmail),
            8 => Ok(Self::TaggerTimestamp),
            9 => Ok(Self::Message),
            10 => Ok(Self::Signature),
            _ => Err(()),
        }
    }
}

pub fn bind_columns(bind: &BindInfo) -> Result<(), Box<dyn std::error::Error>> {
    bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("refname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("object_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("object_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("commit_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("is_annotated", LogicalTypeHandle::from(LogicalTypeId::Boolean));
    bind.add_result_column("tagger", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("tagger_email", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column(
        "tagger_timestamp",
        LogicalTypeHandle::from(LogicalTypeId::TimestampTZ),
    );
    bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("signature", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    Ok(())
}

pub fn needs_annotated_meta(column_indices: &[u64]) -> bool {
    [
        GitTagColumn::Tagger,
        GitTagColumn::TaggerEmail,
        GitTagColumn::TaggerTimestamp,
        GitTagColumn::Message,
        GitTagColumn::Signature,
    ]
    .iter()
    .any(|c| column_indices.contains(&c.index()))
}

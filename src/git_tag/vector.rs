use crate::git::ref_row::TagRow;
use crate::git_tag::schema::GitTagColumn;
use duckdb::core::{DataChunkHandle, FlatVector, Inserter};

pub struct TagVectorInserter<'a> {
    name: Option<FlatVector<'a>>,
    refname: Option<FlatVector<'a>>,
    object_id: Option<FlatVector<'a>>,
    object_type: Option<FlatVector<'a>>,
    commit_id: Option<FlatVector<'a>>,
    is_annotated: Option<FlatVector<'a>>,
    tagger: Option<FlatVector<'a>>,
    tagger_email: Option<FlatVector<'a>>,
    tagger_timestamp: Option<FlatVector<'a>>,
    message: Option<FlatVector<'a>>,
    signature: Option<FlatVector<'a>>,
    row_idx: usize,
}

impl<'a> TagVectorInserter<'a> {
    pub fn new(chunk: &'a DataChunkHandle, column_indices: &[u64]) -> Self {
        let mut s = Self {
            name: None,
            refname: None,
            object_id: None,
            object_type: None,
            commit_id: None,
            is_annotated: None,
            tagger: None,
            tagger_email: None,
            tagger_timestamp: None,
            message: None,
            signature: None,
            row_idx: 0,
        };
        for (chunk_pos, &orig_idx) in column_indices.iter().enumerate() {
            match GitTagColumn::try_from(orig_idx) {
                Ok(GitTagColumn::Name) => s.name = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::Refname) => s.refname = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::ObjectId) => s.object_id = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::ObjectType) => s.object_type = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::CommitId) => s.commit_id = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::IsAnnotated) => s.is_annotated = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::Tagger) => s.tagger = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::TaggerEmail) => s.tagger_email = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::TaggerTimestamp) => {
                    s.tagger_timestamp = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitTagColumn::Message) => s.message = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitTagColumn::Signature) => s.signature = Some(chunk.flat_vector(chunk_pos)),
                Err(()) => {}
            }
        }
        s
    }

    pub fn write_row(&mut self, row: &TagRow) {
        let idx = self.row_idx;
        if let Some(v) = self.name.as_mut() {
            v.insert(idx, row.name.as_bytes());
        }
        if let Some(v) = self.refname.as_mut() {
            v.insert(idx, row.refname.as_bytes());
        }
        if let Some(v) = self.object_id.as_mut() {
            v.insert(idx, row.object_id.as_bytes());
        }
        if let Some(v) = self.object_type.as_mut() {
            v.insert(idx, row.object_type.as_bytes());
        }
        write_optional_str(&mut self.commit_id, idx, row.commit_id.as_deref());
        if let Some(v) = self.is_annotated.as_mut() {
            unsafe {
                v.as_mut_slice::<bool>()[idx] = row.is_annotated;
            }
        }
        write_optional_str(&mut self.tagger, idx, row.tagger.as_deref());
        write_optional_str(&mut self.tagger_email, idx, row.tagger_email.as_deref());
        if let Some(v) = self.tagger_timestamp.as_mut() {
            match row.tagger_timestamp {
                Some(ts) => unsafe {
                    v.as_mut_slice::<i64>()[idx] = ts * 1_000_000;
                },
                None => v.set_null(idx),
            }
        }
        write_optional_str(&mut self.message, idx, row.message.as_deref());
        write_optional_str(&mut self.signature, idx, row.signature.as_deref());
        self.row_idx += 1;
    }
}

fn write_optional_str(v: &mut Option<FlatVector<'_>>, idx: usize, value: Option<&str>) {
    if let Some(vec) = v {
        match value {
            Some(s) => vec.insert(idx, s.as_bytes()),
            None => vec.set_null(idx),
        }
    }
}

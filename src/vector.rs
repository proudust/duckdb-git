use crate::schema::{FileChangeField, GitLogColumn};
use crate::types::CommitData;
use duckdb::core::{DataChunkHandle, FlatVector, Inserter, ListVector};

pub struct VectorInserter<'a> {
    commit_id: Option<FlatVector<'a>>,
    author: Option<FlatVector<'a>>,
    author_email: Option<FlatVector<'a>>,
    author_timestamp: Option<FlatVector<'a>>,
    committer: Option<FlatVector<'a>>,
    committer_email: Option<FlatVector<'a>>,
    committer_timestamp: Option<FlatVector<'a>>,
    message: Option<FlatVector<'a>>,
    parents: Option<ListVector<'a>>,
    parents_offset: usize,
    decorate: Option<ListVector<'a>>,
    decorate_offset: usize,
    file_changes: Option<ListVector<'a>>,
    file_changes_offset: usize,
}

impl<'a> VectorInserter<'a> {
    pub fn new(chunk: &'a DataChunkHandle, column_indices: &[u64]) -> Self {
        let mut commit_id = None;
        let mut author = None;
        let mut author_email = None;
        let mut author_timestamp = None;
        let mut committer = None;
        let mut committer_email = None;
        let mut committer_timestamp = None;
        let mut message = None;
        let mut parents = None;
        let mut decorate = None;
        let mut file_changes = None;

        for (chunk_pos, &orig_idx) in column_indices.iter().enumerate() {
            match GitLogColumn::try_from(orig_idx) {
                Ok(GitLogColumn::CommitId) => commit_id = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitLogColumn::Author) => author = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitLogColumn::AuthorEmail) => author_email = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitLogColumn::AuthorTimestamp) => {
                    author_timestamp = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitLogColumn::Committer) => committer = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitLogColumn::CommitterEmail) => {
                    committer_email = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitLogColumn::CommitterTimestamp) => {
                    committer_timestamp = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitLogColumn::Message) => message = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitLogColumn::Parents) => parents = Some(chunk.list_vector(chunk_pos)),
                Ok(GitLogColumn::Decorate) => decorate = Some(chunk.list_vector(chunk_pos)),
                Ok(GitLogColumn::FileChanges) => {
                    file_changes = Some(chunk.list_vector(chunk_pos))
                }
                Err(()) => {}
            }
        }

        VectorInserter {
            commit_id,
            author,
            author_email,
            author_timestamp,
            committer,
            committer_email,
            committer_timestamp,
            message,
            parents,
            parents_offset: 0,
            decorate,
            decorate_offset: 0,
            file_changes,
            file_changes_offset: 0,
        }
    }

    pub fn push(&mut self, idx: usize, oid: &str, commit: &CommitData, refs: &[String]) {
        if let Some(v) = self.commit_id.as_mut() {
            v.insert(idx, oid);
        }
        if let Some(v) = self.author.as_mut() {
            v.insert(idx, &commit.author_name);
        }
        if let Some(v) = self.author_email.as_mut() {
            v.insert(idx, &commit.author_email);
        }
        if let Some(v) = self.author_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[idx] = commit.author_timestamp * 1_000_000;
            }
        }
        if let Some(v) = self.committer.as_mut() {
            v.insert(idx, &commit.committer_name);
        }
        if let Some(v) = self.committer_email.as_mut() {
            v.insert(idx, &commit.committer_email);
        }
        if let Some(v) = self.committer_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[idx] = commit.committer_timestamp * 1_000_000;
            }
        }
        if let Some(v) = self.message.as_mut() {
            v.insert(idx, &commit.message);
        }

        if let Some(parents_vec) = self.parents.as_mut() {
            let parents = &commit.parents;
            let parents_child = parents_vec.child(self.parents_offset + parents.len());
            for (i, parent) in parents.iter().enumerate() {
                parents_child.insert(self.parents_offset + i, parent.as_str());
            }
            parents_vec.set_entry(idx, self.parents_offset, parents.len());
            self.parents_offset += parents.len();
        }

        if let Some(decorate_vec) = self.decorate.as_mut() {
            let decorate_child = decorate_vec.child(self.decorate_offset + refs.len());
            for (i, ref_name) in refs.iter().enumerate() {
                decorate_child.insert(self.decorate_offset + i, ref_name.as_str());
            }
            decorate_vec.set_entry(idx, self.decorate_offset, refs.len());
            self.decorate_offset += refs.len();
        }

        if let Some(fc_vec) = self.file_changes.as_mut() {
            let file_changes = &commit.file_changes;
            let len = file_changes.len();
            let total = self.file_changes_offset + len;
            let struct_child = fc_vec.struct_child(total);
            let path = struct_child.child(FileChangeField::Path.index(), total);
            let mut old_path = struct_child.child(FileChangeField::OldPath.index(), total);
            let status = struct_child.child(FileChangeField::Status.index(), total);
            let blob_id = struct_child.child(FileChangeField::BlobId.index(), total);
            let mut file_size = struct_child.child(FileChangeField::FileSize.index(), total);
            let mut add_lines = struct_child.child(FileChangeField::AddLines.index(), total);
            let mut del_lines = struct_child.child(FileChangeField::DelLines.index(), total);
            let off = self.file_changes_offset;
            for (i, fc) in file_changes.iter().enumerate() {
                path.insert(off + i, fc.path.as_str());
                if let Some(ref p) = fc.old_path {
                    old_path.insert(off + i, p.as_str());
                } else {
                    old_path.set_null(off + i);
                }
                status.insert(off + i, fc.status);
                blob_id.insert(off + i, fc.blob_id.as_str());
                unsafe {
                    file_size.as_mut_slice::<i64>()[off + i] = fc.file_size;
                    add_lines.as_mut_slice::<i32>()[off + i] = fc.add_lines;
                    del_lines.as_mut_slice::<i32>()[off + i] = fc.del_lines;
                }
            }
            fc_vec.set_entry(idx, self.file_changes_offset, len);
            self.file_changes_offset += len;
        }
    }

    pub fn finish(mut self) {
        if let Some(parents_vec) = self.parents.as_mut() {
            parents_vec.set_len(self.parents_offset);
        }
        if let Some(decorate_vec) = self.decorate.as_mut() {
            decorate_vec.set_len(self.decorate_offset);
        }
        if let Some(fc_vec) = self.file_changes.as_mut() {
            fc_vec.set_len(self.file_changes_offset);
        }
    }
}

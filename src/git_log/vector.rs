use crate::git::sink::{CommitSink, FileChangeRef};
use crate::git_log::schema::{FileChangeField, GitLogColumn};
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
    parents_insert_at: usize,
    parents_set: bool,
    decorate: Option<ListVector<'a>>,
    decorate_offset: usize,
    decorate_insert_at: usize,
    decorate_set: bool,
    contained_branches: Option<ListVector<'a>>,
    contained_branches_offset: usize,
    contained_branches_insert_at: usize,
    contained_branches_set: bool,
    contained_tags: Option<ListVector<'a>>,
    contained_tags_offset: usize,
    contained_tags_insert_at: usize,
    contained_tags_set: bool,
    file_changes: Option<ListVector<'a>>,
    file_changes_offset: usize,
    file_changes_row_count: usize,
    row_idx: usize,
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
        let mut contained_branches = None;
        let mut contained_tags = None;
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
                Ok(GitLogColumn::ContainedBranches) => {
                    contained_branches = Some(chunk.list_vector(chunk_pos))
                }
                Ok(GitLogColumn::ContainedTags) => {
                    contained_tags = Some(chunk.list_vector(chunk_pos))
                }
                Ok(GitLogColumn::FileChanges) => file_changes = Some(chunk.list_vector(chunk_pos)),
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
            parents_insert_at: 0,
            parents_set: false,
            decorate,
            decorate_offset: 0,
            decorate_insert_at: 0,
            decorate_set: false,
            contained_branches,
            contained_branches_offset: 0,
            contained_branches_insert_at: 0,
            contained_branches_set: false,
            contained_tags,
            contained_tags_offset: 0,
            contained_tags_insert_at: 0,
            contained_tags_set: false,
            file_changes,
            file_changes_offset: 0,
            file_changes_row_count: 0,
            row_idx: 0,
        }
    }

    pub fn finish(mut self) {
        if let Some(parents_vec) = self.parents.as_mut() {
            parents_vec.set_len(self.parents_offset);
        }
        if let Some(decorate_vec) = self.decorate.as_mut() {
            decorate_vec.set_len(self.decorate_offset);
        }
        if let Some(contained_branches_vec) = self.contained_branches.as_mut() {
            contained_branches_vec.set_len(self.contained_branches_offset);
        }
        if let Some(contained_tags_vec) = self.contained_tags.as_mut() {
            contained_tags_vec.set_len(self.contained_tags_offset);
        }
        if let Some(fc_vec) = self.file_changes.as_mut() {
            fc_vec.set_len(self.file_changes_offset);
        }
    }
}

impl CommitSink for VectorInserter<'_> {
    fn begin_row(&mut self, idx: usize) {
        self.row_idx = idx;
        self.parents_set = false;
        self.decorate_set = false;
        self.contained_branches_set = false;
        self.contained_tags_set = false;
        self.file_changes_row_count = 0;
    }

    fn commit_id(&mut self, hex: &[u8]) {
        if let Some(v) = self.commit_id.as_mut() {
            v.insert(self.row_idx, hex);
        }
    }

    fn author(&mut self, name: &[u8], email: &[u8], seconds: i64) {
        if let Some(v) = self.author.as_mut() {
            v.insert(self.row_idx, name);
        }
        if let Some(v) = self.author_email.as_mut() {
            v.insert(self.row_idx, email);
        }
        if let Some(v) = self.author_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[self.row_idx] = seconds * 1_000_000;
            }
        }
    }

    fn committer(&mut self, name: &[u8], email: &[u8], seconds: i64) {
        if let Some(v) = self.committer.as_mut() {
            v.insert(self.row_idx, name);
        }
        if let Some(v) = self.committer_email.as_mut() {
            v.insert(self.row_idx, email);
        }
        if let Some(v) = self.committer_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[self.row_idx] = seconds * 1_000_000;
            }
        }
    }

    fn message(&mut self, msg: &[u8]) {
        if let Some(v) = self.message.as_mut() {
            v.insert(self.row_idx, msg);
        }
    }

    fn begin_parents(&mut self, count: usize) {
        if let Some(parents_vec) = self.parents.as_mut() {
            let _ = parents_vec.child(self.parents_offset + count);
            parents_vec.set_entry(self.row_idx, self.parents_offset, count);
            self.parents_insert_at = self.parents_offset;
            self.parents_offset += count;
            self.parents_set = true;
        }
    }

    fn parent(&mut self, hex: &[u8]) {
        if let Some(parents_vec) = self.parents.as_mut() {
            let child = parents_vec.child(self.parents_offset);
            child.insert(self.parents_insert_at, hex);
            self.parents_insert_at += 1;
        }
    }

    fn begin_decorate(&mut self, count: usize) {
        if let Some(decorate_vec) = self.decorate.as_mut() {
            let _ = decorate_vec.child(self.decorate_offset + count);
            decorate_vec.set_entry(self.row_idx, self.decorate_offset, count);
            self.decorate_insert_at = self.decorate_offset;
            self.decorate_offset += count;
            self.decorate_set = true;
        }
    }

    fn decorate_name(&mut self, name: &str) {
        if let Some(decorate_vec) = self.decorate.as_mut() {
            let child = decorate_vec.child(self.decorate_offset);
            child.insert(self.decorate_insert_at, name);
            self.decorate_insert_at += 1;
        }
    }

    fn begin_contained_branches(&mut self, count: usize) {
        if let Some(vec) = self.contained_branches.as_mut() {
            let _ = vec.child(self.contained_branches_offset + count);
            vec.set_entry(self.row_idx, self.contained_branches_offset, count);
            self.contained_branches_insert_at = self.contained_branches_offset;
            self.contained_branches_offset += count;
            self.contained_branches_set = true;
        }
    }

    fn contained_branch(&mut self, name: &str) {
        if let Some(vec) = self.contained_branches.as_mut() {
            let child = vec.child(self.contained_branches_offset);
            child.insert(self.contained_branches_insert_at, name);
            self.contained_branches_insert_at += 1;
        }
    }

    fn begin_contained_tags(&mut self, count: usize) {
        if let Some(vec) = self.contained_tags.as_mut() {
            let _ = vec.child(self.contained_tags_offset + count);
            vec.set_entry(self.row_idx, self.contained_tags_offset, count);
            self.contained_tags_insert_at = self.contained_tags_offset;
            self.contained_tags_offset += count;
            self.contained_tags_set = true;
        }
    }

    fn contained_tag(&mut self, name: &str) {
        if let Some(vec) = self.contained_tags.as_mut() {
            let child = vec.child(self.contained_tags_offset);
            child.insert(self.contained_tags_insert_at, name);
            self.contained_tags_insert_at += 1;
        }
    }

    fn begin_file_changes(&mut self, count: usize) {
        if let Some(fc_vec) = self.file_changes.as_mut() {
            let _ = fc_vec.struct_child(self.file_changes_offset + count);
        }
    }

    fn file_change(&mut self, fc: FileChangeRef<'_>) {
        let Some(fc_vec) = self.file_changes.as_mut() else {
            return;
        };
        let i = self.file_changes_row_count;
        let total = self.file_changes_offset + i + 1;
        let struct_child = fc_vec.struct_child(total);
        let path = struct_child.child(FileChangeField::Path.index(), total);
        let mut old_path = struct_child.child(FileChangeField::OldPath.index(), total);
        let status = struct_child.child(FileChangeField::Status.index(), total);
        let blob_id = struct_child.child(FileChangeField::BlobId.index(), total);
        let mut file_size = struct_child.child(FileChangeField::FileSize.index(), total);
        let mut add_lines = struct_child.child(FileChangeField::AddLines.index(), total);
        let mut del_lines = struct_child.child(FileChangeField::DelLines.index(), total);
        let off = self.file_changes_offset + i;

        path.insert(off, fc.path);
        if let Some(p) = fc.old_path {
            old_path.insert(off, p);
        } else {
            old_path.set_null(off);
        }
        status.insert(off, fc.status);
        blob_id.insert(off, fc.blob_id);
        if let Some(size) = fc.file_size {
            unsafe {
                file_size.as_mut_slice::<i64>()[off] = size;
            }
        } else {
            file_size.set_null(off);
        }
        if let Some(n) = fc.add_lines {
            unsafe {
                add_lines.as_mut_slice::<i32>()[off] = n;
            }
        } else {
            add_lines.set_null(off);
        }
        if let Some(n) = fc.del_lines {
            unsafe {
                del_lines.as_mut_slice::<i32>()[off] = n;
            }
        } else {
            del_lines.set_null(off);
        }

        self.file_changes_row_count += 1;
    }

    fn finish_row(&mut self) {
        if self.parents.is_some() && !self.parents_set {
            self.begin_parents(0);
        }
        if self.decorate.is_some() && !self.decorate_set {
            self.begin_decorate(0);
        }
        if self.contained_branches.is_some() && !self.contained_branches_set {
            self.begin_contained_branches(0);
        }
        if self.contained_tags.is_some() && !self.contained_tags_set {
            self.begin_contained_tags(0);
        }
        if let Some(fc_vec) = self.file_changes.as_mut() {
            fc_vec.set_entry(
                self.row_idx,
                self.file_changes_offset,
                self.file_changes_row_count,
            );
            self.file_changes_offset += self.file_changes_row_count;
        }
    }
}

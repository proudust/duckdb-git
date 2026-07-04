use crate::git_log::Commit;
use duckdb::core::{DataChunkHandle, FlatVector, Inserter, ListVector};

/// 出力チャンクの各カラムベクターを事前取得し、コミット単位の転記を担うインサーター。
pub struct VectorInserter<'a> {
    commit_id: FlatVector<'a>,
    author: FlatVector<'a>,
    author_email: FlatVector<'a>,
    author_timestamp: FlatVector<'a>,
    committer: FlatVector<'a>,
    committer_email: FlatVector<'a>,
    committer_timestamp: FlatVector<'a>,
    message: FlatVector<'a>,
    parents: ListVector<'a>,
    parents_offset: usize,
    file_changes: Option<ListVector<'a>>,
    file_changes_offset: usize,
}

impl<'a> VectorInserter<'a> {
    pub fn new(chunk: &'a DataChunkHandle, with_file_changes: bool) -> Self {
        VectorInserter {
            commit_id: chunk.flat_vector(0),
            author: chunk.flat_vector(1),
            author_email: chunk.flat_vector(2),
            author_timestamp: chunk.flat_vector(3),
            committer: chunk.flat_vector(4),
            committer_email: chunk.flat_vector(5),
            committer_timestamp: chunk.flat_vector(6),
            message: chunk.flat_vector(7),
            parents: chunk.list_vector(8),
            parents_offset: 0,
            file_changes: with_file_changes.then(|| chunk.list_vector(9)),
            file_changes_offset: 0,
        }
    }

    /// 1コミット分をチャンクの `idx` 行へ転記する。
    pub fn push(
        &mut self,
        idx: usize,
        oid: &str,
        commit: &Commit,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.commit_id.insert(idx, oid);
        self.author.insert(idx, commit.author_name());
        self.author_email.insert(idx, commit.author_email());
        // TIMESTAMP はマイクロ秒で格納
        unsafe {
            self.author_timestamp.as_mut_slice::<i64>()[idx] =
                commit.author_timestamp() * 1_000_000;
        }
        self.committer.insert(idx, commit.committer_name());
        self.committer_email.insert(idx, commit.committer_email());
        unsafe {
            self.committer_timestamp.as_mut_slice::<i64>()[idx] =
                commit.committer_timestamp() * 1_000_000;
        }
        self.message.insert(idx, commit.message());

        // parents (VARCHAR[])
        let parents = commit.parents();
        let parents_child = self.parents.child(self.parents_offset + parents.len());
        for (i, parent) in parents.iter().enumerate() {
            parents_child.insert(self.parents_offset + i, parent.as_str());
        }
        self.parents.set_entry(idx, self.parents_offset, parents.len());
        self.parents_offset += parents.len();

        // file_changes (STRUCT(...)[])
        if let Some(fc_vec) = self.file_changes.as_mut() {
            let file_changes = commit.file_changes()?;
            let len = file_changes.len();
            let total = self.file_changes_offset + len;
            let struct_child = fc_vec.struct_child(total);
            let path = struct_child.child(0, total);
            let status = struct_child.child(1, total);
            let blob_id = struct_child.child(2, total);
            let mut file_size = struct_child.child(3, total);
            let mut add_lines = struct_child.child(4, total);
            let mut del_lines = struct_child.child(5, total);
            let off = self.file_changes_offset;
            for (i, fc) in file_changes.iter().enumerate() {
                path.insert(off + i, fc.path.as_str());
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

        Ok(())
    }

    /// 全行転記後にリストベクターの長さを確定する。
    pub fn finish(mut self) {
        self.parents.set_len(self.parents_offset);
        if let Some(fc_vec) = self.file_changes.as_mut() {
            fc_vec.set_len(self.file_changes_offset);
        }
    }
}

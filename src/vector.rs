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
    file_changes: Option<ListVector<'a>>,
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
            file_changes: with_file_changes.then(|| chunk.list_vector(9)),
        }
    }

    /// 1コミット分をチャンクの `idx` 行へ転記する。
    pub fn push(
        &mut self,
        idx: usize,
        oid: &git2::Oid,
        commit: &Commit,
    ) -> Result<(), git2::Error> {
        self.commit_id.insert(idx, &oid.to_string());
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
        let parents_child = self.parents.child(parents.len());
        for (i, parent) in parents.iter().enumerate() {
            parents_child.insert(i, parent.as_str());
        }
        self.parents.set_entry(idx, 0, parents.len());

        // file_changes (STRUCT(...)[])
        if let Some(fc_vec) = self.file_changes.as_mut() {
            let file_changes = commit.file_changes()?;
            let len = file_changes.len();
            let struct_child = fc_vec.struct_child(len);
            let path = struct_child.child(0, len);
            let status = struct_child.child(1, len);
            let blob_id = struct_child.child(2, len);
            let mut file_size = struct_child.child(3, len);
            let mut add_lines = struct_child.child(4, len);
            let mut del_lines = struct_child.child(5, len);
            for (i, fc) in file_changes.iter().enumerate() {
                path.insert(i, fc.path.as_str());
                status.insert(i, fc.status);
                blob_id.insert(i, fc.blob_id.as_str());
                unsafe {
                    file_size.as_mut_slice::<i64>()[i] = fc.file_size;
                    add_lines.as_mut_slice::<i32>()[i] = fc.add_lines;
                    del_lines.as_mut_slice::<i32>()[i] = fc.del_lines;
                }
            }
            fc_vec.set_entry(idx, 0, len);
        }

        Ok(())
    }

    /// 全行転記後にリストベクターの長さを確定する。
    pub fn finish(mut self, len: usize) {
        self.parents.set_len(len);
        if let Some(fc_vec) = self.file_changes.as_mut() {
            fc_vec.set_len(len);
        }
    }
}

use crate::git::ref_row::BranchRow;
use crate::git_branch::schema::GitBranchColumn;
use duckdb::core::{DataChunkHandle, FlatVector, Inserter};

pub struct BranchVectorInserter<'a> {
    name: Option<FlatVector<'a>>,
    refname: Option<FlatVector<'a>>,
    is_head: Option<FlatVector<'a>>,
    commit_id: Option<FlatVector<'a>>,
    subject: Option<FlatVector<'a>>,
    author: Option<FlatVector<'a>>,
    author_email: Option<FlatVector<'a>>,
    author_timestamp: Option<FlatVector<'a>>,
    committer: Option<FlatVector<'a>>,
    committer_email: Option<FlatVector<'a>>,
    committer_timestamp: Option<FlatVector<'a>>,
    upstream: Option<FlatVector<'a>>,
    upstream_ahead: Option<FlatVector<'a>>,
    upstream_behind: Option<FlatVector<'a>>,
    upstream_gone: Option<FlatVector<'a>>,
    push: Option<FlatVector<'a>>,
    symref_target: Option<FlatVector<'a>>,
    row_idx: usize,
}

impl<'a> BranchVectorInserter<'a> {
    pub fn new(chunk: &'a DataChunkHandle, column_indices: &[u64]) -> Self {
        let mut s = Self {
            name: None,
            refname: None,
            is_head: None,
            commit_id: None,
            subject: None,
            author: None,
            author_email: None,
            author_timestamp: None,
            committer: None,
            committer_email: None,
            committer_timestamp: None,
            upstream: None,
            upstream_ahead: None,
            upstream_behind: None,
            upstream_gone: None,
            push: None,
            symref_target: None,
            row_idx: 0,
        };
        for (chunk_pos, &orig_idx) in column_indices.iter().enumerate() {
            match GitBranchColumn::try_from(orig_idx) {
                Ok(GitBranchColumn::Name) => s.name = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::Refname) => s.refname = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::IsHead) => s.is_head = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::CommitId) => s.commit_id = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::Subject) => s.subject = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::Author) => s.author = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::AuthorEmail) => {
                    s.author_email = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::AuthorTimestamp) => {
                    s.author_timestamp = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::Committer) => s.committer = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::CommitterEmail) => {
                    s.committer_email = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::CommitterTimestamp) => {
                    s.committer_timestamp = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::Upstream) => s.upstream = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::UpstreamAhead) => {
                    s.upstream_ahead = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::UpstreamBehind) => {
                    s.upstream_behind = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::UpstreamGone) => {
                    s.upstream_gone = Some(chunk.flat_vector(chunk_pos))
                }
                Ok(GitBranchColumn::Push) => s.push = Some(chunk.flat_vector(chunk_pos)),
                Ok(GitBranchColumn::SymrefTarget) => {
                    s.symref_target = Some(chunk.flat_vector(chunk_pos))
                }
                Err(()) => {}
            }
        }
        s
    }

    pub fn write_row(&mut self, row: &BranchRow) {
        let idx = self.row_idx;
        if let Some(v) = self.name.as_mut() {
            v.insert(idx, row.name.as_bytes());
        }
        if let Some(v) = self.refname.as_mut() {
            v.insert(idx, row.refname.as_bytes());
        }
        if let Some(v) = self.is_head.as_mut() {
            unsafe {
                v.as_mut_slice::<bool>()[idx] = row.is_head;
            }
        }
        if let Some(v) = self.commit_id.as_mut() {
            v.insert(idx, row.commit_id.as_bytes());
        }
        if let Some(v) = self.subject.as_mut() {
            v.insert(idx, row.subject.as_bytes());
        }
        if let Some(v) = self.author.as_mut() {
            v.insert(idx, row.author.as_bytes());
        }
        if let Some(v) = self.author_email.as_mut() {
            v.insert(idx, row.author_email.as_bytes());
        }
        if let Some(v) = self.author_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[idx] = row.author_timestamp * 1_000_000;
            }
        }
        if let Some(v) = self.committer.as_mut() {
            v.insert(idx, row.committer.as_bytes());
        }
        if let Some(v) = self.committer_email.as_mut() {
            v.insert(idx, row.committer_email.as_bytes());
        }
        if let Some(v) = self.committer_timestamp.as_mut() {
            unsafe {
                v.as_mut_slice::<i64>()[idx] = row.committer_timestamp * 1_000_000;
            }
        }
        write_optional_str(&mut self.upstream, idx, row.upstream.as_deref());
        write_optional_i64(&mut self.upstream_ahead, idx, row.upstream_ahead);
        write_optional_i64(&mut self.upstream_behind, idx, row.upstream_behind);
        write_optional_bool(&mut self.upstream_gone, idx, row.upstream_gone);
        write_optional_str(&mut self.push, idx, row.push.as_deref());
        write_optional_str(&mut self.symref_target, idx, row.symref_target.as_deref());
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

fn write_optional_i64(v: &mut Option<FlatVector<'_>>, idx: usize, value: Option<i64>) {
    if let Some(vec) = v {
        match value {
            Some(n) => unsafe {
                vec.as_mut_slice::<i64>()[idx] = n;
            },
            None => vec.set_null(idx),
        }
    }
}

fn write_optional_bool(v: &mut Option<FlatVector<'_>>, idx: usize, value: Option<bool>) {
    if let Some(vec) = v {
        match value {
            Some(b) => unsafe {
                vec.as_mut_slice::<bool>()[idx] = b;
            },
            None => vec.set_null(idx),
        }
    }
}

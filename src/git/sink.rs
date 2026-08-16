/// Borrowed file-change fields valid for the duration of one `file_change` call.
pub struct FileChangeRef<'a> {
    pub path: &'a [u8],
    pub old_path: Option<&'a [u8]>,
    pub status: &'static str,
    pub blob_id: &'a [u8],
    pub file_size: Option<i64>,
    pub add_lines: Option<i32>,
    pub del_lines: Option<i32>,
}

/// Write destination for one commit's columns.
///
/// Backends call these while git objects are still alive; implementations copy
/// into their own storage (e.g. DuckDB vectors) before returning from each method.
pub trait CommitSink {
    fn begin_row(&mut self, idx: usize);
    fn commit_id(&mut self, hex: &[u8]);
    fn author(&mut self, name: &[u8], email: &[u8], seconds: i64);
    fn committer(&mut self, name: &[u8], email: &[u8], seconds: i64);
    fn message(&mut self, msg: &[u8]);
    fn begin_parents(&mut self, count: usize);
    fn parent(&mut self, hex: &[u8]);
    fn begin_decorate(&mut self, count: usize);
    fn decorate_name(&mut self, name: &str);
    fn begin_contained_branches(&mut self, count: usize);
    fn contained_branch(&mut self, name: &str);
    fn begin_contained_tags(&mut self, count: usize);
    fn contained_tag(&mut self, name: &str);
    /// Reserve space for upcoming [`file_change`](Self::file_change) calls when the
    /// count is known. Default is a no-op; sinks may use this to avoid per-item growth.
    fn begin_file_changes(&mut self, _count: usize) {}
    fn file_change(&mut self, fc: FileChangeRef<'_>);
    fn finish_row(&mut self);
}

/// Format a SHA-1 object id (20 bytes) as lowercase hex into a stack buffer.
///
/// Panics if `oid.len() != 20`. SHA-256 (32-byte) support would widen this API later.
pub fn oid_hex(oid: &[u8]) -> [u8; 40] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    assert_eq!(oid.len(), 20, "expected SHA-1 object id (20 bytes)");
    let mut out = [0u8; 40];
    for (i, &b) in oid.iter().enumerate() {
        out[i * 2] = HEX[(b >> 4) as usize];
        out[i * 2 + 1] = HEX[(b & 0xf) as usize];
    }
    out
}

#[cfg(test)]
#[derive(Clone, Default)]
#[allow(dead_code)]
pub struct FileChange {
    pub path: String,
    pub old_path: Option<String>,
    pub status: &'static str,
    pub blob_id: String,
    pub file_size: Option<i64>,
    pub add_lines: Option<i32>,
    pub del_lines: Option<i32>,
}

#[cfg(test)]
#[derive(Default)]
#[allow(dead_code)]
pub struct CommitData {
    pub commit_id: String,
    pub author_name: Vec<u8>,
    pub author_email: Vec<u8>,
    pub author_timestamp: i64,
    pub committer_name: Vec<u8>,
    pub committer_email: Vec<u8>,
    pub committer_timestamp: i64,
    pub message: Vec<u8>,
    pub parents: Vec<String>,
    pub decorate: Vec<String>,
    pub contained_branches: Vec<String>,
    pub contained_tags: Vec<String>,
    pub file_changes: Vec<FileChange>,
}

/// Test helper that rebuilds owned commit data from sink events.
#[cfg(test)]
#[derive(Default)]
pub struct CollectingSink {
    pub row: CommitData,
}

#[cfg(test)]
impl CommitSink for CollectingSink {
    fn begin_row(&mut self, _idx: usize) {
        self.row = CommitData::default();
    }

    fn commit_id(&mut self, hex: &[u8]) {
        self.row.commit_id = String::from_utf8_lossy(hex).into_owned();
    }

    fn author(&mut self, name: &[u8], email: &[u8], seconds: i64) {
        self.row.author_name = name.to_vec();
        self.row.author_email = email.to_vec();
        self.row.author_timestamp = seconds;
    }

    fn committer(&mut self, name: &[u8], email: &[u8], seconds: i64) {
        self.row.committer_name = name.to_vec();
        self.row.committer_email = email.to_vec();
        self.row.committer_timestamp = seconds;
    }

    fn message(&mut self, msg: &[u8]) {
        self.row.message = msg.to_vec();
    }

    fn begin_parents(&mut self, count: usize) {
        self.row.parents.clear();
        self.row.parents.reserve(count);
    }

    fn parent(&mut self, hex: &[u8]) {
        self.row
            .parents
            .push(String::from_utf8_lossy(hex).into_owned());
    }

    fn begin_decorate(&mut self, count: usize) {
        self.row.decorate.clear();
        self.row.decorate.reserve(count);
    }

    fn decorate_name(&mut self, name: &str) {
        self.row.decorate.push(name.to_owned());
    }

    fn begin_contained_branches(&mut self, count: usize) {
        self.row.contained_branches.clear();
        self.row.contained_branches.reserve(count);
    }

    fn contained_branch(&mut self, name: &str) {
        self.row.contained_branches.push(name.to_owned());
    }

    fn begin_contained_tags(&mut self, count: usize) {
        self.row.contained_tags.clear();
        self.row.contained_tags.reserve(count);
    }

    fn contained_tag(&mut self, name: &str) {
        self.row.contained_tags.push(name.to_owned());
    }

    fn begin_file_changes(&mut self, count: usize) {
        self.row.file_changes.reserve(count);
    }

    fn file_change(&mut self, fc: FileChangeRef<'_>) {
        self.row.file_changes.push(FileChange {
            path: String::from_utf8_lossy(fc.path).into_owned(),
            old_path: fc
                .old_path
                .map(|p| String::from_utf8_lossy(p).into_owned()),
            status: fc.status,
            blob_id: String::from_utf8_lossy(fc.blob_id).into_owned(),
            file_size: fc.file_size,
            add_lines: fc.add_lines,
            del_lines: fc.del_lines,
        });
    }

    fn finish_row(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::oid_hex;

    #[test]
    fn oid_hex_formats_sha1() {
        let mut oid = [0u8; 20];
        oid[0] = 0x00;
        oid[1] = 0x01;
        oid[2] = 0x0a;
        oid[3] = 0x0f;
        oid[4] = 0xff;
        let hex = oid_hex(&oid);
        assert_eq!(&hex[..10], b"00010a0fff");
        assert_eq!(&hex[10..], &[b'0'; 30]);
    }

    #[test]
    #[should_panic(expected = "expected SHA-1 object id (20 bytes)")]
    fn oid_hex_rejects_wrong_length() {
        let _ = oid_hex(&[0u8; 32]);
    }
}

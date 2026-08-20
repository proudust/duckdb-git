/// git diff --numstat convention for gitlink (submodule) entries.
pub fn gitlink_numstat(status: &str) -> (i32, i32) {
    match status {
        "A" => (1, 0),
        "D" => (0, 1),
        "M" | "R" | "T" | "C" => (1, 1),
        _ => (0, 0),
    }
}

/// Match `git log --numstat` when a blob object is missing (`diff.c`: `unable to read %s`).
pub fn unable_to_read_object(oid: impl std::fmt::Display) -> String {
    format!("unable to read {oid}")
}

/// git's binary heuristic: NUL in the first 8000 bytes (`buffer_is_binary`).
pub fn is_binary_content(content: &[u8]) -> bool {
    let len = content.len().min(8000);
    content[..len].contains(&0)
}

/// Numstat for a tree delta whose old and new blob OIDs are equal
/// (chmod, or a content-identical rename/copy).
///
/// Matches `git log --numstat` / `builtin_diffstat` (`may_differ = !oideq`):
/// text is `0/0`, binary is `-/-` (NULL/NULL). Does not run Myers / `xdl_diff`.
pub fn same_oid_numstat(content: &[u8]) -> (Option<i32>, Option<i32>) {
    if is_binary_content(content) {
        (None, None)
    } else {
        (Some(0), Some(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_oid_numstat_skips_myers() {
        assert_eq!(same_oid_numstat(b"hello\nworld\n"), (Some(0), Some(0)));
        assert_eq!(same_oid_numstat(b""), (Some(0), Some(0)));
        assert_eq!(same_oid_numstat(b"\x00binary"), (None, None));
        // NUL past the 8KB window is still text.
        let mut text = vec![b'a'; 8000];
        text.push(0);
        assert_eq!(same_oid_numstat(&text), (Some(0), Some(0)));
        let mut bin = vec![b'a'; 7999];
        bin.push(0);
        assert_eq!(same_oid_numstat(&bin), (None, None));
    }
}

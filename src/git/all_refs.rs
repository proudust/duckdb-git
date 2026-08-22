/// Whether a ref name is included by `git log --all` under this extension's
/// intentional subset: `refs/heads`, `refs/tags`, and `refs/remotes` only.
pub(crate) fn is_log_all_ref(name: &str) -> bool {
    name.starts_with("refs/heads/")
        || name.starts_with("refs/tags/")
        || name.starts_with("refs/remotes/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_heads_tags_remotes() {
        assert!(is_log_all_ref("refs/heads/main"));
        assert!(is_log_all_ref("refs/tags/v1.0"));
        assert!(is_log_all_ref("refs/remotes/origin/main"));
    }

    #[test]
    fn excludes_notes_stash_and_other_namespaces() {
        assert!(!is_log_all_ref("refs/notes/commits"));
        assert!(!is_log_all_ref("refs/stash"));
        assert!(!is_log_all_ref("refs/bisect/bad"));
        assert!(!is_log_all_ref("refs/worktree/xyz/HEAD"));
        assert!(!is_log_all_ref("HEAD"));
        assert!(!is_log_all_ref("refs/heads")); // missing trailing slash
    }
}

use super::blob_ring::BlobRing;
use super::diff::emit_file_changes;
use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
use git2::Repository;
use std::error::Error;

fn push_all_tips(repo: &Repository, revwalk: &mut git2::Revwalk<'_>) -> Result<(), Box<dyn Error>> {
    // Unborn HEAD: skip and still walk other ref tips (like `git log --all`).
    let _ = revwalk.push_head();
    for reference in repo.references()? {
        let reference = reference?;
        let name = reference.name().unwrap_or("");
        if !crate::git::all_refs::is_log_all_ref(name) {
            continue;
        }
        // Symbolic refs (e.g. refs/remotes/origin/HEAD) and non-commit tips: skip.
        if let Ok(commit) = reference.peel_to_commit() {
            revwalk.push(commit.id())?;
        }
    }
    Ok(())
}

pub(crate) fn walk_commit_oids(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<Vec<git2::Oid>, Box<dyn Error>> {
    let mut revwalk = repo.revwalk()?;

    if all_refs {
        push_all_tips(repo, &mut revwalk)?;
    } else if revision.is_none() {
        revwalk.push_head()?;
    }

    if let Some(terms) = revision {
        for term in terms {
            let obj = repo
                .revparse_single(&term.spec)
                .map_err(|_| -> Box<dyn Error> {
                    unresolved_revision_error(&term.origin).into()
                })?;
            // Peel annotated tags (and anything else) to a commit, like `git log <rev>`.
            let id = obj
                .peel_to_commit()
                .map(|c| c.id())
                .map_err(|e| -> Box<dyn Error> {
                    format!(
                        "revision '{}' does not resolve to a commit: {e}",
                        term.origin
                    )
                    .into()
                })?;
            if term.negate {
                revwalk.hide(id)?;
            } else {
                revwalk.push(id)?;
            }
        }
    }

    if first_parent {
        revwalk.simplify_first_parent()?;
    }

    let commit_oids: Result<Vec<git2::Oid>, _> = match max_count {
        Some(count) => revwalk.take(count).collect(),
        None => revwalk.collect(),
    };
    Ok(commit_oids?)
}

pub(crate) fn emit_commit(
    repo: &Repository,
    oid: git2::Oid,
    ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
    rename_threshold: Option<u16>,
    sink: &mut impl CommitSink,
    ring: &mut BlobRing,
) -> Result<(), Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;
    let header = commit_header(commit.raw_header_bytes());
    let author = parse_ident(header, b"author")?;
    let committer = parse_ident(header, b"committer")?;

    let hex = oid_hex(oid.as_bytes());
    sink.commit_id(&hex);
    sink.author(author.name, author.email, author.seconds);
    sink.committer(committer.name, committer.email, committer.seconds);
    sink.message(commit.message_raw_bytes());

    let parent_count = commit.parent_count();
    sink.begin_parents(parent_count);
    for i in 0..parent_count {
        let parent_hex = oid_hex(commit.parent_id(i)?.as_bytes());
        sink.parent(&parent_hex);
    }

    let skip = skip_file_changes || (diff_merges == DiffMerges::Off && parent_count > 1);
    if !skip {
        emit_file_changes(
            repo,
            &commit,
            ignore_all_space,
            rename_threshold,
            sink,
            ring,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::sink::CollectingSink;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const PARITY: &str = "test/fixtures/parity.git";
    const MISSING_BLOB: &str = "test/fixtures/missing-blob.git";

    fn peel_commit(repo: &Repository, spec: &str) -> git2::Oid {
        repo.revparse_single(spec)
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .id()
    }

    fn emit(
        repo: &Repository,
        oid: git2::Oid,
        skip_file_changes: bool,
        diff_merges: DiffMerges,
        ring: &mut BlobRing,
    ) -> Result<(), Box<dyn Error>> {
        let mut sink = CollectingSink::default();
        sink.begin_row(0);
        let result = emit_commit(
            repo,
            oid,
            false,
            skip_file_changes,
            diff_merges,
            None,
            &mut sink,
            ring,
        );
        sink.finish_row();
        result
    }

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = Repository::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        let mut skipped = CollectingSink::default();
        skipped.begin_row(0);
        let mut skip_ring = BlobRing::new();
        emit_commit(
            &repo,
            oid,
            false,
            true,
            DiffMerges::FirstParent,
            None,
            &mut skipped,
            &mut skip_ring,
        )
        .unwrap();
        skipped.finish_row();
        assert!(skipped.row.file_changes.is_empty());

        let mut kept = CollectingSink::default();
        kept.begin_row(0);
        let mut keep_ring = BlobRing::new();
        emit_commit(
            &repo,
            oid,
            false,
            false,
            DiffMerges::FirstParent,
            None,
            &mut kept,
            &mut keep_ring,
        )
        .unwrap();
        kept.finish_row();
        assert!(!kept.row.file_changes.is_empty());
    }

    #[test]
    fn note_commit_finishes_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "note");
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(ring.finish_count() >= 1);
    }

    #[test]
    fn root_commit_runs_epilogue() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "v1");
        assert_eq!(repo.find_commit(oid).unwrap().parent_count(), 0);
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(ring.finish_count() >= 1);
        assert_eq!(ring.len(), 0);
    }

    /// Invalid UTF-8 filename; both root and parent diffs must keep raw bytes.
    const NON_UTF8_PATH: &[u8] = b"\xff.txt";

    fn index_entry(path: &[u8]) -> git2::IndexEntry {
        git2::IndexEntry {
            ctime: git2::IndexTime::new(0, 0),
            mtime: git2::IndexTime::new(0, 0),
            dev: 0,
            ino: 0,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            file_size: 0,
            id: git2::Oid::ZERO_SHA1,
            flags: 0,
            flags_extended: 0,
            path: path.to_vec(),
        }
    }

    /// Keeps raw `file_change` paths (CollectingSink is UTF-8-lossy).
    #[derive(Default)]
    struct PathSink {
        paths: Vec<Vec<u8>>,
    }

    impl CommitSink for PathSink {
        fn begin_row(&mut self, _idx: usize) {}
        fn commit_id(&mut self, _hex: &[u8]) {}
        fn author(&mut self, _name: &[u8], _email: &[u8], _seconds: i64) {}
        fn committer(&mut self, _name: &[u8], _email: &[u8], _seconds: i64) {}
        fn message(&mut self, _msg: &[u8]) {}
        fn begin_parents(&mut self, _count: usize) {}
        fn parent(&mut self, _hex: &[u8]) {}
        fn begin_decorate(&mut self, _count: usize) {}
        fn decorate_name(&mut self, _name: &str) {}
        fn begin_contained_branches(&mut self, _count: usize) {}
        fn contained_branch(&mut self, _name: &str) {}
        fn begin_contained_tags(&mut self, _count: usize) {}
        fn contained_tag(&mut self, _name: &str) {}
        fn file_change(&mut self, fc: crate::git::sink::FileChangeRef<'_>) {
            self.paths.push(fc.path.to_vec());
        }
        fn finish_row(&mut self) {}
    }

    fn emit_paths(repo: &Repository, oid: git2::Oid) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let mut sink = PathSink::default();
        sink.begin_row(0);
        emit_commit(
            repo,
            oid,
            false,
            false,
            DiffMerges::Off,
            None,
            &mut sink,
            &mut BlobRing::new(),
        )?;
        sink.finish_row();
        Ok(sink.paths)
    }

    /// Parent-diff emits non-UTF-8 paths via `path_bytes()`.
    #[test]
    fn non_root_commit_emits_non_utf8_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        let empty = repo.treebuilder(None).unwrap().write().unwrap();
        let empty_tree = repo.find_tree(empty).unwrap();
        let root = repo
            .commit(None, &sig, &sig, "empty root", &empty_tree, &[])
            .unwrap();
        let root_commit = repo.find_commit(root).unwrap();

        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(&index_entry(NON_UTF8_PATH), b"hello\n")
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let child = repo
            .commit(None, &sig, &sig, "add non-utf8", &tree, &[&root_commit])
            .unwrap();

        let paths = emit_paths(&repo, child).unwrap();
        assert!(
            paths.iter().any(|p| p.as_slice() == NON_UTF8_PATH),
            "parent-diff must keep non-UTF-8 paths; got {paths:?}"
        );
    }

    /// Root commits also go through tree-to-tree + `path_bytes()` (not `entry.name()`).
    #[test]
    fn root_commit_emits_non_utf8_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(&index_entry(NON_UTF8_PATH), b"hello\n")
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let root = repo
            .commit(None, &sig, &sig, "root with non-utf8", &tree, &[])
            .unwrap();
        assert_eq!(repo.find_commit(root).unwrap().parent_count(), 0);

        let paths = emit_paths(&repo, root).unwrap();
        assert!(
            paths.iter().any(|p| p.as_slice() == NON_UTF8_PATH),
            "root commit must emit non-UTF-8 paths like parent-diff; got {paths:?}"
        );
    }

    #[test]
    fn rename_inserts_old_blob_bytes() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "rename");
        let parent = repo.find_commit(oid).unwrap().parent(0).unwrap();
        let old_id = parent.tree().unwrap().get_name("note.txt").unwrap().id();
        let expected = repo.find_blob(old_id).unwrap().content().to_vec();

        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(ring.len() >= 1);
        assert_eq!(ring.lookup(old_id), Some(expected.as_slice()));
        assert!(ring.contains_path(b"note.txt"));
        assert!(!ring.contains_path(b"renamed.txt"));
    }

    #[test]
    fn merge_off_does_not_touch_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "merged");
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 0);
    }

    #[test]
    fn skip_file_changes_does_not_finish_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "note");
        let mut ring = BlobRing::new();
        emit(&repo, oid, true, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 0);
    }

    #[test]
    fn missing_blob_does_not_mutate_ring() {
        let repo = Repository::open(MISSING_BLOB).unwrap();
        let oid = repo.head().unwrap().peel_to_commit().unwrap().id();
        let mut ring = BlobRing::new();
        assert!(emit(&repo, oid, false, DiffMerges::Off, &mut ring).is_err());
        assert_eq!(ring.finish_count(), 0);
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn err_after_old_miss_does_not_finish_pending() {
        // `a.txt` sorts before `b.bin` so the keep-file old is pending before
        // the missing blob fails (path-sorted deltas).
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        std::fs::write(dir.path().join("a.txt"), "v1\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let c1 = repo
            .commit(Some("HEAD"), &sig, &sig, "one", &tree, &[])
            .unwrap();
        let parent = repo.find_commit(c1).unwrap();
        let old_keep = tree.get_name("a.txt").unwrap().id();

        std::fs::write(dir.path().join("a.txt"), "v2\n").unwrap();
        std::fs::write(dir.path().join("b.bin"), "gone\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.add_path(std::path::Path::new("b.bin")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let c2 = repo
            .commit(Some("HEAD"), &sig, &sig, "two", &tree, &[&parent])
            .unwrap();
        let z_id = tree.get_name("b.bin").unwrap().id();
        let hex = z_id.to_string();
        std::fs::remove_file(
            dir.path()
                .join(".git/objects")
                .join(&hex[..2])
                .join(&hex[2..]),
        )
        .unwrap();

        let mut ring = BlobRing::new();
        emit(&repo, c1, false, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 1);
        assert!(emit(&repo, c2, false, DiffMerges::Off, &mut ring).is_err());
        assert_eq!(ring.finish_count(), 1);
        assert!(ring.lookup(old_keep).is_none());
    }
}

#[cfg(test)]
mod walk_all_refs_tests {
    use super::walk_commit_oids;
    use git2::Repository;

    const PARITY: &str = "test/fixtures/parity.git";

    #[test]
    fn walk_all_refs_matches_rev_list_all() {
        let repo = Repository::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        assert_eq!(oids.len(), 14);
        let default = walk_commit_oids(&repo, None, None, false, false).unwrap();
        assert_eq!(default.len(), 10);
        let orphan = git2::Oid::from_str("8a2afdc773a23dcd4aeb85aee134cd884f9463f9").unwrap();
        assert!(oids.contains(&orphan));
        assert!(!default.contains(&orphan));
    }

    #[test]
    fn walk_all_refs_peels_annotated_tag_tip() {
        let repo = Repository::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        let v1 = git2::Oid::from_str("ff09a62b129cc936f13bc67c5e2dba84f397c64b").unwrap();
        assert!(oids.contains(&v1));
    }
}

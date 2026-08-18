use super::blob_ring::BlobRing;
use super::diff::emit_file_changes;
use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
use git2::Repository;
use std::error::Error;

pub(crate) fn walk_commit_oids(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
) -> Result<Vec<git2::Oid>, Box<dyn Error>> {
    let mut revwalk = repo.revwalk()?;

    match revision {
        Some(terms) => {
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
        None => {
            revwalk.push_head()?;
        }
    }

    let revwalk_iter: Box<dyn Iterator<Item = _>> = match max_count {
        Some(count) => Box::new(revwalk.take(count)),
        None => Box::new(revwalk),
    };

    let mut commit_oids = Vec::new();
    for oid in revwalk_iter {
        commit_oids.push(oid?);
    }

    Ok(commit_oids)
}

pub(crate) fn emit_commit(
    repo: &Repository,
    oid: git2::Oid,
    ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
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
        emit_file_changes(repo, &commit, ignore_all_space, sink, ring)?;
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
            &mut kept,
            &mut keep_ring,
        )
        .unwrap();
        kept.finish_row();
        assert!(!kept.row.file_changes.is_empty());
    }

    #[test]
    fn note_commit_advances_ring_generation() {
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
    fn merge_off_does_not_advance_generation() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "merged");
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 0);
    }

    #[test]
    fn skip_file_changes_does_not_advance_generation() {
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

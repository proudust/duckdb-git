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
                let id = obj.peel_to_commit().map(|c| c.id()).map_err(|e| -> Box<dyn Error> {
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
        emit_file_changes(repo, &commit, ignore_all_space, sink)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::sink::CollectingSink;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = Repository::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        let mut skipped = CollectingSink::default();
        skipped.begin_row(0);
        emit_commit(&repo, oid, false, true, DiffMerges::FirstParent, &mut skipped).unwrap();
        skipped.finish_row();
        assert!(skipped.row.file_changes.is_empty());

        let mut kept = CollectingSink::default();
        kept.begin_row(0);
        emit_commit(&repo, oid, false, false, DiffMerges::FirstParent, &mut kept).unwrap();
        kept.finish_row();
        assert!(!kept.row.file_changes.is_empty());
    }
}

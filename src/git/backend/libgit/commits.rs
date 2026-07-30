use super::diff::collect_file_changes;
use crate::git::model::CommitData;
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
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
                if term.negate {
                    revwalk.hide(obj.id())?;
                } else {
                    revwalk.push(obj.id())?;
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

pub(crate) fn read_commit(
    repo: &Repository,
    oid: git2::Oid,
    ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
) -> Result<CommitData, Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;
    let author = commit.author();
    let committer = commit.committer();

    let skip = skip_file_changes || (diff_merges == DiffMerges::Off && commit.parent_count() > 1);
    let file_changes = if skip {
        Vec::new()
    } else {
        collect_file_changes(repo, &commit, ignore_all_space)?
    };

    Ok(CommitData {
        author_name: author.name_bytes().to_vec(),
        author_email: author.email_bytes().to_vec(),
        author_timestamp: author.when().seconds(),
        committer_name: committer.name_bytes().to_vec(),
        committer_email: committer.email_bytes().to_vec(),
        committer_timestamp: committer.when().seconds(),
        message: commit.message_bytes().to_vec(),
        parents: (0..commit.parent_count())
            .map(|i| commit.parent_id(i).unwrap().to_string())
            .collect(),
        file_changes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = Repository::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        let skipped = read_commit(&repo, oid, false, true, DiffMerges::FirstParent).unwrap();
        assert!(skipped.file_changes.is_empty());

        let kept = read_commit(&repo, oid, false, false, DiffMerges::FirstParent).unwrap();
        assert!(!kept.file_changes.is_empty());
    }
}

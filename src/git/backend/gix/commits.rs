use super::diff::collect_file_changes;
use crate::git::ident::{commit_header, parse_ident};
use crate::git::model::CommitData;
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use std::error::Error;

pub(crate) fn walk_commit_oids(
    repo: &gix::Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
    let (tips, hidden) = match revision {
        Some(terms) => {
            let mut tips = Vec::new();
            let mut hidden = Vec::new();
            for term in terms {
                let id = repo
                    .rev_parse_single(term.spec.as_str())
                    .map_err(|_| -> Box<dyn Error> {
                        unresolved_revision_error(&term.origin).into()
                    })?
                    .detach();
                if term.negate {
                    hidden.push(id);
                } else {
                    tips.push(id);
                }
            }
            (tips, hidden)
        }
        None => (vec![repo.head_id()?.detach()], Vec::new()),
    };

    let walk = repo.rev_walk(tips).with_hidden(hidden);
    let all = walk.all()?;

    let oids: Result<Vec<gix::ObjectId>, Box<dyn Error>> = match max_count {
        Some(count) => all.take(count).map(|info| Ok(info?.id)).collect(),
        None => all.map(|info| Ok(info?.id)).collect(),
    };

    oids
}

pub(crate) fn read_commit(
    repo: &gix::Repository,
    oid: gix::ObjectId,
    // TODO: gix does not yet support ignore_all_space option for diffs
    _ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
) -> Result<CommitData, Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;
    let header = commit_header(commit.data.as_ref());
    let author = parse_ident(header, b"author")?;
    let committer = parse_ident(header, b"committer")?;
    let message = commit.message_raw()?;

    let skip =
        skip_file_changes || (diff_merges == DiffMerges::Off && commit.parent_ids().count() > 1);
    let file_changes = if skip {
        Vec::new()
    } else {
        collect_file_changes(repo, &commit)?
    };

    Ok(CommitData {
        author_name: author.name,
        author_email: author.email,
        author_timestamp: author.seconds,
        committer_name: committer.name,
        committer_email: committer.email,
        committer_timestamp: committer.seconds,
        message: message.to_vec(),
        parents: commit.parent_ids().map(|id| id.to_string()).collect(),
        file_changes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = gix::open(".").unwrap();
        let oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();

        let skipped = read_commit(&repo, oid, false, true, DiffMerges::FirstParent).unwrap();
        assert!(skipped.file_changes.is_empty());

        let kept = read_commit(&repo, oid, false, false, DiffMerges::FirstParent).unwrap();
        assert!(!kept.file_changes.is_empty());
    }
}

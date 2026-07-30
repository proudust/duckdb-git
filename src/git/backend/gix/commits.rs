use super::diff::collect_file_changes;
use crate::git::model::CommitData;
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use std::error::Error;

pub(super) fn walk_commit_oids(
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

pub(super) fn read_commit(
    repo: &gix::Repository,
    oid: gix::ObjectId,
    // TODO: gix does not yet support ignore_all_space option for diffs
    _ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
) -> Result<CommitData, Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;

    let author_name = commit.author().map(|a| a.name.to_vec()).unwrap_or_default();
    let author_email = commit
        .author()
        .map(|a| a.email.to_vec())
        .unwrap_or_default();
    let author_timestamp = commit
        .author()
        .ok()
        .and_then(|a| a.time().ok())
        .map(|t| t.seconds)
        .unwrap_or(0);

    let committer_name = commit
        .committer()
        .map(|a| a.name.to_vec())
        .unwrap_or_default();
    let committer_email = commit
        .committer()
        .map(|a| a.email.to_vec())
        .unwrap_or_default();
    let committer_timestamp = commit
        .committer()
        .ok()
        .and_then(|a| a.time().ok())
        .map(|t| t.seconds)
        .unwrap_or(0);

    let message = commit.message_raw_sloppy().to_vec();
    let parents = commit.parent_ids().map(|id| id.to_string()).collect();

    let skip =
        skip_file_changes || (diff_merges == DiffMerges::Off && commit.parent_ids().count() > 1);
    let file_changes = if skip {
        Vec::new()
    } else {
        collect_file_changes(repo, &commit)?
    };

    Ok(CommitData {
        author_name,
        author_email,
        author_timestamp,
        committer_name,
        committer_email,
        committer_timestamp,
        message,
        parents,
        file_changes,
    })
}

#[cfg(test)]
mod tests {
    use super::super::SECOND_COMMIT;
    use super::*;

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

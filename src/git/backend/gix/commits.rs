use super::diff::emit_file_changes;
use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
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
                // Peel annotated tags like `git log <rev>` (`v1` → underlying commit).
                let spec = if term.spec.contains("^{") {
                    term.spec.clone()
                } else {
                    format!("{}^{{commit}}", term.spec)
                };
                let id = repo
                    .rev_parse_single(spec.as_str())
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

pub(crate) fn emit_commit(
    repo: &gix::Repository,
    oid: gix::ObjectId,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
    sink: &mut impl CommitSink,
) -> Result<(), Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;
    let header = commit_header(commit.data.as_ref());
    let author = parse_ident(header, b"author")?;
    let committer = parse_ident(header, b"committer")?;
    let message = commit.message_raw()?;

    let hex = oid_hex(oid.as_bytes());
    sink.commit_id(&hex);
    sink.author(author.name, author.email, author.seconds);
    sink.committer(committer.name, committer.email, committer.seconds);
    sink.message(message);

    let parent_ids: Vec<_> = commit.parent_ids().collect();
    sink.begin_parents(parent_ids.len());
    for parent_id in &parent_ids {
        let parent_hex = oid_hex(parent_id.as_bytes());
        sink.parent(&parent_hex);
    }

    let skip = skip_file_changes || (diff_merges == DiffMerges::Off && parent_ids.len() > 1);
    if !skip {
        emit_file_changes(repo, &commit, sink)?;
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
        let repo = gix::open(".").unwrap();
        let oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();

        let mut skipped = CollectingSink::default();
        skipped.begin_row(0);
        emit_commit(&repo, oid, true, DiffMerges::FirstParent, &mut skipped).unwrap();
        skipped.finish_row();
        assert!(skipped.row.file_changes.is_empty());

        let mut kept = CollectingSink::default();
        kept.begin_row(0);
        emit_commit(&repo, oid, false, DiffMerges::FirstParent, &mut kept).unwrap();
        kept.finish_row();
        assert!(!kept.row.file_changes.is_empty());
    }
}

#[cfg(test)]
mod peel_tests {
    use super::walk_commit_oids;
    use crate::git::revision::RevisionTerm;

    #[test]
    fn walk_peels_annotated_tag() {
        let repo = gix::open("test/fixtures/parity.git").unwrap();
        let terms = [RevisionTerm {
            spec: "v1".into(),
            negate: false,
            origin: "v1".into(),
        }];
        let oids = walk_commit_oids(&repo, Some(&terms), Some(1)).unwrap();
        assert_eq!(oids.len(), 1);
        let obj = repo.find_object(oids[0]).unwrap();
        assert_eq!(obj.kind, gix::object::Kind::Commit);
        assert_eq!(
            oids[0].to_string(),
            "ff09a62b129cc936f13bc67c5e2dba84f397c64b"
        );
    }
}

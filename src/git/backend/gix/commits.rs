use super::diff::emit_file_changes;
use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
use std::error::Error;

fn collect_all_tips(repo: &gix::Repository) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
    let mut tips = Vec::new();
    // Unborn HEAD: skip and still walk other ref tips (like `git log --all`).
    if let Ok(head) = repo.head_id() {
        tips.push(head.detach());
    }
    let platform = repo.references()?;
    for reference in platform.all()? {
        let mut reference = reference.map_err(|e| e.to_string())?;
        let name = reference.name().as_bstr().to_string();
        if !crate::git::all_refs::is_log_all_ref(&name) {
            continue;
        }
        if let Ok(commit) = reference.peel_to_commit() {
            tips.push(commit.id);
        }
    }
    Ok(tips)
}

pub(crate) fn walk_commit_oids(
    repo: &gix::Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
    // Same two-stage order as libgit: all_refs tips first, then revision push/hide.
    let (mut tips, mut hidden) = if all_refs {
        (collect_all_tips(repo)?, Vec::new())
    } else if revision.is_none() {
        (vec![repo.head_id()?.detach()], Vec::new())
    } else {
        (Vec::new(), Vec::new())
    };

    if let Some(terms) = revision {
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
    }

    let mut walk = repo.rev_walk(tips).with_hidden(hidden);
    if first_parent {
        walk = walk.first_parent_only();
    }
    // Rename iterator binding so it does not shadow the `all_refs` parameter.
    let walk_iter = walk.all()?;

    let oids: Result<Vec<gix::ObjectId>, Box<dyn Error>> = match max_count {
        Some(count) => walk_iter.take(count).map(|info| Ok(info?.id)).collect(),
        None => walk_iter.map(|info| Ok(info?.id)).collect(),
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
        let oids = walk_commit_oids(&repo, Some(&terms), Some(1), false, false).unwrap();
        assert_eq!(oids.len(), 1);
        let obj = repo.find_object(oids[0]).unwrap();
        assert_eq!(obj.kind, gix::object::Kind::Commit);
        assert_eq!(
            oids[0].to_string(),
            "ff09a62b129cc936f13bc67c5e2dba84f397c64b"
        );
    }

    #[test]
    fn walk_all_refs_matches_rev_list_all() {
        let repo = gix::open("test/fixtures/parity.git").unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        assert_eq!(oids.len(), 14);
        let default = walk_commit_oids(&repo, None, None, false, false).unwrap();
        assert_eq!(default.len(), 10);
        // Orphan tip `common-tail` is only reachable via --all.
        let orphan = gix::ObjectId::from_hex(b"8a2afdc773a23dcd4aeb85aee134cd884f9463f9").unwrap();
        assert!(oids.contains(&orphan));
        assert!(!default.contains(&orphan));
    }

    #[test]
    fn walk_all_refs_peels_annotated_tag_tip() {
        let repo = gix::open("test/fixtures/parity.git").unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        let v1 = gix::ObjectId::from_hex(b"ff09a62b129cc936f13bc67c5e2dba84f397c64b").unwrap();
        assert!(oids.contains(&v1));
    }
}

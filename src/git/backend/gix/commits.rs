use super::diff::emit_file_changes;
use crate::git::date_walk::{oid_bytes_from_slice, walk_by_commit_date, OidBytes};
use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
use std::collections::HashSet;
use std::error::Error;

fn oid_to_bytes(oid: gix::ObjectId) -> OidBytes {
    oid_bytes_from_slice(oid.as_bytes()).expect("gix oid is 20 bytes")
}

fn bytes_to_oid(bytes: OidBytes) -> gix::ObjectId {
    gix::ObjectId::from(bytes)
}

/// Collect all_refs tip OIDs in `git log --all` seed order: raw refname bytes, then HEAD.
fn collect_all_tips(repo: &gix::Repository) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
    // Sort key is raw bytes (git `for_each_ref`), matching libgit `name_bytes()`.
    let mut named: Vec<(Vec<u8>, gix::ObjectId)> = Vec::new();
    let platform = repo.references()?;
    for reference in platform.all()? {
        let mut reference = reference.map_err(|e| e.to_string())?;
        let name = reference.name().as_bstr().to_vec();
        if !crate::git::all_refs::is_log_all_ref_bytes(&name) {
            continue;
        }
        if let Ok(commit) = reference.peel_to_commit() {
            named.push((name, commit.id));
        }
    }
    named.sort_by(|a, b| a.0.cmp(&b.0));

    let mut tips: Vec<gix::ObjectId> = named.into_iter().map(|(_, id)| id).collect();

    // Unborn HEAD: skip and still walk other ref tips (like `git log --all`).
    // Peel like libgit (`head.peel_to_commit`) so annotated-tag detached HEAD stays a commit tip.
    if let Ok(mut head) = repo.head() {
        if let Ok(commit) = head.peel_to_commit() {
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
    // Only `tips` seed the date PQ; `hidden` is used solely for interesting-set construction.
    let (mut tips, mut hidden) = if all_refs {
        (collect_all_tips(repo)?, Vec::new())
    } else if revision.is_none() {
        let mut head = repo.head()?;
        (vec![head.peel_to_commit()?.id], Vec::new())
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

    let tips_for_pq = tips.clone();
    let mut walk = repo.rev_walk(tips).with_hidden(hidden);
    if first_parent {
        walk = walk.first_parent_only();
    }
    let walk_iter = walk.all()?;

    // Interesting set: full revwalk, never truncated by max_count.
    let interesting: HashSet<OidBytes> = walk_iter
        .map(|info| Ok(oid_to_bytes(info?.id)))
        .collect::<Result<HashSet<_>, Box<dyn Error>>>()?;

    let tip_bytes: Vec<OidBytes> = tips_for_pq.into_iter().map(oid_to_bytes).collect();

    let ordered = walk_by_commit_date(
        tip_bytes,
        &interesting,
        max_count,
        |id| {
            let commit = repo.find_commit(bytes_to_oid(id))?;
            let parent_ids: Vec<_> = commit.parent_ids().collect();
            let n = if first_parent {
                parent_ids.len().min(1)
            } else {
                parent_ids.len()
            };
            Ok(parent_ids
                .into_iter()
                .take(n)
                .map(|p| oid_to_bytes(p.detach()))
                .collect())
        },
        |id| {
            let commit = repo.find_commit(bytes_to_oid(id))?;
            let header = commit_header(commit.data.as_ref());
            Ok(parse_ident(header, b"committer")?.seconds)
        },
    )?;

    Ok(ordered.into_iter().map(bytes_to_oid).collect())
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

    const SECOND_COMMIT: &str = "48b38a077c6ef03744be652baee901373a8ae06d";
    const PARITY: &str = "test/fixtures/parity.git";
    const V1_COMMIT: &str = "ff09a62b129cc936f13bc67c5e2dba84f397c64b";
    const ORPHAN: &str = "8a2afdc773a23dcd4aeb85aee134cd884f9463f9";

    #[test]
    fn walk_peels_annotated_tag() {
        let repo = gix::open(PARITY).unwrap();
        let terms = [RevisionTerm {
            spec: "v1".into(),
            negate: false,
            origin: "v1".into(),
        }];
        let oids = walk_commit_oids(&repo, Some(&terms), Some(1), false, false).unwrap();
        assert_eq!(oids.len(), 1);
        assert_eq!(oids[0].to_string(), V1_COMMIT);
    }

    #[test]
    fn walk_all_refs_matches_rev_list_all() {
        let repo = gix::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        assert_eq!(oids.len(), 14);
        let default = walk_commit_oids(&repo, None, None, false, false).unwrap();
        assert_eq!(default.len(), 10);
        let orphan = gix::ObjectId::from_hex(ORPHAN.as_bytes()).unwrap();
        assert!(oids.contains(&orphan));
        assert!(!default.contains(&orphan));
    }

    #[test]
    fn walk_all_refs_peels_annotated_tag_tip() {
        let repo = gix::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        let v1 = gix::ObjectId::from_hex(V1_COMMIT.as_bytes()).unwrap();
        assert!(oids.contains(&v1));
    }

    #[test]
    fn all_tips_seed_order_is_refname_then_head() {
        let repo = gix::open(PARITY).unwrap();
        let tips = collect_all_tips(&repo).unwrap();

        let mut expected_named: Vec<(Vec<u8>, gix::ObjectId)> = Vec::new();
        let platform = repo.references().unwrap();
        for reference in platform.all().unwrap() {
            let mut reference = reference.unwrap();
            let name = reference.name().as_bstr().to_vec();
            if !crate::git::all_refs::is_log_all_ref_bytes(&name) {
                continue;
            }
            if let Ok(commit) = reference.peel_to_commit() {
                expected_named.push((name, commit.id));
            }
        }
        expected_named.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<_> = expected_named.into_iter().map(|(_, id)| id).collect();
        if let Ok(mut head) = repo.head() {
            if let Ok(commit) = head.peel_to_commit() {
                expected.push(commit.id);
            }
        }
        assert_eq!(tips, expected, "all_refs tips must be refname-byte-sorted then HEAD");
    }

    #[cfg(feature = "libgit-backend")]
    #[test]
    fn walk_order_matches_libgit_on_parity_all_refs() {
        let gix_repo = gix::open(PARITY).unwrap();
        let gix_oids = walk_commit_oids(&gix_repo, None, None, false, true).unwrap();
        let libgit_oids: Vec<String> = {
            let repo = git2::Repository::open(PARITY).unwrap();
            crate::git::backend::libgit::walk_commit_oids(&repo, None, None, false, true)
                .unwrap()
                .into_iter()
                .map(|o| o.to_string())
                .collect()
        };
        let gix_hex: Vec<String> = gix_oids.iter().map(|o| o.to_string()).collect();
        assert_eq!(gix_hex, libgit_oids);
    }

    #[test]
    fn emit_commit_reads_message() {
        let repo = gix::open(PARITY).unwrap();
        let oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();
        let mut sink = CollectingSink::default();
        sink.begin_row(0);
        emit_commit(&repo, oid, true, DiffMerges::Off, &mut sink).unwrap();
        assert!(!sink.row.message.is_empty());
    }
}

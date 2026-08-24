use super::diff::emit_file_changes;
use crate::git::date_walk::{oid_bytes_from_slice, CommitDateWalk, DateWalkCallbacks, OidBytes};
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

fn resolve_revision_tips(
    repo: &gix::Repository,
    revision: Option<&[RevisionTerm]>,
    all_refs: bool,
) -> Result<(Vec<gix::ObjectId>, Vec<gix::ObjectId>), Box<dyn Error>> {
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

    Ok((tips, hidden))
}

/// Inputs for a date-ordered commit walk (tips + optional hide filter).
pub(crate) struct WalkPrepared {
    pub tips: Vec<OidBytes>,
    pub interesting: Option<HashSet<OidBytes>>,
    pub first_parent: bool,
    pub max_count: Option<usize>,
}

pub(crate) fn prepare_walk(
    repo: &gix::Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<WalkPrepared, Box<dyn Error>> {
    let has_hide = revision.is_some_and(|t| t.iter().any(|x| x.negate));
    let (tips, hidden) = resolve_revision_tips(repo, revision, all_refs)?;

    let tip_bytes: Vec<OidBytes> = tips.iter().copied().map(oid_to_bytes).collect();

    let interesting = if has_hide {
        let mut walk = repo.rev_walk(tips).with_hidden(hidden);
        if first_parent {
            walk = walk.first_parent_only();
        }
        let walk_iter = walk.all()?;
        Some(
            walk_iter
                .map(|info| Ok(oid_to_bytes(info?.id)))
                .collect::<Result<HashSet<_>, Box<dyn Error>>>()?,
        )
    } else {
        drop(hidden);
        None
    };

    Ok(WalkPrepared {
        tips: tip_bytes,
        interesting,
        first_parent,
        max_count,
    })
}

pub(crate) struct GixWalkCallbacks<'a> {
    repo: &'a gix::Repository,
    first_parent: bool,
}

impl DateWalkCallbacks for GixWalkCallbacks<'_> {
    fn parents(&mut self, id: OidBytes) -> Result<Vec<OidBytes>, Box<dyn Error>> {
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_walker_find_commit();
        let commit = self.repo.find_commit(bytes_to_oid(id))?;
        let parent_ids: Vec<_> = commit.parent_ids().collect();
        let n = if self.first_parent {
            parent_ids.len().min(1)
        } else {
            parent_ids.len()
        };
        Ok(parent_ids
            .into_iter()
            .take(n)
            .map(|p| oid_to_bytes(p.detach()))
            .collect())
    }

    fn committer_seconds(&mut self, id: OidBytes) -> Result<i64, Box<dyn Error>> {
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::record_walker_find_commit();
        let commit = self.repo.find_commit(bytes_to_oid(id))?;
        let header = commit_header(commit.data.as_ref());
        Ok(parse_ident(header, b"committer")?.seconds)
    }
}

pub(crate) fn run_commit_date_walk(
    repo: &gix::Repository,
    prep: WalkPrepared,
    mut on_oid: impl FnMut(OidBytes) -> Result<bool, String>,
) -> Result<(), String> {
    let mut callbacks = GixWalkCallbacks {
        repo,
        first_parent: prep.first_parent,
    };
    let mut walk = CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )
    .map_err(|e| e.to_string())?;

    while let Some(oid) = walk.next(&mut callbacks).map_err(|e| e.to_string())? {
        if !on_oid(oid)? {
            break;
        }
    }
    Ok(())
}

/// Start a date walk that can be resumed across calls with a fresh repo borrow.
pub(crate) fn start_commit_date_walk(
    repo: &gix::Repository,
    prep: WalkPrepared,
) -> Result<CommitDateWalk, Box<dyn Error>> {
    let mut callbacks = GixWalkCallbacks {
        repo,
        first_parent: prep.first_parent,
    };
    CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )
}

pub(crate) fn walk_next_oid(
    repo: &gix::Repository,
    first_parent: bool,
    walk: &mut CommitDateWalk,
) -> Result<Option<OidBytes>, Box<dyn Error>> {
    let mut callbacks = GixWalkCallbacks { repo, first_parent };
    walk.next(&mut callbacks)
}

pub(crate) fn walk_commit_oids(
    repo: &gix::Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
    let prep = prepare_walk(repo, revision, max_count, first_parent, all_refs)?;
    let first_parent = prep.first_parent;
    let mut callbacks = GixWalkCallbacks {
        repo,
        first_parent,
    };
    let ordered = CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )?
    .drain_into_vec(&mut callbacks)?;

    Ok(ordered.into_iter().map(bytes_to_oid).collect())
}

pub(crate) fn emit_commit(
    repo: &gix::Repository,
    oid: gix::ObjectId,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
    sink: &mut impl CommitSink,
) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "prefetch-stats")]
    crate::git::diag::record_emit_find_commit();
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
    fn walk_with_hide_keeps_interesting_filter() {
        let repo = gix::open(PARITY).unwrap();
        let terms = [
            RevisionTerm {
                spec: "rename".into(),
                negate: false,
                origin: "rename".into(),
            },
            RevisionTerm {
                spec: "note".into(),
                negate: true,
                origin: "^note".into(),
            },
        ];
        let oids = walk_commit_oids(&repo, Some(&terms), None, false, false).unwrap();
        assert_eq!(oids.len(), 1);
        assert_eq!(
            oids[0].to_string(),
            "95937d42365c812ebe6893e756cde1d0d86ae10b"
        );
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

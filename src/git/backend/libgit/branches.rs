use crate::git::ident::{commit_header, parse_ident};
use crate::git::ref_filter::{
    oid_matches_points_at, passes_commit_filters, RefFilterParams, ResolvedCommitFilters,
    ResolvedPointsAt,
};
use crate::git::ref_list::{BranchListOpts, BranchScope};
use crate::git::ref_name::branch_display_name;
use crate::git::ref_row::BranchRow;
use crate::git::sink::oid_hex_str;
use git2::{BranchType, ObjectType, Oid, Repository};
use std::error::Error;

pub(crate) fn list_branches(
    repo: &Repository,
    opts: &BranchListOpts<'_>,
) -> Result<Vec<BranchRow>, Box<dyn Error>> {
    let commit_filters = resolve_commit_filters(repo, opts.filter)?;
    let points_at = resolve_points_at(repo, opts.filter)?;

    let branch_type = match opts.scope {
        BranchScope::Local => Some(BranchType::Local),
        BranchScope::Remote => Some(BranchType::Remote),
        BranchScope::All => None,
    };

    let mut rows = Vec::new();
    for branch in repo.branches(branch_type)? {
        let (branch, _typ) = branch?;
        let reference = branch.get();
        let refname = reference.name()?.to_string();

        if !refname.starts_with("refs/heads/") && !refname.starts_with("refs/remotes/") {
            continue;
        }

        let symref_target = if opts.need_symref {
            reference
                .symbolic_target()
                .ok()
                .flatten()
                .map(|t| branch_display_name(t, opts.format))
        } else {
            None
        };

        let tip_commit = match reference.peel_to_commit() {
            Ok(c) => c.id(),
            Err(_) => continue,
        };
        let tip_bytes: [u8; 20] = tip_commit.as_bytes().try_into().expect("20 byte oid");

        if !commit_filters.is_empty()
            && !passes_commit_filters(&tip_bytes, &commit_filters, &|ancestor, tip| {
                is_ancestor(repo, ancestor, tip)
            })?
        {
            continue;
        }

        if !points_at.oids.is_empty() {
            let peeled = reference.peel(ObjectType::Any)?;
            let direct: [u8; 20] = peeled
                .id()
                .as_bytes()
                .try_into()
                .expect("20 byte oid");
            let peel_chain = ref_peel_chain(repo, &peeled)?;
            if !oid_matches_points_at(&direct, &peel_chain, &points_at) {
                continue;
            }
        }

        let is_head = branch.is_head();

        let (
            subject,
            author,
            author_email,
            author_timestamp,
            committer,
            committer_email,
            committer_timestamp,
        ) = if opts.need_tip_meta {
            let commit = repo.find_commit(tip_commit)?;
            let header = commit_header(commit.raw_header_bytes());
            let author = parse_ident(header, b"author")?;
            let committer = parse_ident(header, b"committer")?;
            let subject = commit.summary()?.unwrap_or("").to_string();
            (
                subject,
                String::from_utf8_lossy(author.name).into_owned(),
                String::from_utf8_lossy(author.email).into_owned(),
                author.seconds,
                String::from_utf8_lossy(committer.name).into_owned(),
                String::from_utf8_lossy(committer.email).into_owned(),
                committer.seconds,
            )
        } else {
            (
                String::new(),
                String::new(),
                String::new(),
                0,
                String::new(),
                String::new(),
                0,
            )
        };

        let mut upstream = None;
        let mut upstream_ahead = None;
        let mut upstream_behind = None;
        let mut upstream_gone = None;
        let mut push = None;

        if refname.starts_with("refs/heads/") {
            if opts.need_upstream || opts.need_ahead_behind {
                match repo.branch_upstream_name(&refname) {
                    Ok(upstream_name) => {
                        let upstream_refname = upstream_name.as_str()?;
                        upstream = Some(branch_display_name(upstream_refname, opts.format));
                        match repo.find_reference(upstream_refname) {
                            Ok(upstream_ref) => match upstream_ref.peel_to_commit() {
                                Ok(upstream_tip) => {
                                    upstream_gone = Some(false);
                                    if opts.need_ahead_behind {
                                        let (ahead, behind) =
                                            repo.graph_ahead_behind(tip_commit, upstream_tip.id())?;
                                        upstream_ahead = Some(ahead as i64);
                                        upstream_behind = Some(behind as i64);
                                    }
                                }
                                Err(_) => upstream_gone = Some(true),
                            },
                            Err(_) => upstream_gone = Some(true),
                        }
                    }
                    Err(_) => upstream_gone = None,
                }
            }
            if opts.need_push {
                push = resolve_push_ref(repo, &refname, opts.format);
            }
        }

        rows.push(BranchRow {
            name: branch_display_name(&refname, opts.format),
            refname,
            is_head,
            commit_id: oid_hex_str(&tip_bytes),
            subject,
            author,
            author_email,
            author_timestamp,
            committer,
            committer_email,
            committer_timestamp,
            upstream,
            upstream_ahead,
            upstream_behind,
            upstream_gone,
            push,
            symref_target,
        });
    }

    rows.sort_by(|a, b| a.refname.cmp(&b.refname));
    Ok(rows)
}

fn resolve_commit_filters(
    repo: &Repository,
    filter: &RefFilterParams,
) -> Result<ResolvedCommitFilters, Box<dyn Error>> {
    Ok(ResolvedCommitFilters {
        contains: resolve_revspecs_to_commits(repo, &filter.contains)?,
        no_contains: resolve_revspecs_to_commits(repo, &filter.no_contains)?,
        merged: resolve_revspecs_to_commits(repo, &filter.merged)?,
        no_merged: resolve_revspecs_to_commits(repo, &filter.no_merged)?,
    })
}

fn resolve_points_at(
    repo: &Repository,
    filter: &RefFilterParams,
) -> Result<ResolvedPointsAt, Box<dyn Error>> {
    let mut oids = Vec::new();
    for spec in &filter.points_at {
        let obj = repo.revparse_single(spec)?;
        oids.push(obj.id().as_bytes().try_into().expect("20 byte oid"));
    }
    Ok(ResolvedPointsAt { oids })
}

fn resolve_revspecs_to_commits(
    repo: &Repository,
    specs: &[String],
) -> Result<Vec<[u8; 20]>, Box<dyn Error>> {
    let mut out = Vec::new();
    for spec in specs {
        let obj = repo.revparse_single(spec)?;
        let commit = obj.peel_to_commit().map_err(|_| {
            format!("'{spec}' is not committish (required for contains/merged filters)")
        })?;
        out.push(commit.id().as_bytes().try_into().expect("20 byte oid"));
    }
    Ok(out)
}

fn is_ancestor(
    repo: &Repository,
    ancestor: &[u8; 20],
    tip: &[u8; 20],
) -> Result<bool, Box<dyn Error>> {
    if ancestor == tip {
        return Ok(true);
    }
    let ancestor_oid = Oid::from_bytes(ancestor)?;
    let tip_oid = Oid::from_bytes(tip)?;
    let mut walk = repo.revwalk()?;
    walk.push(tip_oid)?;
    for oid in walk {
        if oid? == ancestor_oid {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ref_peel_chain(repo: &Repository, obj: &git2::Object<'_>) -> Result<Vec<[u8; 20]>, Box<dyn Error>> {
    let mut chain = Vec::new();
    let mut current = obj.clone();
    for _ in 0..32 {
        if current.kind() != Some(ObjectType::Tag) {
            break;
        }
        let tag = current.as_tag().expect("tag");
        let next = tag.target_id();
        chain.push(next.as_bytes().try_into().expect("20 byte oid"));
        current = repo.find_object(next, None)?;
    }
    Ok(chain)
}

fn resolve_push_ref(repo: &Repository, refname: &str, format: crate::git::options::DecorateFormat) -> Option<String> {
    let short = refname.strip_prefix("refs/heads/")?;
    let config = repo.config().ok()?;
    let push_remote = config
        .get_string(&format!("branch.{short}.pushRemote"))
        .ok()
        .or_else(|| config.get_string("remote.pushDefault").ok());
    let remote = push_remote.or_else(|| config.get_string(&format!("branch.{short}.remote")).ok())?;
    let push_ref = format!("refs/remotes/{remote}/{short}");
    Some(branch_display_name(&push_ref, format))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::options::DecorateFormat;

    const PARITY: &str = "test/fixtures/parity.git";

    #[test]
    fn lists_local_and_remote_branches() {
        let repo = Repository::open(PARITY).unwrap();
        let filter = RefFilterParams::default();
        let rows = list_branches(
            &repo,
            &BranchListOpts {
                scope: BranchScope::All,
                format: DecorateFormat::Short,
                filter: &filter,
                need_tip_meta: true,
                need_upstream: false,
                need_push: false,
                need_symref: true,
                need_ahead_behind: false,
            },
        )
        .unwrap();
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"master"));
        assert!(names.contains(&"side"));
        assert!(names.contains(&"remotes/origin/main"));
    }

    #[test]
    fn upstream_tracking_and_gone() {
        let repo = Repository::open(PARITY).unwrap();
        let filter = RefFilterParams::default();
        let rows = list_branches(
            &repo,
            &BranchListOpts {
                scope: BranchScope::Local,
                format: DecorateFormat::Short,
                filter: &filter,
                need_tip_meta: false,
                need_upstream: true,
                need_push: false,
                need_symref: false,
                need_ahead_behind: true,
            },
        )
        .unwrap();
        let master = rows.iter().find(|r| r.name == "master").unwrap();
        assert_eq!(master.upstream.as_deref(), Some("remotes/origin/main"));
        assert_eq!(master.upstream_ahead, Some(6));
        assert_eq!(master.upstream_behind, Some(0));
        assert_eq!(master.upstream_gone, Some(false));

        let side = rows.iter().find(|r| r.name == "side").unwrap();
        assert_eq!(side.upstream.as_deref(), Some("remotes/origin/gone"));
        assert_eq!(side.upstream_gone, Some(true));
    }

    #[test]
    fn upstream_with_push_not_computed() {
        let repo = Repository::open(PARITY).unwrap();
        let filter = RefFilterParams::default();
        let rows = list_branches(
            &repo,
            &BranchListOpts {
                scope: BranchScope::Local,
                format: DecorateFormat::Short,
                filter: &filter,
                need_tip_meta: false,
                need_upstream: true,
                need_push: true,
                need_symref: false,
                need_ahead_behind: true,
            },
        )
        .unwrap();
        let side = rows.iter().find(|r| r.name == "side").unwrap();
        assert_eq!(side.upstream.as_deref(), Some("remotes/origin/gone"));
        assert_eq!(side.upstream_gone, Some(true));
    }
}

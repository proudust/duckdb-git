use crate::git::ident::{commit_header, parse_ident};
use crate::git::options::DecorateFormat;
use crate::git::ref_filter::{
    passes_commit_filters, oid_matches_points_at, RefFilterParams, ResolvedCommitFilters,
    ResolvedPointsAt,
};
use crate::git::ref_name::branch_display_name;
use crate::git::ref_row::BranchRow;
use crate::git::sink::oid_hex_str;
use gix::refs::Target;
use gix::Repository;
use std::error::Error;

use crate::git::ref_list::{BranchListOpts, BranchScope};

pub(crate) fn list_branches(
    repo: &Repository,
    opts: &BranchListOpts<'_>,
) -> Result<Vec<BranchRow>, Box<dyn Error>> {
    let commit_filters = resolve_commit_filters(repo, opts.filter)?;
    let points_at = resolve_points_at(repo, opts.filter)?;

    let platform = repo.references()?;
    let mut refs = Vec::new();
    match opts.scope {
        BranchScope::Local => {
            for r in platform.local_branches()? {
                refs.push(r.map_err(|e| e.to_string())?);
            }
        }
        BranchScope::Remote => {
            for r in platform.remote_branches()? {
                refs.push(r.map_err(|e| e.to_string())?);
            }
        }
        BranchScope::All => {
            for r in platform.local_branches()? {
                refs.push(r.map_err(|e| e.to_string())?);
            }
            for r in platform.remote_branches()? {
                refs.push(r.map_err(|e| e.to_string())?);
            }
        }
    }

    let head_ref = repo
        .head()
        .ok()
        .and_then(|h| h.referent_name().map(|n| n.as_bstr().to_string()));

    let mut rows = Vec::new();
    for mut reference in refs {
        let refname = reference.name().as_bstr().to_string();
        if !refname.starts_with("refs/heads/") && !refname.starts_with("refs/remotes/") {
            continue;
        }

        let symref_target = if opts.need_symref {
            match &reference.inner.target {
                Target::Symbolic(name) => Some(branch_display_name(
                    std::str::from_utf8(name.as_bstr()).expect("utf8 refname"),
                    opts.format,
                )),
                Target::Object(_) => None,
            }
        } else {
            None
        };

        let direct_oid = reference.peel_to_id()?.detach();
        let direct_bytes = direct_oid.as_bytes().try_into().expect("20 byte oid");

        let tip_commit = match reference.peel_to_commit() {
            Ok(c) => c.id,
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

        if !points_at.oids.is_empty() && !oid_matches_points_at(&direct_bytes, &[], &points_at) {
            continue;
        }

        let is_head = head_ref.as_deref() == Some(refname.as_str());

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
            let header = commit_header(commit.data.as_ref());
            let author = parse_ident(header, b"author")?;
            let committer = parse_ident(header, b"committer")?;
            let subject = commit
                .message()
                .map(|m| m.title.to_string())
                .unwrap_or_default();
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
            let full_name = gix::refs::FullName::try_from(refname.as_str()).ok();
            if let Some(full_name) = full_name {
                if opts.need_upstream || opts.need_ahead_behind {
                    if let Some(Ok(tracking)) = repo.branch_remote_tracking_ref_name(
                        full_name.as_ref(),
                        gix::remote::Direction::Fetch,
                    ) {
                        upstream = Some(branch_display_name(
                            std::str::from_utf8(tracking.as_bstr()).expect("utf8 refname"),
                            opts.format,
                        ));
                        if let Ok(mut upstream_ref) = repo.find_reference(&tracking) {
                            match upstream_ref.peel_to_commit() {
                                Ok(upstream_tip) => {
                                    upstream_gone = Some(false);
                                    if opts.need_ahead_behind {
                                        let (ahead, behind) = ahead_behind(
                                            repo,
                                            &tip_bytes,
                                            upstream_tip.id.as_bytes().try_into().unwrap(),
                                        )?;
                                        upstream_ahead = Some(ahead);
                                        upstream_behind = Some(behind);
                                    }
                                }
                                Err(_) => upstream_gone = Some(true),
                            }
                        } else {
                            upstream_gone = Some(true);
                        }
                    }
                }
                if opts.need_push {
                    if let Some(Ok(push_ref)) = repo.branch_remote_tracking_ref_name(
                        full_name.as_ref(),
                        gix::remote::Direction::Push,
                    ) {
                        push = Some(branch_display_name(
                            std::str::from_utf8(push_ref.as_bstr()).expect("utf8 refname"),
                            opts.format,
                        ));
                    } else {
                        push = resolve_push_ref(repo, &refname, opts.format);
                    }
                }
            }
        }

        rows.push(BranchRow {
            name: branch_display_name(&refname, opts.format),
            refname,
            is_head,
            commit_id: oid_hex_str(tip_commit.as_bytes()),
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

fn ahead_behind(
    repo: &Repository,
    local: &[u8; 20],
    upstream: &[u8; 20],
) -> Result<(i64, i64), Box<dyn Error>> {
    let local_id = gix::ObjectId::from(*local);
    let upstream_id = gix::ObjectId::from(*upstream);
    if local_id == upstream_id {
        return Ok((0, 0));
    }
    Ok((
        count_rev_list_range(repo, upstream_id, local_id)?,
        count_rev_list_range(repo, local_id, upstream_id)?,
    ))
}

/// Equivalent to `git rev-list --count base..tip`.
fn count_rev_list_range(
    repo: &Repository,
    base: gix::ObjectId,
    tip: gix::ObjectId,
) -> Result<i64, Box<dyn Error>> {
    let mut count = 0i64;
    let base_bytes: [u8; 20] = base.as_bytes().try_into().expect("20 byte oid");
    for info in repo.rev_walk([tip]).all()? {
        let id = info?.id;
        let id_bytes: [u8; 20] = id.as_bytes().try_into().expect("20 byte oid");
        if is_ancestor(repo, &id_bytes, &base_bytes)? {
            continue;
        }
        count += 1;
    }
    Ok(count)
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
        let id = repo.rev_parse_single(spec.as_str())?.detach();
        oids.push(id.as_bytes().try_into().expect("20 byte oid"));
    }
    Ok(ResolvedPointsAt { oids })
}

fn resolve_revspecs_to_commits(
    repo: &Repository,
    specs: &[String],
) -> Result<Vec<[u8; 20]>, Box<dyn Error>> {
    let mut out = Vec::new();
    for spec in specs {
        let spec = if spec.contains("^{") {
            spec.clone()
        } else {
            format!("{spec}^{{commit}}")
        };
        let id = repo
            .rev_parse_single(spec.as_str())
            .map_err(|_| format!("'{spec}' is not committish (required for contains/merged filters)"))?
            .detach();
        out.push(id.as_bytes().try_into().expect("20 byte oid"));
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
    let tip_id = gix::ObjectId::from(*tip);
    let ancestor_id = gix::ObjectId::from(*ancestor);
    for info in repo.rev_walk([tip_id]).all()? {
        let info = info?;
        if info.id == ancestor_id {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resolve_push_ref(
    repo: &Repository,
    refname: &str,
    format: crate::git::options::DecorateFormat,
) -> Option<String> {
    let short = refname.strip_prefix("refs/heads/")?;
    let remote = repo.branch_remote_name(short, gix::remote::Direction::Push)?;
    let push_ref = format!("refs/remotes/{}/{}", remote.as_bstr(), short);
    Some(branch_display_name(
        std::str::from_utf8(push_ref.as_bytes()).ok()?,
        format,
    ))
}

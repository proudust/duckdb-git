use crate::git::ref_filter::{
    oid_matches_points_at, passes_commit_filters, RefFilterParams, ResolvedCommitFilters,
    ResolvedPointsAt,
};
use crate::git::ref_name::{tag_display_name, tag_message_without_signature};
use crate::git::ref_row::TagRow;
use crate::git::sink::oid_hex_str;
use gix::object::Kind;
use gix::Repository;
use std::error::Error;

use crate::git::ref_list::TagListOpts;

pub(crate) fn list_tags(
    repo: &Repository,
    opts: &TagListOpts<'_>,
) -> Result<Vec<TagRow>, Box<dyn Error>> {
    let commit_filters = resolve_commit_filters(repo, opts.filter)?;
    let points_at = resolve_points_at(repo, opts.filter)?;

    let platform = repo.references()?;
    let mut rows = Vec::new();
    for reference in platform.tags()? {
        let mut reference = reference.map_err(|e| e.to_string())?;
        let refname = reference.name().as_bstr().to_string();

        let direct_oid = reference
            .try_id()
            .ok_or("tag ref missing target oid")?
            .detach();
        let direct_bytes: [u8; 20] = direct_oid.as_bytes().try_into().expect("20 byte oid");

        let peel_chain = tag_peel_chain(repo, direct_oid)?;
        let commit_id = reference
            .peel_to_commit()
            .ok()
            .map(|c| oid_hex_str(c.id.as_bytes()));

        if let Some(ref commit_hex) = commit_id {
            let tip_bytes = hex_to_bytes(commit_hex)?;
            if !commit_filters.is_empty()
                && !passes_commit_filters(&tip_bytes, &commit_filters, &|ancestor, tip| {
                    is_ancestor(repo, ancestor, tip)
                })?
            {
                continue;
            }
        } else if !commit_filters.is_empty() {
            continue;
        }

        if !points_at.oids.is_empty()
            && !oid_matches_points_at(&direct_bytes, &peel_chain, &points_at)
        {
            continue;
        }

        let object_type = match repo.find_object(direct_oid).ok().map(|o| o.kind) {
            Some(Kind::Commit) => "commit",
            Some(Kind::Tag) => "tag",
            Some(Kind::Tree) => "tree",
            Some(Kind::Blob) => "blob",
            _ => "unknown",
        }
        .to_string();

        let is_annotated = object_type == "tag";

        let (tagger, tagger_email, tagger_timestamp, message, signature) =
            if is_annotated && opts.need_annotated_meta {
                let tag = repo.find_tag(direct_oid)?;
                let decoded = tag.decode()?;
                let raw_message = decoded.message.to_string();
                let signature = decoded.pgp_signature.map(|s| s.to_string());
                let message = tag_message_without_signature(&raw_message);
                if let Some(sig) = decoded.tagger()? {
                    (
                        Some(sig.name.to_string()),
                        Some(sig.email.to_string()),
                        Some(sig.time()?.seconds),
                        Some(message),
                        signature,
                    )
                } else {
                    (None, None, None, Some(message), signature)
                }
            } else {
                (None, None, None, None, None)
            };

        rows.push(TagRow {
            name: tag_display_name(&refname, opts.format),
            refname,
            object_id: oid_hex_str(&direct_bytes),
            object_type,
            commit_id,
            is_annotated,
            tagger,
            tagger_email,
            tagger_timestamp,
            message,
            signature,
        });
    }

    rows.sort_by(|a, b| a.refname.cmp(&b.refname));
    Ok(rows)
}

fn hex_to_bytes(hex: &str) -> Result<[u8; 20], Box<dyn Error>> {
    let mut out = [0u8; 20];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        if i >= 20 {
            break;
        }
        let s = std::str::from_utf8(chunk)?;
        out[i] = u8::from_str_radix(s, 16)?;
    }
    Ok(out)
}

fn tag_peel_chain(repo: &Repository, direct: gix::ObjectId) -> Result<Vec<[u8; 20]>, Box<dyn Error>> {
    let mut chain = Vec::new();
    let mut current = direct;
    for _ in 0..32 {
        let obj = repo.find_object(current)?;
        if obj.kind != Kind::Tag {
            break;
        }
        let tag = obj.into_tag();
        current = tag.target_id()?.into();
        chain.push(current.as_bytes().try_into().expect("20 byte oid"));
    }
    Ok(chain)
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

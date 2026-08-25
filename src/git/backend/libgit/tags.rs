use crate::git::ref_filter::{
    oid_matches_points_at, passes_commit_filters, RefFilterParams, ResolvedCommitFilters,
    ResolvedPointsAt,
};
use crate::git::ref_list::TagListOpts;
use crate::git::ref_name::{tag_display_name, tag_message_without_signature};
use crate::git::ref_row::TagRow;
use crate::git::sink::oid_hex_str;
use git2::{ObjectType, Oid, Repository};
use std::error::Error;

pub(crate) fn list_tags(
    repo: &Repository,
    opts: &TagListOpts<'_>,
) -> Result<Vec<TagRow>, Box<dyn Error>> {
    let commit_filters = resolve_commit_filters(repo, opts.filter)?;
    let points_at = resolve_points_at(repo, opts.filter)?;

    let mut rows = Vec::new();
    for reference in repo.references()? {
        let reference = reference?;
        if !reference.is_tag() {
            continue;
        }
        let refname = reference.name()?.to_string();

        let direct_oid = reference.target().unwrap_or(Oid::zero());
        let direct_bytes: [u8; 20] = direct_oid.as_bytes().try_into().expect("20 byte oid");

        let peel_chain = tag_peel_chain(repo, &direct_oid)?;
        let commit_id = reference
            .peel_to_commit()
            .ok()
            .map(|c| oid_hex_str(c.id().as_bytes()));

        if let Some(ref commit_hex) = commit_id {
            let tip_bytes = Oid::from_str(commit_hex)?.as_bytes().try_into().expect("20 byte oid");
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

        let object_type = match repo.find_object(direct_oid, None).ok().and_then(|o| o.kind()) {
            Some(ObjectType::Commit) => "commit",
            Some(ObjectType::Tag) => "tag",
            Some(ObjectType::Tree) => "tree",
            Some(ObjectType::Blob) => "blob",
            Some(ObjectType::Any) | None => "unknown",
        }
        .to_string();

        let is_annotated = object_type == "tag";

        let (tagger, tagger_email, tagger_timestamp, message, signature) =
            if is_annotated && opts.need_annotated_meta {
                let tag = repo.find_tag(direct_oid)?;
                let raw_message = tag.message()?.unwrap_or("").to_string();
                let signature = extract_pgp_signature(&raw_message);
                let message = tag_message_without_signature(&raw_message);
                let tagger_fields = tag.tagger().map(|sig| {
                    (
                        sig.name().ok().map(str::to_string),
                        sig.email().ok().map(str::to_string),
                        Some(sig.when().seconds()),
                    )
                });
                match tagger_fields {
                    Some((name, email, ts)) => (name, email, ts, Some(message), signature),
                    None => (None, None, None, Some(message), signature),
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

fn tag_peel_chain(repo: &Repository, direct: &Oid) -> Result<Vec<[u8; 20]>, Box<dyn Error>> {
    let mut chain = Vec::new();
    let mut current = *direct;
    loop {
        let obj = repo.find_object(current, None)?;
        if obj.kind() != Some(ObjectType::Tag) {
            break;
        }
        let tag = obj.as_tag().ok_or("expected tag object")?;
        current = tag.target_id();
        chain.push(current.as_bytes().try_into().expect("20 byte oid"));
        if chain.len() > 32 {
            break;
        }
    }
    Ok(chain)
}

fn extract_pgp_signature(message: &str) -> Option<String> {
    const BEGIN: &str = "-----BEGIN PGP SIGNATURE-----";
    message.find(BEGIN).map(|start| message[start..].to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::options::DecorateFormat;

    const PARITY: &str = "test/fixtures/parity.git";

    #[test]
    fn lists_tags_including_annotated() {
        let repo = Repository::open(PARITY).unwrap();
        let filter = RefFilterParams::default();
        let rows = list_tags(
            &repo,
            &TagListOpts {
                format: DecorateFormat::Short,
                filter: &filter,
                need_annotated_meta: true,
            },
        )
        .unwrap();
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"v1"));
        let v1 = rows.iter().find(|r| r.name == "v1").unwrap();
        assert!(v1.is_annotated);
        assert!(v1.message.as_ref().is_some_and(|m| !m.is_empty()));
    }
}

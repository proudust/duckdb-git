use std::error::Error;

#[derive(Clone, Debug, Default)]
pub struct RefFilterParams {
    pub contains: Vec<String>,
    pub no_contains: Vec<String>,
    pub merged: Vec<String>,
    pub no_merged: Vec<String>,
    pub points_at: Vec<String>,
}

impl RefFilterParams {
    pub fn is_empty(&self) -> bool {
        self.contains.is_empty()
            && self.no_contains.is_empty()
            && self.merged.is_empty()
            && self.no_merged.is_empty()
            && self.points_at.is_empty()
    }
}

/// Resolved commit OIDs for graph filters (peel-to-commit).
#[derive(Clone, Debug, Default)]
pub struct ResolvedCommitFilters {
    pub contains: Vec<[u8; 20]>,
    pub no_contains: Vec<[u8; 20]>,
    pub merged: Vec<[u8; 20]>,
    pub no_merged: Vec<[u8; 20]>,
}

impl ResolvedCommitFilters {
    pub fn is_empty(&self) -> bool {
        self.contains.is_empty()
            && self.no_contains.is_empty()
            && self.merged.is_empty()
            && self.no_merged.is_empty()
    }
}

/// Raw object OIDs for points_at (no peel on argument side).
#[derive(Clone, Debug, Default)]
pub struct ResolvedPointsAt {
    pub oids: Vec<[u8; 20]>,
}

pub fn oid_matches_points_at(
    direct_oid: &[u8; 20],
    peel_chain: &[[u8; 20]],
    points_at: &ResolvedPointsAt,
) -> bool {
    if points_at.oids.is_empty() {
        return true;
    }
    points_at.oids.iter().any(|target| {
        direct_oid == target || peel_chain.iter().any(|oid| oid == target)
    })
}

pub fn passes_commit_filters(
    tip_commit: &[u8; 20],
    filters: &ResolvedCommitFilters,
    is_ancestor: &dyn Fn(&[u8; 20], &[u8; 20]) -> Result<bool, Box<dyn Error>>,
) -> Result<bool, Box<dyn Error>> {
    for commit in &filters.contains {
        if !is_ancestor(commit, tip_commit)? {
            return Ok(false);
        }
    }
    for commit in &filters.no_contains {
        if is_ancestor(commit, tip_commit)? {
            return Ok(false);
        }
    }
    for commit in &filters.merged {
        if !is_ancestor(tip_commit, commit)? {
            return Ok(false);
        }
    }
    for commit in &filters.no_merged {
        if is_ancestor(tip_commit, commit)? {
            return Ok(false);
        }
    }
    Ok(true)
}

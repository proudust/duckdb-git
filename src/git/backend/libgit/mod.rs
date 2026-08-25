mod blob_ring;
mod branches;
mod commits;
mod contained;
mod decorate;
mod diff;
mod repo;
mod tags;
mod xdiff;

pub(crate) use blob_ring::{
    flush_blob_ring_stats, reset_blob_ring_stats, snapshot_blob_ring_stats,
};
pub use blob_ring::{BlobRing, BlobRingStats, PendingOlds};
pub(crate) use branches::list_branches;
pub(crate) use commits::{emit_commit, walk_commit_oids, EmitOpts};
pub(crate) use contained::build_contained_index;
pub(crate) use decorate::collect_refs;
pub(crate) use repo::CachedRepo;
pub(crate) use tags::list_tags;
pub(crate) use crate::git::ref_list::{BranchListOpts, BranchScope, TagListOpts};
pub(crate) use xdiff::diff_line_counts;

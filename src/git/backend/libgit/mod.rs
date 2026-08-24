mod blob_ring;
mod commits;
mod decorate;
mod diff;
mod repo;
mod xdiff;

pub(crate) use blob_ring::{
    flush_blob_ring_stats, reset_blob_ring_stats, snapshot_blob_ring_stats,
};
pub use blob_ring::{BlobRing, BlobRingStats, PendingOlds};
pub(crate) use commits::{
    emit_commit, emit_inspected_commit, prepare_walk, run_commit_date_walk,
    start_commit_date_walk, walk_commit_oids, walk_next_oid, EmitOpts, InspectedCommit,
    WalkPrepared,
};
pub(crate) use decorate::collect_refs;
pub(crate) use repo::CachedRepo;
pub(crate) use xdiff::diff_line_counts;

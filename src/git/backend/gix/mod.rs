mod commits;
mod decorate;
mod diff;
mod repo;

pub(crate) use commits::{
    emit_inspected_commit, emit_prefetch_item, prepare_walk, run_prefetch_commit_walk,
    start_commit_date_walk, walk_next_oid, InspectedCommit, PrefetchItem, WalkPrepared,
};
// Used by unit tests in commits.rs (and re-exported for cross-backend parity tests).
#[allow(unused_imports)]
pub(crate) use commits::{emit_commit, walk_commit_oids};
pub(crate) use decorate::collect_refs;
pub(crate) use repo::CachedRepo;

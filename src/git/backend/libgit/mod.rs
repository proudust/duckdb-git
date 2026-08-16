mod commits;
mod contained;
mod decorate;
mod diff;
mod repo;
mod xdiff;

pub(crate) use commits::{emit_commit, walk_commit_oids};
pub(crate) use contained::build_contained_index;
pub(crate) use decorate::collect_refs;
pub(crate) use repo::CachedRepo;
pub(crate) use xdiff::diff_line_counts;

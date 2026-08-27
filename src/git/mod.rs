pub mod all_refs;
pub mod backend;
pub mod date_walk;
#[cfg(feature = "git-log-stats")]
pub mod diag;
pub mod ident;
pub mod meta_proj;
pub mod model;
pub mod options;
#[cfg(any(feature = "libgit-backend", feature = "gix-backend"))]
pub mod remote;
pub mod revision;
pub mod sink;

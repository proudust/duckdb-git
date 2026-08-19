pub mod backend;
pub mod ident;
pub mod model;
pub mod options;
#[cfg(any(feature = "libgit-backend", feature = "gix-backend"))]
pub mod ref_index;
#[cfg(any(feature = "libgit-backend", feature = "gix-backend"))]
pub mod remote;
pub mod revision;
pub mod sink;

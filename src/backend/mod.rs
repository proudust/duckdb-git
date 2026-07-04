#[cfg(feature = "git2-backend")]
mod git2;
#[cfg(feature = "gix-backend")]
mod gix;

use crate::types::{CommitData, DiffMerges};
use std::error::Error;

pub trait GitBackend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn Error>>;

    fn get_commit(&self, oid: &str) -> Result<CommitData, Box<dyn Error>>;
}

#[cfg(feature = "git2-backend")]
pub fn open(
    repo_path: &str,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
) -> Result<impl GitBackend, Box<dyn Error>> {
    git2::Git2Backend::new(repo_path, ignore_all_space, diff_merges)
}

#[cfg(feature = "gix-backend")]
pub fn open(
    repo_path: &str,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
) -> Result<impl GitBackend, Box<dyn Error>> {
    gix::GixBackend::new(repo_path, ignore_all_space, diff_merges)
}

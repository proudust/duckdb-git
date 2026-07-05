#[cfg(feature = "git-cli-backend")]
mod git_cli;
#[cfg(feature = "gix-backend")]
mod gix;
#[cfg(feature = "libgit-backend")]
mod libgit;

use crate::types::CommitData;
use std::collections::HashMap;
use std::error::Error;

pub trait GitBackend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn Error>>;

    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>>;

    fn get_commits(
        &mut self,
        oids: &[String],
        ignore_all_space: bool,
        need_file_changes: bool,
    ) -> Result<Vec<CommitData>, Box<dyn Error>>;
}

#[cfg(feature = "libgit-backend")]
pub fn open(repo_path: &str) -> Result<impl GitBackend, Box<dyn Error>> {
    libgit::LibGitBackend::new(repo_path)
}

#[cfg(feature = "gix-backend")]
pub fn open(repo_path: &str) -> Result<impl GitBackend, Box<dyn Error>> {
    gix::GixBackend::new(repo_path)
}

#[cfg(feature = "git-cli-backend")]
pub fn open(repo_path: &str) -> Result<impl GitBackend, Box<dyn Error>> {
    git_cli::GitCliBackend::new(repo_path)
}

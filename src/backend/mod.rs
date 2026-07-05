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

    fn get_commit(
        &self,
        oid: &str,
        ignore_all_space: bool,
        skip_file_changes: bool,
    ) -> Result<CommitData, Box<dyn Error>>;

    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Libgit,
    #[cfg(feature = "gix-backend")]
    Gix,
}

pub enum Backend {
    #[cfg(feature = "libgit-backend")]
    Libgit(libgit::LibGitBackend),
    #[cfg(feature = "gix-backend")]
    Gix(gix::GixBackend),
}

impl BackendKind {
    pub fn default() -> Self {
        Self::Libgit
    }

    pub fn parse(s: &str) -> Result<Self, Box<dyn Error>> {
        match s.to_lowercase().as_str() {
            "libgit" => Ok(Self::Libgit),
            "gix" => {
                #[cfg(feature = "gix-backend")]
                {
                    Ok(Self::Gix)
                }
                #[cfg(not(feature = "gix-backend"))]
                {
                    Err("'gix' backend not enabled in this build".into())
                }
            }
            other => Err(format!("unknown backend: '{other}'").into()),
        }
    }
}

pub fn open(repo_path: &str, kind: BackendKind) -> Result<Backend, Box<dyn Error>> {
    match kind {
        BackendKind::Libgit => {
            #[cfg(feature = "libgit-backend")]
            {
                Ok(Backend::Libgit(libgit::LibGitBackend::new(repo_path)?))
            }
            #[cfg(not(feature = "libgit-backend"))]
            {
                Err("'libgit' backend not enabled in this build".into())
            }
        }
        #[cfg(feature = "gix-backend")]
        BackendKind::Gix => Ok(Backend::Gix(gix::GixBackend::new(repo_path)?)),
    }
}

impl GitBackend for Backend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        match self {
            #[cfg(feature = "libgit-backend")]
            Backend::Libgit(backend) => backend.get_commit_oids(revision, max_count),
            #[cfg(feature = "gix-backend")]
            Backend::Gix(backend) => backend.get_commit_oids(revision, max_count),
        }
    }

    fn get_commit(
        &self,
        oid: &str,
        ignore_all_space: bool,
        skip_file_changes: bool,
    ) -> Result<CommitData, Box<dyn Error>> {
        match self {
            #[cfg(feature = "libgit-backend")]
            Backend::Libgit(backend) => {
                backend.get_commit(oid, ignore_all_space, skip_file_changes)
            }
            #[cfg(feature = "gix-backend")]
            Backend::Gix(backend) => backend.get_commit(oid, ignore_all_space, skip_file_changes),
        }
    }

    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
        match self {
            #[cfg(feature = "libgit-backend")]
            Backend::Libgit(backend) => backend.get_refs(),
            #[cfg(feature = "gix-backend")]
            Backend::Gix(backend) => backend.get_refs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_libgit() {
        assert_eq!(BackendKind::parse("libgit").unwrap(), BackendKind::Libgit);
        assert_eq!(BackendKind::parse("LIBGIT").unwrap(), BackendKind::Libgit);
    }

    #[test]
    #[cfg(feature = "gix-backend")]
    fn parse_gix() {
        assert_eq!(BackendKind::parse("gix").unwrap(), BackendKind::Gix);
        assert_eq!(BackendKind::parse("GIX").unwrap(), BackendKind::Gix);
    }

    #[test]
    #[cfg(not(feature = "gix-backend"))]
    fn parse_gix_not_enabled() {
        let err = BackendKind::parse("gix").unwrap_err();
        assert!(err.to_string().contains("not enabled"));
    }

    #[test]
    fn parse_unknown() {
        assert!(BackendKind::parse("unknown").is_err());
    }

    #[test]
    fn default_is_libgit() {
        assert_eq!(BackendKind::default(), BackendKind::Libgit);
    }
}

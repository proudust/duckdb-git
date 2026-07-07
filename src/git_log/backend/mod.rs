#[cfg(feature = "gix-backend")]
mod gix;
#[cfg(feature = "libgit-backend")]
mod libgit;

use crate::git_log::params::GitLogParameter;
use crate::git_log::GitLogReadPlanner;
use std::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecorateFormat {
    Short,
    Full,
}

impl DecorateFormat {
    pub fn default() -> Self {
        Self::Short
    }

    pub fn parse(s: &str) -> Result<Self, Box<dyn Error>> {
        match s.to_lowercase().as_str() {
            "short" => Ok(Self::Short),
            "full" => Ok(Self::Full),
            "no" => Err("decorate='no' is not supported; omit the decorate column instead".into()),
            other => Err(format!(
                "unknown decorate format: '{other}' (expected 'short' or 'full')"
            )
            .into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Libgit,
    #[cfg(feature = "gix-backend")]
    Gix,
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

pub fn open_planner(
    repo_path: &str,
    kind: BackendKind,
    params: &GitLogParameter,
    column_indices: &[u64],
) -> Result<Box<dyn GitLogReadPlanner>, Box<dyn Error>> {
    match kind {
        BackendKind::Libgit => {
            #[cfg(feature = "libgit-backend")]
            {
                Ok(Box::new(libgit::LibGitLogReadPlanner::open(
                    repo_path,
                    params,
                    column_indices,
                )?))
            }
            #[cfg(not(feature = "libgit-backend"))]
            {
                Err("'libgit' backend not enabled in this build".into())
            }
        }
        #[cfg(feature = "gix-backend")]
        BackendKind::Gix => Ok(Box::new(gix::GixLogReadPlanner::open(
            repo_path,
            params,
            column_indices,
        )?)),
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

    #[test]
    fn parse_decorate() {
        assert_eq!(
            DecorateFormat::parse("short").unwrap(),
            DecorateFormat::Short
        );
        assert_eq!(
            DecorateFormat::parse("SHORT").unwrap(),
            DecorateFormat::Short
        );

        assert_eq!(DecorateFormat::parse("full").unwrap(), DecorateFormat::Full);
        assert_eq!(DecorateFormat::parse("FULL").unwrap(), DecorateFormat::Full);

        assert!(DecorateFormat::parse("no")
            .unwrap_err()
            .to_string()
            .contains("not supported"));

        assert!(DecorateFormat::parse("unknown").is_err());

        assert_eq!(DecorateFormat::default(), DecorateFormat::Short);
    }
}

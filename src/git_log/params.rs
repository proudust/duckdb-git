use std::error::Error;

use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Value},
    Result,
};

use crate::git::options::{DecorateFormat, DiffMerges};
use crate::git::revision::{parse_revision_terms, RevisionTerm};

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

const REVISION: &str = "revision";
const MAX_COUNT: &str = "max_count";
const IGNORE_ALL_SPACE: &str = "ignore_all_space";
const BACKEND: &str = "backend";
const DECORATE: &str = "decorate";
const DIFF_MERGES: &str = "diff_merges";

fn extract_revision_tokens(value: &Value) -> Result<Vec<String>, Box<dyn Error>> {
    match value.logical_type_id() {
        LogicalTypeId::List => Ok(value
            .to_list()
            .ok_or("revision list must not be NULL")?
            .iter()
            .map(|v| v.to_string())
            .collect()),
        LogicalTypeId::Varchar => Ok(vec![value.to_string()]),
        other => Err(format!("revision must be VARCHAR or LIST(VARCHAR), got {other:?}").into()),
    }
}

pub(crate) struct GitLogParameter {
    pub repo_path: String,
    pub revision: Option<Vec<RevisionTerm>>,
    pub max_count: Option<usize>,
    pub ignore_all_space: bool,
    pub backend: BackendKind,
    pub decorate: DecorateFormat,
    pub diff_merges: DiffMerges,
}

pub fn parameters() -> Vec<LogicalTypeHandle> {
    vec![
        LogicalTypeHandle::from(LogicalTypeId::Varchar), // repo_path
    ]
}

pub fn named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    vec![
        (
            REVISION.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ),
        (
            MAX_COUNT.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        ),
        (
            IGNORE_ALL_SPACE.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        ),
        (
            BACKEND.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            DECORATE.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            DIFF_MERGES.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
    ]
}

pub(crate) fn is_remote_url(s: &str) -> bool {
    s.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    })
}

/// Rejects HTTP(S) URLs that embed credentials (`https://user:token@host/...`).
///
/// Anonymous HTTPS only: userinfo would otherwise land in error strings and in the
/// bare-clone `origin` URL under the shared OS temp cache.
pub(crate) fn validate_remote_url(url: &str) -> Result<(), Box<dyn Error>> {
    if remote_url_has_userinfo(url) {
        return Err(
            "remote URLs with embedded credentials (userinfo) are not supported; only anonymous HTTP(S) is allowed"
                .into(),
        );
    }
    Ok(())
}

fn remote_url_has_userinfo(url: &str) -> bool {
    let Some((_scheme, after_scheme)) = url.split_once("://") else {
        return false;
    };
    let authority = after_scheme
        .split_once(['/', '?', '#'])
        .map(|(authority, _)| authority)
        .unwrap_or(after_scheme);
    authority.contains('@')
}

pub fn bind(bind: &BindInfo) -> Result<GitLogParameter, Box<dyn std::error::Error>> {
    let repo_path = bind.get_parameter(0).to_string();

    let revision = bind
        .get_named_parameter(REVISION)
        .map(|value| {
            extract_revision_tokens(&value).and_then(|tokens| parse_revision_terms(&tokens))
        })
        .transpose()?;

    let max_count = bind
        .get_named_parameter(MAX_COUNT)
        .map(|value| parse_max_count(&value.to_string()))
        .transpose()?;

    let ignore_all_space = bind
        .get_named_parameter(IGNORE_ALL_SPACE)
        .map(|value| parse_ignore_all_space(&value.to_string()))
        .unwrap_or(false);

    let backend = bind
        .get_named_parameter(BACKEND)
        .map(|value| BackendKind::parse(&value.to_string()))
        .transpose()?
        .unwrap_or_else(BackendKind::default);

    let decorate = bind
        .get_named_parameter(DECORATE)
        .map(|value| DecorateFormat::parse(&value.to_string()))
        .transpose()?
        .unwrap_or_else(DecorateFormat::default);

    let diff_merges = bind
        .get_named_parameter(DIFF_MERGES)
        .map(|value| DiffMerges::parse(&value.to_string()))
        .transpose()?
        .unwrap_or_else(DiffMerges::default);

    Ok(GitLogParameter {
        repo_path,
        revision,
        max_count,
        ignore_all_space,
        backend,
        decorate,
        diff_merges,
    })
}

fn parse_max_count(value: &str) -> Result<usize, Box<dyn Error>> {
    value.parse::<usize>().map_err(|_| {
        format!("invalid max_count: '{value}' (expected a non-negative integer)").into()
    })
}

fn parse_ignore_all_space(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_backend() {
        assert_eq!(BackendKind::parse("libgit").unwrap(), BackendKind::Libgit);
        assert_eq!(BackendKind::parse("LIBGIT").unwrap(), BackendKind::Libgit);
        assert!(BackendKind::parse("unknown").is_err());
        assert_eq!(BackendKind::default(), BackendKind::Libgit);
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
    fn parse_max_count_test() {
        assert_eq!(parse_max_count("10").unwrap(), 10);
        assert!(parse_max_count("not-a-number").is_err());
        assert!(parse_max_count("-1").is_err());
    }

    #[test]
    fn parse_ignore_all_space_test() {
        assert!(parse_ignore_all_space("true"));
        assert!(parse_ignore_all_space("TRUE"));
        assert!(!parse_ignore_all_space("false"));
        assert!(!parse_ignore_all_space(""));
    }

    #[test]
    fn is_remote_url_accepts_http() {
        assert!(is_remote_url("http://example.com/repo.git"));
        assert!(is_remote_url("https://github.com/foo/bar"));
        assert!(is_remote_url("HTTP://EXAMPLE.COM/repo"));
        assert!(is_remote_url("HTTPS://example.com/repo"));
        assert!(is_remote_url("Http://example.com/repo"));
    }

    #[test]
    fn is_remote_url_rejects_local_and_other_schemes() {
        assert!(!is_remote_url("."));
        assert!(!is_remote_url("/path/to/repo"));
        assert!(!is_remote_url("relative/path"));
        assert!(!is_remote_url("C:\\path\\to\\repo"));
        assert!(!is_remote_url(""));
        assert!(!is_remote_url("ssh://git@github.com/foo/bar"));
        assert!(!is_remote_url("git://github.com/foo/bar"));
        assert!(!is_remote_url("file:///path/to/repo"));
    }

    #[test]
    fn validate_remote_url_rejects_userinfo() {
        assert!(validate_remote_url("https://github.com/foo/bar.git").is_ok());
        assert!(validate_remote_url("http://example.com/repo").is_ok());
        for url in [
            "https://user:token@github.com/foo/bar.git",
            "https://user@github.com/foo/bar.git",
            "HTTP://user:pass@EXAMPLE.COM/repo",
            "https://user:token@host",
        ] {
            let err = validate_remote_url(url).unwrap_err().to_string();
            assert!(err.contains("embedded credentials"), "url={url}, err={err}");
            assert!(!err.contains("token"), "url={url}, err={err}");
            assert!(!err.contains("pass"), "url={url}, err={err}");
        }
    }

    #[test]
    fn remote_url_has_userinfo_detection() {
        assert!(!remote_url_has_userinfo("https://github.com/foo/bar"));
        assert!(!remote_url_has_userinfo("https://github.com/foo/bar@baz")); // @ in path
        assert!(remote_url_has_userinfo("https://user:token@github.com/foo"));
        assert!(remote_url_has_userinfo("https://user@github.com/foo"));
    }
}

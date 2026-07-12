use std::error::Error;

use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

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
pub enum DiffMerges {
    Off,
    FirstParent,
}

impl DiffMerges {
    pub fn default() -> Self {
        Self::Off
    }

    pub fn parse(s: &str) -> Result<Self, Box<dyn Error>> {
        match s.to_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "first_parent" | "first-parent" => Ok(Self::FirstParent),
            other => Err(format!(
                "unknown diff_merges format: '{other}' (expected 'off', 'first_parent', or 'first-parent')"
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

const REVISION: &str = "revision";
const MAX_COUNT: &str = "max_count";
const IGNORE_ALL_SPACE: &str = "ignore_all_space";
const BACKEND: &str = "backend";
const DECORATE: &str = "decorate";
const DIFF_MERGES: &str = "diff_merges";

pub(crate) struct GitLogParameter {
    pub repo_path: String,
    pub revision: Option<String>,
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
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

pub fn bind(bind: &BindInfo) -> Result<GitLogParameter, Box<dyn std::error::Error>> {
    let repo_path = bind.get_parameter(0).to_string();

    let revision = bind
        .get_named_parameter(REVISION)
        .map(|value| value.to_string());

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
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid max_count: '{value}' (expected a non-negative integer)").into())
}

fn parse_ignore_all_space(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
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

    #[test]
    fn parse_diff_merges() {
        assert_eq!(DiffMerges::parse("off").unwrap(), DiffMerges::Off);
        assert_eq!(DiffMerges::parse("OFF").unwrap(), DiffMerges::Off);

        assert_eq!(
            DiffMerges::parse("first_parent").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("FIRST_PARENT").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("first-parent").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("FIRST-PARENT").unwrap(),
            DiffMerges::FirstParent
        );

        assert!(DiffMerges::parse("unknown").is_err());

        assert_eq!(DiffMerges::default(), DiffMerges::Off);
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
}

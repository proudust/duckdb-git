use std::error::Error;

use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Value},
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RevisionTerm {
    pub spec: String,
    pub negate: bool,
    pub origin: String,
}

pub(crate) fn unresolved_revision_error(origin: &str) -> String {
    if origin.starts_with('^') {
        format!("bad revision '{origin}'")
    } else {
        format!("ambiguous argument '{origin}': unknown revision or path not in the working tree.")
    }
}

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

fn parse_revision_terms(tokens: &[String]) -> Result<Vec<RevisionTerm>, Box<dyn Error>> {
    let mut terms = Vec::new();
    for token in tokens {
        let (negate, rest) = match token.strip_prefix('^') {
            Some(rest) => (true, rest),
            None => (false, token.as_str()),
        };
        if rest.is_empty() {
            return Err(format!("bad revision '{token}'").into());
        }
        if rest.contains("...") {
            return Err(format!(
                "symmetric difference ('{rest}') is not supported in revision; see git-log(1)"
            )
            .into());
        }
        if let Some(idx) = rest.find("..") {
            if negate {
                return Err(format!("bad revision '{token}'").into());
            }
            let from = &rest[..idx];
            let to = &rest[idx + 2..];
            let from = if from.is_empty() { "HEAD" } else { from };
            let to = if to.is_empty() { "HEAD" } else { to };
            terms.push(RevisionTerm {
                spec: to.to_string(),
                negate: false,
                origin: token.clone(),
            });
            terms.push(RevisionTerm {
                spec: from.to_string(),
                negate: true,
                origin: token.clone(),
            });
        } else {
            terms.push(RevisionTerm {
                spec: rest.to_string(),
                negate,
                origin: token.clone(),
            });
        }
    }
    Ok(terms)
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

    #[test]
    fn parse_revision_terms_single_spec() {
        let terms = parse_revision_terms(&["main".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![RevisionTerm {
                spec: "main".to_string(),
                negate: false,
                origin: "main".to_string(),
            }]
        );
    }

    #[test]
    fn parse_revision_terms_excludes_with_caret() {
        let terms = parse_revision_terms(&["dev".to_string(), "^main".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![
                RevisionTerm {
                    spec: "dev".to_string(),
                    negate: false,
                    origin: "dev".to_string(),
                },
                RevisionTerm {
                    spec: "main".to_string(),
                    negate: true,
                    origin: "^main".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parse_revision_terms_range_pushes_to_and_hides_from() {
        let terms = parse_revision_terms(&["main..dev".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![
                RevisionTerm {
                    spec: "dev".to_string(),
                    negate: false,
                    origin: "main..dev".to_string(),
                },
                RevisionTerm {
                    spec: "main".to_string(),
                    negate: true,
                    origin: "main..dev".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parse_revision_terms_range_defaults_missing_side_to_head() {
        let terms = parse_revision_terms(&["main..".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![
                RevisionTerm {
                    spec: "HEAD".to_string(),
                    negate: false,
                    origin: "main..".to_string(),
                },
                RevisionTerm {
                    spec: "main".to_string(),
                    negate: true,
                    origin: "main..".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parse_revision_terms_rejects_negated_range() {
        let err = parse_revision_terms(&["^main..dev".to_string()]).unwrap_err();
        assert!(err.to_string().contains("bad revision"));
    }

    #[test]
    fn unresolved_revision_error_plain_spec_matches_git_wording() {
        assert_eq!(
            unresolved_revision_error("nonexistent-ref"),
            "ambiguous argument 'nonexistent-ref': unknown revision or path not in the working tree."
        );
    }

    #[test]
    fn unresolved_revision_error_caret_spec_matches_git_wording() {
        assert_eq!(
            unresolved_revision_error("^nonexistent-ref"),
            "bad revision '^nonexistent-ref'"
        );
    }

    #[test]
    fn unresolved_revision_error_uses_whole_original_range_token() {
        assert_eq!(
            unresolved_revision_error("main..typo"),
            "ambiguous argument 'main..typo': unknown revision or path not in the working tree."
        );
    }

    #[test]
    fn parse_revision_terms_rejects_symmetric_difference() {
        let err = parse_revision_terms(&["main...dev".to_string()]).unwrap_err();
        assert!(err.to_string().contains("symmetric difference"));
    }

    #[test]
    fn parse_revision_terms_rejects_bare_caret() {
        let err = parse_revision_terms(&["^".to_string()]).unwrap_err();
        assert_eq!(err.to_string(), "bad revision '^'");
    }
}

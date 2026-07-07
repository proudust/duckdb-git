use crate::git_log::backend::{BackendKind, DecorateFormat};
use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

const REVISION: &str = "revision";
const MAX_COUNT: &str = "max_count";
const IGNORE_ALL_SPACE: &str = "ignore_all_space";
const BACKEND: &str = "backend";
const DECORATE: &str = "decorate";

pub(crate) struct GitLogParameter {
    pub repo_path: String,
    pub revision: Option<String>,
    pub max_count: Option<usize>,
    pub ignore_all_space: bool,
    pub backend: BackendKind,
    pub decorate: DecorateFormat,
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
    ]
}

pub fn bind(bind: &BindInfo) -> Result<GitLogParameter, Box<dyn std::error::Error>> {
    let repo_path = bind.get_parameter(0).to_string();

    let revision = bind
        .get_named_parameter(REVISION)
        .map(|value| value.to_string());

    let max_count = bind
        .get_named_parameter(MAX_COUNT)
        .and_then(|value| parse_max_count(&value.to_string()));

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

    Ok(GitLogParameter {
        repo_path,
        revision,
        max_count,
        ignore_all_space,
        backend,
        decorate,
    })
}

fn parse_max_count(value: &str) -> Option<usize> {
    value.parse::<usize>().ok()
}

fn parse_ignore_all_space(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_max_count_test() {
        assert_eq!(parse_max_count("10"), Some(10));
        assert_eq!(parse_max_count("not-a-number"), None);
    }

    #[test]
    fn parse_ignore_all_space_test() {
        assert!(parse_ignore_all_space("true"));
        assert!(parse_ignore_all_space("TRUE"));
        assert!(!parse_ignore_all_space("false"));
        assert!(!parse_ignore_all_space(""));
    }
}

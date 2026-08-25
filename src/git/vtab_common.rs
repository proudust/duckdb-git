use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::Value;
use std::error::Error;

use crate::git::backend_kind::BackendKind;
use crate::git::options::DecorateFormat;

pub(crate) fn extract_varchar_list(value: &Value, param: &str) -> Result<Vec<String>, Box<dyn Error>> {
    match value.logical_type_id() {
        LogicalTypeId::List => Ok(value
            .to_list()
            .ok_or_else(|| format!("{param} list must not be NULL"))?
            .iter()
            .map(|v| v.to_string())
            .collect()),
        LogicalTypeId::Varchar => Ok(vec![value.to_string()]),
        other => Err(format!("{param} must be VARCHAR or LIST(VARCHAR), got {other:?}").into()),
    }
}

pub(crate) fn parse_bool_param(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

pub(crate) fn bind_common_named(
    out: &mut Vec<(String, LogicalTypeHandle)>,
    include_branch_scope: bool,
) {
    if include_branch_scope {
        out.push((
            "remotes".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        ));
        out.push((
            "all_branches".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        ));
    }
    for name in [
        "contains",
        "no_contains",
        "merged",
        "no_merged",
        "points_at",
    ] {
        out.push((
            name.to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ));
    }
    out.push((
        "decorate".to_string(),
        LogicalTypeHandle::from(LogicalTypeId::Varchar),
    ));
    out.push((
        "backend".to_string(),
        LogicalTypeHandle::from(LogicalTypeId::Varchar),
    ));
}

pub(crate) fn bind_optional_filter_list(
    bind: &duckdb::vtab::BindInfo,
    name: &str,
) -> Result<Option<Vec<String>>, Box<dyn Error>> {
    bind.get_named_parameter(name)
        .map(|value| extract_varchar_list(&value, name))
        .transpose()
}

pub(crate) fn bind_decorate_and_backend(
    bind: &duckdb::vtab::BindInfo,
) -> Result<(DecorateFormat, BackendKind), Box<dyn Error>> {
    let decorate = bind
        .get_named_parameter("decorate")
        .map(|value| DecorateFormat::parse(&value.to_string()))
        .transpose()?
        .unwrap_or_else(DecorateFormat::default);

    let backend = bind
        .get_named_parameter("backend")
        .map(|value| BackendKind::parse(&value.to_string()))
        .transpose()?
        .unwrap_or_else(BackendKind::default);

    Ok((decorate, backend))
}

pub(crate) fn is_remote_url(s: &str) -> bool {
    s.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    })
}

/// Rejects HTTP(S) URLs that embed credentials (`https://user:token@host/...`).
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

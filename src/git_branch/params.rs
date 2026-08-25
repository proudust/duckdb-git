use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

use crate::git::backend_kind::BackendKind;
use crate::git::options::DecorateFormat;
use crate::git::ref_filter::RefFilterParams;
use crate::git::vtab_common::{
    bind_common_named, bind_decorate_and_backend, bind_optional_filter_list, parse_bool_param,
};

pub struct GitBranchParameter {
    pub repo_path: String,
    pub remotes: bool,
    pub all_branches: bool,
    pub filter: RefFilterParams,
    pub decorate: DecorateFormat,
    pub backend: BackendKind,
}

pub fn parameters() -> Vec<LogicalTypeHandle> {
    vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)]
}

pub fn named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    let mut out = Vec::new();
    bind_common_named(&mut out, true);
    out
}

pub fn bind(bind: &BindInfo) -> Result<GitBranchParameter, Box<dyn std::error::Error>> {
    let repo_path = bind.get_parameter(0).to_string();
    let remotes = bind
        .get_named_parameter("remotes")
        .map(|v| parse_bool_param(&v.to_string()))
        .unwrap_or(false);
    let all_branches = bind
        .get_named_parameter("all_branches")
        .map(|v| parse_bool_param(&v.to_string()))
        .unwrap_or(false);

    let (decorate, backend) = bind_decorate_and_backend(bind)?;

    Ok(GitBranchParameter {
        repo_path,
        remotes,
        all_branches,
        filter: RefFilterParams {
            contains: bind_optional_filter_list(bind, "contains")?.unwrap_or_default(),
            no_contains: bind_optional_filter_list(bind, "no_contains")?.unwrap_or_default(),
            merged: bind_optional_filter_list(bind, "merged")?.unwrap_or_default(),
            no_merged: bind_optional_filter_list(bind, "no_merged")?.unwrap_or_default(),
            points_at: bind_optional_filter_list(bind, "points_at")?.unwrap_or_default(),
        },
        decorate,
        backend,
    })
}

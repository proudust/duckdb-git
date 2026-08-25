use duckdb::{
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::BindInfo,
    Result,
};

use crate::git::backend_kind::BackendKind;
use crate::git::options::DecorateFormat;
use crate::git::ref_filter::RefFilterParams;
use crate::git::vtab_common::{
    bind_common_named, bind_decorate_and_backend, bind_optional_filter_list,
};

pub struct GitTagParameter {
    pub repo_path: String,
    pub filter: RefFilterParams,
    pub decorate: DecorateFormat,
    pub backend: BackendKind,
}

pub fn parameters() -> Vec<LogicalTypeHandle> {
    vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)]
}

pub fn named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    let mut out = Vec::new();
    bind_common_named(&mut out, false);
    out
}

pub fn bind(bind: &BindInfo) -> Result<GitTagParameter, Box<dyn std::error::Error>> {
    let repo_path = bind.get_parameter(0).to_string();
    let (decorate, backend) = bind_decorate_and_backend(bind)?;

    Ok(GitTagParameter {
        repo_path,
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

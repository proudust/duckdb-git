use crate::git::options::DecorateFormat;
use crate::git::ref_filter::RefFilterParams;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchScope {
    Local,
    Remote,
    All,
}

pub struct BranchListOpts<'a> {
    pub scope: BranchScope,
    pub format: DecorateFormat,
    pub filter: &'a RefFilterParams,
    pub need_tip_meta: bool,
    pub need_upstream: bool,
    pub need_push: bool,
    pub need_symref: bool,
    pub need_ahead_behind: bool,
}

pub struct TagListOpts<'a> {
    pub format: DecorateFormat,
    pub filter: &'a RefFilterParams,
    pub need_annotated_meta: bool,
}

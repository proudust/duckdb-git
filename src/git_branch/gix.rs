use crate::git::backend::gix::{list_branches, BranchListOpts, BranchScope, CachedRepo};
use crate::git::ref_row::BranchRow;
use crate::git_branch::params::GitBranchParameter;
use crate::git_branch::schema;
use crate::git_branch::vector::BranchVectorInserter;
use duckdb::core::DataChunkHandle;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Inner {
    rows: Vec<BranchRow>,
    current_index: AtomicUsize,
    batch_size: usize,
}

pub struct GixBranchScanner {
    inner: Arc<Inner>,
}

impl GixBranchScanner {
    pub fn open(
        repo_path: &str,
        params: &GitBranchParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();

        let scope = branch_scope(params);
        let rows = list_branches(
            repo,
            &BranchListOpts {
                scope,
                format: params.decorate,
                filter: &params.filter,
                need_tip_meta: schema::needs_tip_meta(column_indices),
                need_upstream: schema::needs_upstream(column_indices),
                need_push: schema::needs_push(column_indices),
                need_symref: schema::needs_symref(column_indices),
                need_ahead_behind: schema::needs_ahead_behind(column_indices),
            },
        )?;

        Ok(Self {
            inner: Arc::new(Inner {
                rows,
                current_index: AtomicUsize::new(0),
                batch_size: 2048,
            }),
        })
    }

    pub fn max_threads(&self) -> u64 {
        1
    }

    pub fn read(
        &self,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let start = self
            .inner
            .current_index
            .fetch_add(self.inner.batch_size, Ordering::Relaxed);
        if start >= self.inner.rows.len() {
            return Ok(0);
        }
        let end = (start + self.inner.batch_size).min(self.inner.rows.len());
        let mut writer = BranchVectorInserter::new(output, column_indices);
        for row in &self.inner.rows[start..end] {
            writer.write_row(row);
        }
        Ok((end - start) as u32)
    }
}

fn branch_scope(params: &GitBranchParameter) -> BranchScope {
    if params.all_branches {
        BranchScope::All
    } else if params.remotes {
        BranchScope::Remote
    } else {
        BranchScope::Local
    }
}

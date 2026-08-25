#[cfg(feature = "gix-backend")]
mod gix;
#[cfg(feature = "libgit-backend")]
mod libgit;
mod params;
pub(crate) mod schema;
mod vector;

use crate::git::backend_kind::BackendKind;
use crate::git::vtab_repo::resolve_repo_path;
use params::GitBranchParameter;

use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::error::Error;

enum GitBranchScanner {
    #[cfg(feature = "libgit-backend")]
    Libgit(libgit::LibGitBranchScanner),
    #[cfg(feature = "gix-backend")]
    Gix(gix::GixBranchScanner),
}

impl GitBranchScanner {
    fn open(
        repo_path: &str,
        kind: BackendKind,
        params: &GitBranchParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        match kind {
            BackendKind::Libgit => {
                #[cfg(feature = "libgit-backend")]
                {
                    Ok(Self::Libgit(libgit::LibGitBranchScanner::open(
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
            BackendKind::Gix => Ok(Self::Gix(gix::GixBranchScanner::open(
                repo_path,
                params,
                column_indices,
            )?)),
        }
    }

    fn max_threads(&self) -> u64 {
        match self {
            #[cfg(feature = "libgit-backend")]
            Self::Libgit(s) => s.max_threads(),
            #[cfg(feature = "gix-backend")]
            Self::Gix(s) => s.max_threads(),
        }
    }

    fn read(
        &self,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        match self {
            #[cfg(feature = "libgit-backend")]
            Self::Libgit(s) => s.read(output, column_indices),
            #[cfg(feature = "gix-backend")]
            Self::Gix(s) => s.read(output, column_indices),
        }
    }
}

struct GitBranchInitData {
    scanner: GitBranchScanner,
    column_indices: Vec<u64>,
}

struct GitBranchVTab;

impl VTab for GitBranchVTab {
    type InitData = GitBranchInitData;
    type BindData = GitBranchParameter;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        schema::bind_columns(bind)?;
        params::bind(bind)
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let params = info.get_bind_data::<GitBranchParameter>();
        let params = unsafe { &*params };
        let column_indices = info.get_column_indices();
        let repo_path = resolve_repo_path(&params.repo_path, params.backend)?;
        let scanner =
            GitBranchScanner::open(&repo_path, params.backend, params, &column_indices)?;
        info.set_max_threads(scanner.max_threads());
        Ok(GitBranchInitData {
            scanner,
            column_indices,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let row_count = init_data
            .scanner
            .read(output, &init_data.column_indices)?;
        output.set_len(row_count as usize);
        Ok(())
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(params::parameters())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(params::named_parameters())
    }
}

pub fn register(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GitBranchVTab>("git_branch")?;
    Ok(())
}

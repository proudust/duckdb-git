#[cfg(feature = "gix-backend")]
mod gix;
#[cfg(feature = "libgit-backend")]
mod libgit;
mod params;
mod prefetch;
pub(crate) mod schema;
pub(crate) mod vector;

use params::{BackendKind, GitLogParameter};

use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::error::Error;

enum GitLogScanner {
    #[cfg(feature = "libgit-backend")]
    Libgit(libgit::LibGitLogScanner),
    #[cfg(feature = "gix-backend")]
    Gix(gix::GixLogScanner),
}

impl GitLogScanner {
    fn open(
        repo_path: &str,
        kind: BackendKind,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        match kind {
            BackendKind::Libgit => {
                #[cfg(feature = "libgit-backend")]
                {
                    Ok(Self::Libgit(libgit::LibGitLogScanner::open(
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
            BackendKind::Gix => Ok(Self::Gix(gix::GixLogScanner::open(
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
        params: &GitLogParameter,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        match self {
            #[cfg(feature = "libgit-backend")]
            Self::Libgit(s) => s.read(params, output, column_indices),
            #[cfg(feature = "gix-backend")]
            Self::Gix(s) => s.read(params, output, column_indices),
        }
    }
}

struct GitLogInitData {
    scanner: GitLogScanner,
    column_indices: Vec<u64>,
}

struct GitLogVTab;

impl VTab for GitLogVTab {
    type InitData = GitLogInitData;
    type BindData = GitLogParameter;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        schema::bind_columns(bind)?;
        params::bind(bind)
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let params = info.get_bind_data::<GitLogParameter>();
        let params = unsafe { &*params };

        let column_indices = info.get_column_indices();

        let repo_path: std::borrow::Cow<str> = if params::is_remote_url(&params.repo_path) {
            params::validate_remote_url(&params.repo_path)?;
            let engine = match params.backend {
                #[cfg(feature = "libgit-backend")]
                BackendKind::Libgit => crate::git::remote::RemoteEngine::Libgit,
                #[cfg(not(feature = "libgit-backend"))]
                BackendKind::Libgit => {
                    return Err("'libgit' backend not enabled in this build".into());
                }
                #[cfg(feature = "gix-backend")]
                BackendKind::Gix => crate::git::remote::RemoteEngine::Gix,
            };
            let local = crate::git::remote::ensure_local_clone(&params.repo_path, engine)?;
            std::borrow::Cow::Owned(local.to_string_lossy().into_owned())
        } else {
            std::borrow::Cow::Borrowed(&params.repo_path)
        };

        let scanner = GitLogScanner::open(&repo_path, params.backend, params, &column_indices)?;
        info.set_max_threads(scanner.max_threads());

        Ok(GitLogInitData {
            scanner,
            column_indices,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let row_count = init_data
            .scanner
            .read(bind_data, output, &init_data.column_indices)?;
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
    con.register_table_function::<GitLogVTab>("git_log")?;
    Ok(())
}

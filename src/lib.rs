mod git;
mod git_branch;
mod git_log;
mod git_tag;
#[doc(hidden)]
pub mod microbench;

use duckdb::{Connection, Result};
use std::error::Error;

pub fn register(con: &Connection) -> Result<(), Box<dyn Error>> {
    git_log::register(con)?;
    git_branch::register(con)?;
    git_tag::register(con)
}

#[cfg(feature = "loadable-extension")]
#[duckdb::duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register(&con)
}

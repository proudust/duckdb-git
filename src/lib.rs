mod backend;
mod git_log;
mod params;
mod schema;
mod types;
mod vector;

use duckdb::{Connection, Result};
use std::error::Error;

pub fn register(con: &Connection) -> Result<(), Box<dyn Error>> {
    git_log::register(con)
}

#[cfg(feature = "loadable-extension")]
#[duckdb::duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register(&con)
}

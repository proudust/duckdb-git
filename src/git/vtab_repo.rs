use crate::git::backend_kind::BackendKind;
use crate::git::vtab_common::{is_remote_url, validate_remote_url};
use std::error::Error;

pub(crate) fn resolve_repo_path(repo_path: &str, backend: BackendKind) -> Result<String, Box<dyn Error>> {
    if is_remote_url(repo_path) {
        validate_remote_url(repo_path)?;
        let engine = match backend {
            BackendKind::Libgit => {
                #[cfg(feature = "libgit-backend")]
                {
                    crate::git::remote::RemoteEngine::Libgit
                }
                #[cfg(not(feature = "libgit-backend"))]
                {
                    return Err("'libgit' backend not enabled in this build".into());
                }
            }
            #[cfg(feature = "gix-backend")]
            BackendKind::Gix => crate::git::remote::RemoteEngine::Gix,
        };
        let local = crate::git::remote::ensure_local_clone(repo_path, engine)?;
        Ok(local.to_string_lossy().into_owned())
    } else {
        Ok(repo_path.to_string())
    }
}

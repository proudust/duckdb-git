use git2::Repository;
use std::cell::RefCell;
use std::error::Error;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

/// Holds at most one repository per thread: dropping a handle installs its
/// repository, evicting whatever was cached for a different path. A query
/// alternating between repositories on the same worker thread therefore
/// misses every time.
pub(crate) struct CachedRepo {
    repo: Option<Repository>,
    repo_path: String,
}

impl CachedRepo {
    pub(crate) fn open(repo_path: &str) -> Result<Self, Box<dyn Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(CachedRepo {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
        })
    }

    pub(crate) fn repo(&self) -> &Repository {
        self.repo.as_ref().expect("repo is present outside Drop")
    }
}

impl Drop for CachedRepo {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The only non-obvious behaviour in this module: drop installs into a
    /// one-slot thread cache, a matching path is taken back out, and a
    /// mismatching one is evicted only once the new handle drops.
    #[test]
    fn cached_repo_round_trips_through_thread_cache() {
        // --test-threads=1 runs tests on the shared main thread, so start clean.
        CACHED_REPO.with_borrow_mut(|cached| *cached = None);

        {
            let handle = CachedRepo::open(".").unwrap();
            assert!(handle.repo().path().exists());
        }
        CACHED_REPO.with_borrow(|cached| {
            assert!(
                matches!(cached, Some((path, _)) if path == "."),
                "drop installs"
            );
        });

        {
            let _handle = CachedRepo::open(".").unwrap();
            CACHED_REPO.with_borrow(|cached| {
                assert!(cached.is_none(), "a matching path is taken from the cache");
            });
        }

        {
            let _handle = CachedRepo::open("./.git").unwrap();
            CACHED_REPO.with_borrow(|cached| {
                assert!(
                    matches!(cached, Some((path, _)) if path == "."),
                    "a miss leaves the cached entry alone"
                );
            });
        }
        CACHED_REPO.with_borrow(|cached| {
            assert!(
                matches!(cached, Some((path, _)) if path == "./.git"),
                "drop evicts the previous entry"
            );
        });

        CACHED_REPO.with_borrow_mut(|cached| *cached = None);
    }
}

use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

pub fn ensure_local_clone(url: &str) -> Result<PathBuf, Box<dyn Error>> {
    let dir = cache_dir_for(url);
    let lock = dir_lock(&dir);
    let _guard = lock
        .lock()
        .map_err(|e| format!("lock poisoned for '{}': {e}", dir.display()))?;

    match gix::open(&dir) {
        Ok(_) => {
            fetch_existing(&dir).map_err(|e| format!("failed to fetch '{url}': {e}"))?;
        }
        Err(_) => {
            if dir.exists() {
                std::fs::remove_dir_all(&dir).ok();
            }
            clone_into(url, &dir)?;
        }
    }

    Ok(dir)
}

fn normalize_url(url: &str) -> &str {
    let s = url.trim_end_matches('/');
    s.strip_suffix(".git").unwrap_or(s)
}

fn cache_dir_for(url: &str) -> PathBuf {
    let normalized = normalize_url(url);
    let hash = fnv1a_64(normalized.as_bytes());
    let name = sanitize_name(url);
    std::env::temp_dir()
        .join("duckdb-git")
        .join(format!("{name}-{hash:016x}"))
}

fn sanitize_name(url: &str) -> String {
    let segment = url.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    let name = segment.strip_suffix(".git").unwrap_or(segment);
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let truncated = if sanitized.len() > 64 {
        &sanitized[..64]
    } else {
        &sanitized
    };
    if truncated.is_empty() {
        "repo".to_string()
    } else {
        truncated.to_string()
    }
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn clone_into(url: &str, dir: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(dir.parent().unwrap_or(dir))?;

    let tmp = dir.with_file_name(format!(
        "{}.tmp-{}",
        dir.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }

    let interrupt = std::sync::atomic::AtomicBool::new(false);
    let mut fetch = gix::prepare_clone_bare(url, &tmp)
        .map_err(|e| format!("failed to prepare clone of '{url}': {e}"))?
        .configure_remote(|mut remote| {
            use gix::bstr::ByteSlice;
            remote.replace_refspecs(
                ["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]
                    .iter()
                    .map(|s| s.as_bytes().as_bstr()),
                gix::remote::Direction::Fetch,
            )?;
            Ok(remote)
        });
    let result = fetch
        .fetch_only(gix::progress::Discard, &interrupt)
        .map_err(|e| format!("failed to fetch from '{url}': {e}"))?;
    drop(result);

    // gix 0.86 / gix-ref 0.66 always acquires per-ref locks during packed-refs-only
    // clone writes. Dropping those locks cleans empty parent dirs up to the gitdir,
    // which can remove the empty `refs/` tree that `gix::open` still requires.
    ensure_refs_dirs(&tmp)?;

    match std::fs::rename(&tmp, dir) {
        Ok(()) => Ok(()),
        Err(_) if dir.exists() => {
            std::fs::remove_dir_all(&tmp).ok();
            Ok(())
        }
        Err(e) => Err(format!("failed to move cloned repo to '{}': {e}", dir.display()).into()),
    }
}

fn fetch_existing(dir: &Path) -> Result<(), Box<dyn Error>> {
    let repo = gix::open(dir)?;
    let remote = repo.find_remote("origin")?;
    let outcome = remote
        .connect(gix::remote::Direction::Fetch)?
        .prepare_fetch(gix::progress::Discard, Default::default())?
        .receive(
            gix::progress::Discard,
            &std::sync::atomic::AtomicBool::new(false),
        )?;
    drop(outcome);
    // Same packed-refs-only cleanup can empty `refs/` on subsequent fetches.
    ensure_refs_dirs(dir)?;
    Ok(())
}

fn ensure_refs_dirs(git_dir: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(git_dir.join("refs/heads"))?;
    std::fs::create_dir_all(git_dir.join("refs/tags"))?;
    Ok(())
}

static DIR_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

fn dir_lock(dir: &Path) -> Arc<Mutex<()>> {
    let map_lock = DIR_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = map_lock.lock().unwrap();
    Arc::clone(
        map.entry(dir.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_dir_different_urls_differ() {
        let a = cache_dir_for("https://github.com/foo/bar.git");
        let b = cache_dir_for("https://github.com/foo/baz.git");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_dir_normalized_variants() {
        let base = cache_dir_for("https://github.com/foo/bar");
        for variant in [
            "https://github.com/foo/bar",
            "https://github.com/foo/bar.git",
            "https://github.com/foo/bar/",
            "https://github.com/foo/bar.git/",
        ] {
            assert_eq!(cache_dir_for(variant), base, "{variant}");
        }
    }

    #[test]
    fn cache_dir_contains_name_segment() {
        let dir = cache_dir_for("https://github.com/foo/bar.git");
        let name = dir.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("bar-"), "expected 'bar-...' got '{name}'");
    }

    #[test]
    fn cache_dir_under_temp() {
        let dir = cache_dir_for("https://example.com/repo");
        assert!(dir.starts_with(std::env::temp_dir().join("duckdb-git")));
    }

    #[test]
    fn sanitize_name_derives_last_segment() {
        for (url, expected) in [
            ("https://github.com/foo/bar.git", "bar"),
            ("https://github.com/foo/bar/", "bar"),
            ("https://example.com/my repo!@#", "my-repo---"),
            ("", "repo"),
            ("/", "repo"),
        ] {
            assert_eq!(sanitize_name(url), expected, "{url}");
        }
    }

    #[test]
    fn sanitize_name_long_truncated() {
        let long_url = format!("https://example.com/{}", "a".repeat(100));
        let name = sanitize_name(&long_url);
        assert!(name.len() <= 64);
    }

    #[test]
    fn fnv1a_64_known_value() {
        assert_eq!(fnv1a_64(b""), 0xcbf29ce484222325);
        assert_ne!(fnv1a_64(b"a"), fnv1a_64(b"b"));
    }

    #[test]
    fn ensure_local_clone_file_url() {
        let project_root = std::env::current_dir().unwrap();
        let url = format!("file://{}", project_root.display());

        let local_path = ensure_local_clone(&url).unwrap();
        assert!(local_path.join("HEAD").exists());

        let repo = gix::open(&local_path).unwrap();
        let head = repo.head_id().unwrap();
        assert!(!head.is_null());

        // Second call exercises the fetch_existing path
        let local_path2 = ensure_local_clone(&url).unwrap();
        assert_eq!(local_path, local_path2);

        std::fs::remove_dir_all(&local_path).ok();
    }

    #[test]
    fn ensure_local_clone_recovers_from_corruption() {
        let project_root = std::env::current_dir().unwrap();
        // Use .git path to get a distinct cache dir from ensure_local_clone_file_url
        let url = format!("file://{}/.git", project_root.display());

        let local_path = ensure_local_clone(&url).unwrap();
        assert!(local_path.join("HEAD").exists());

        // Corrupt the repo by removing objects
        let objects_dir = local_path.join("objects");
        if objects_dir.exists() {
            std::fs::remove_dir_all(&objects_dir).ok();
        }

        // Should recover by re-cloning
        let local_path2 = ensure_local_clone(&url).unwrap();
        assert_eq!(local_path, local_path2);
        assert!(local_path.join("HEAD").exists());

        let repo = gix::open(&local_path).unwrap();
        assert!(!repo.head_id().unwrap().is_null());

        std::fs::remove_dir_all(&local_path).ok();
    }
}

use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// How long to wait for a TCP/TLS connection to a remote to succeed.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// libgit2 socket read/write idle timeout. Progressing transfers keep working; a
/// peer that accepts then goes silent fails instead of blocking DuckDB forever.
#[cfg(feature = "libgit-backend")]
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Absolute ceiling for a single gix/reqwest HTTP request (pack download included).
/// reqwest has no idle timeout, so this is the only way to bound a hung response.
#[cfg(feature = "gix-backend")]
const GIX_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Age after which a `{cache}.tmp-{pid}` for a still-"alive" PID is treated as wedged
/// and reclaimed (cross-process hang / pid reuse edge cases).
const STALE_TMP_AGE: Duration = Duration::from_secs(60 * 60);

/// Which git engine performs the clone/fetch for remote URLs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteEngine {
    #[cfg(feature = "libgit-backend")]
    Libgit,
    #[cfg(feature = "gix-backend")]
    Gix,
}

/// Cache maintenance primitives each engine provides.
///
/// `clone_into` receives an empty target directory; installing it as the cache is
/// handled by [`install_fresh_clone`] so both engines share the same failure modes.
struct RemoteOps {
    is_usable: fn(&Path) -> bool,
    clone_into: CloneFn,
    fetch: FetchFn,
}

type CloneFn = fn(&str, &Path) -> Result<(), Box<dyn Error>>;
type FetchFn = fn(&Path) -> Result<(), Box<dyn Error>>;

fn ops_for(engine: RemoteEngine) -> RemoteOps {
    match engine {
        #[cfg(feature = "libgit-backend")]
        RemoteEngine::Libgit => RemoteOps {
            is_usable: libgit::is_usable,
            clone_into: libgit::clone_into,
            fetch: libgit::fetch,
        },
        #[cfg(feature = "gix-backend")]
        RemoteEngine::Gix => RemoteOps {
            is_usable: gix_remote::is_usable,
            clone_into: gix_remote::clone_into,
            fetch: gix_remote::fetch,
        },
    }
}

pub fn ensure_local_clone(url: &str, engine: RemoteEngine) -> Result<PathBuf, Box<dyn Error>> {
    let dir = cache_dir_for(url, engine);
    let lock = dir_lock(&dir);
    let _guard = lock
        .lock()
        .map_err(|e| format!("lock poisoned for '{}': {e}", dir.display()))?;
    // Declared after the mutex so it is released first: the file lock is what keeps
    // other DuckDB processes out of the check/fetch/swap sequence below.
    let _file_guard = lock_cache_dir(&dir)?;
    let ops = ops_for(engine);

    // An interrupted swap can leave only `{cache}.bak` behind; put it back before we
    // decide whether to fetch or re-clone.
    recover_orphaned_backup(&dir)?;

    if (ops.is_usable)(&dir) {
        match (ops.fetch)(&dir) {
            Ok(()) => return Ok(dir),
            // Any fetch failure triggers a full re-clone. Local corruption (missing
            // `origin`, damaged ODB, unreadable config) opens fine but never recovers
            // without a wipe, and classifying libgit2/gix errors as "transient network
            // vs permanent local damage" is unreliable across backends. The trade-off is
            // intentional: a transient outage or rate limit may also pay for a full clone
            // attempt. The existing cache stays in place until that clone finishes; only
            // then is it moved aside (`.bak`) and replaced. If the clone itself fails,
            // the previous cache is untouched; if the final rename fails, the `.bak` is
            // restored. Prefer over-cloning to leaving a permanently unusable cache that
            // only fails on fetch.
            Err(fetch_err) => {
                install_fresh_clone(url, &dir, &ops).map_err(|clone_err| {
                    format!(
                        "failed to fetch '{}': {fetch_err}; re-clone failed: {clone_err}",
                        redact_url(url)
                    )
                })?;
                return Ok(dir);
            }
        }
    }

    install_fresh_clone(url, &dir, &ops)?;
    Ok(dir)
}

/// Clones into a sibling temp directory, then swaps it into place via `{dir}.bak` so a
/// failed clone never deletes the previous cache, and a failed install can restore it.
fn install_fresh_clone(url: &str, dir: &Path, ops: &RemoteOps) -> Result<(), Box<dyn Error>> {
    let tmp = prepare_tmp_dir(dir)?;

    if let Err(e) = (ops.clone_into)(url, &tmp) {
        std::fs::remove_dir_all(&tmp).ok();
        return Err(e);
    }

    let bak = backup_dir(dir);
    if dir.exists() {
        // Drop a stale leftover from an older interrupted swap before moving current aside.
        remove_cache_dir(&bak)?;
        std::fs::rename(dir, &bak).map_err(|e| {
            std::fs::remove_dir_all(&tmp).ok();
            format!(
                "failed to move existing cache aside to '{}': {e}",
                bak.display()
            )
        })?;
    }

    if let Err(e) = std::fs::rename(&tmp, dir) {
        std::fs::remove_dir_all(&tmp).ok();
        // A usable cache appearing here means a writer outside our lock (older build,
        // manual meddling) got there first; accept it rather than clobbering it.
        // Otherwise put our previous cache back so a rename glitch does not leave the
        // caller empty-handed.
        if (ops.is_usable)(dir) {
            remove_cache_dir(&bak).ok();
            return Ok(());
        }
        if bak.exists() {
            if let Err(restore_err) = std::fs::rename(&bak, dir) {
                return Err(format!(
                    "failed to move cloned repo to '{}': {e}; also failed to restore previous cache: {restore_err}",
                    dir.display()
                )
                .into());
            }
        }
        return Err(format!("failed to move cloned repo to '{}': {e}", dir.display()).into());
    }

    // New cache is live, so the previous one is only garbage now. Deleting it can still
    // fail while another process has the old files open (Windows in particular); that
    // must not fail an install that already succeeded. A leftover `{cache}.bak` next to
    // a live cache is inert and gets dropped by the next install.
    remove_cache_dir(&bak).ok();
    Ok(())
}

fn backup_dir(dir: &Path) -> PathBuf {
    dir.with_file_name(format!(
        "{}.bak",
        dir.file_name().unwrap().to_string_lossy()
    ))
}

fn recover_orphaned_backup(dir: &Path) -> Result<(), Box<dyn Error>> {
    let bak = backup_dir(dir);
    if dir.exists() || !bak.exists() {
        return Ok(());
    }
    std::fs::rename(&bak, dir).map_err(|e| {
        format!(
            "failed to restore orphaned cache backup '{}' -> '{}': {e}",
            bak.display(),
            dir.display()
        )
        .into()
    })
}

/// Canonical form used only for cache identity.
///
/// Collapses scheme/host case, strips userinfo, default ports (`:443` / `:80`),
/// query/fragment, equivalent percent-encoding, trailing `/`, and a final `.git` so
/// equivalent remotes share one cache directory (and credential-bearing variants do
/// not get a separate residue even if they somehow reach this layer).
///
/// Intentionally **not** collapsed:
/// - path letter-case — many hosts are case-sensitive; folding would merge distinct
///   repos. (GitHub is case-insensitive, but that is host policy, not URL equivalence.)
/// - `http` vs `https` — different origins; sharing a cache would be wrong if only one
///   scheme works. The cost is a possible double clone of the same project.
fn normalize_url(url: &str) -> Cow<'_, str> {
    let Some((scheme, after_scheme)) = url.split_once("://") else {
        let trimmed = url.trim_end_matches('/');
        let stripped = trimmed.strip_suffix(".git").unwrap_or(trimmed);
        return if stripped.len() == url.len() {
            Cow::Borrowed(url)
        } else {
            Cow::Owned(stripped.to_string())
        };
    };

    let (authority, rest) = match after_scheme.find(['/', '?', '#']) {
        Some(i) => (&after_scheme[..i], &after_scheme[i..]),
        None => (after_scheme, ""),
    };
    // Query and fragment never participate in git clone identity.
    let path_raw = match rest.find(['?', '#']) {
        Some(i) => &rest[..i],
        None => rest,
    };

    // Drop userinfo (`user:token@host`) so it cannot fork the cache key.
    let hostport = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    let scheme_l = scheme.to_ascii_lowercase();
    let host_lower = hostport.to_ascii_lowercase();
    let host_l = strip_default_port(&scheme_l, &host_lower);
    let path_decoded = percent_decode(path_raw);
    let path_trim = path_decoded.trim_end_matches('/');
    let path_norm = path_trim.strip_suffix(".git").unwrap_or(path_trim);

    let owned = format!("{scheme_l}://{host_l}{path_norm}");
    if owned == url {
        Cow::Borrowed(url)
    } else {
        Cow::Owned(owned)
    }
}

/// Removes `:80` / `:443` when they are the scheme default.
///
/// Bracketed IPv6 (`[::1]:443`) is handled; unbracketed addresses that still contain
/// `:` after a would-be strip are left alone so we never chop into an address.
fn strip_default_port<'a>(scheme: &str, hostport: &'a str) -> &'a str {
    let default = match scheme {
        "http" => "80",
        "https" => "443",
        _ => return hostport,
    };
    let Some(host) = hostport
        .strip_suffix(default)
        .and_then(|h| h.strip_suffix(':'))
    else {
        return hostport;
    };
    if host.starts_with('[') {
        return if host.ends_with(']') { host } else { hostport };
    }
    if host.contains(':') {
        return hostport;
    }
    host
}

/// Decodes `%HH` sequences in a URL path for cache identity.
///
/// Invalid escapes are copied through unchanged. Non-UTF-8 results keep the original
/// path so a decode glitch cannot silently merge unrelated keys.
fn percent_decode(input: &str) -> Cow<'_, str> {
    if !input.as_bytes().contains(&b'%') {
        return Cow::Borrowed(input);
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    match String::from_utf8(out) {
        Ok(s) => Cow::Owned(s),
        Err(_) => Cow::Borrowed(input),
    }
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Masks `userinfo` in URLs so credentials never appear in error messages.
fn redact_url(url: &str) -> Cow<'_, str> {
    let Some((scheme, after_scheme)) = url.split_once("://") else {
        return Cow::Borrowed(url);
    };
    let (authority, suffix) = match after_scheme.find(['/', '?', '#']) {
        Some(i) => (&after_scheme[..i], &after_scheme[i..]),
        None => (after_scheme, ""),
    };
    let Some((_userinfo, host)) = authority.rsplit_once('@') else {
        return Cow::Borrowed(url);
    };
    Cow::Owned(format!("{scheme}://***@{host}{suffix}"))
}

/// Cache directories are namespaced per engine: libgit2 and gix write subtly
/// different bare layouts, and sharing one directory would make behaviour depend on
/// which engine happened to create it first.
///
/// The directory name embeds an FNV-1a 64-bit digest of the normalized URL. That is
/// not a cryptographic hash, so in theory two distinct URLs can collide. On a
/// world-writable `$TMPDIR` the binding threat is simpler: anyone who knows the URL
/// can compute the path and plant content there without needing a collision. The
/// advisory lock only serialises cooperative duckdb-git processes; it is not a
/// trust boundary against other local users (see README).
fn cache_dir_for(url: &str, engine: RemoteEngine) -> PathBuf {
    let normalized = normalize_url(url);
    let hash = fnv1a_64(normalized.as_bytes());
    let name = sanitize_name(&normalized);
    std::env::temp_dir()
        .join("duckdb-git")
        .join(engine_slug(engine))
        .join(format!("{name}-{hash:016x}"))
}

fn engine_slug(engine: RemoteEngine) -> &'static str {
    match engine {
        #[cfg(feature = "libgit-backend")]
        RemoteEngine::Libgit => "libgit",
        #[cfg(feature = "gix-backend")]
        RemoteEngine::Gix => "gix",
    }
}

fn sanitize_name(url: &str) -> String {
    // Prefer the last path segment of a normalized URL so percent-encoded names
    // (`%6dain` → `main`) stay readable in the cache directory prefix.
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

fn prepare_tmp_dir(dir: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let parent = dir.parent().unwrap_or(dir);
    std::fs::create_dir_all(parent)?;
    cleanup_orphan_tmp_dirs(dir);

    let tmp = dir.with_file_name(format!(
        "{}.tmp-{}",
        dir.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    // Our own leftover from a previous attempt in this process (same pid is rare after
    // crash, but covers the "retry after failed clone" path).
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    Ok(tmp)
}

/// Removes `{cache}.tmp-{pid}` siblings left behind by crashed or long-dead installers.
///
/// [`lock_cache_dir`] already guarantees no other lock-abiding process is installing, so
/// this is garbage collection rather than arbitration. Live PIDs are still spared in case
/// a writer outside our lock owns them; those are reclaimed after [`STALE_TMP_AGE`].
fn cleanup_orphan_tmp_dirs(dir: &Path) {
    let Some(parent) = dir.parent() else {
        return;
    };
    let Some(cache_name) = dir.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    let prefix = format!("{cache_name}.tmp-");
    let my_pid = std::process::id();
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };

    for entry in entries.flatten() {
        let fname = entry.file_name();
        let Some(fname) = fname.to_str() else {
            continue;
        };
        let Some(suffix) = fname.strip_prefix(&prefix) else {
            continue;
        };

        let remove = match suffix.parse::<u32>() {
            Ok(pid) if pid == my_pid => true,
            Ok(pid) if !process_seems_alive(pid) => true,
            Ok(_) => entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|mtime| mtime.elapsed().ok())
                .is_some_and(|age| age > STALE_TMP_AGE),
            // Malformed suffix — safe to drop.
            Err(_) => true,
        };

        if remove {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// Best-effort check used only for orphan tmp reclaim.
fn process_seems_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        // SAFETY: signal 0 performs an existence check and does not deliver a signal.
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        let rc = unsafe { kill(pid as i32, 0) };
        if rc == 0 {
            return true;
        }
        // ESRCH (3 on macOS/BSD) means the process does not exist. EPERM means it does.
        const ESRCH: i32 = 3;
        std::io::Error::last_os_error().raw_os_error() != Some(ESRCH)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        // No cheap liveness probe; rely on STALE_TMP_AGE instead.
        true
    }
}

fn remove_cache_dir(dir: &Path) -> Result<(), Box<dyn Error>> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("failed to remove stale cache '{}': {e}", dir.display()).into()),
    }
}

static DIR_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

/// Serialises threads of *this* process on one cache directory.
///
/// Taken before [`lock_cache_dir`] because the same-process semantics of advisory file
/// locks are platform-dependent, and because contending threads should not each need an
/// open file descriptor to wait.
fn dir_lock(dir: &Path) -> Arc<Mutex<()>> {
    let map_lock = DIR_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = map_lock.lock().unwrap();
    Arc::clone(
        map.entry(dir.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

/// A sibling of the cache directory, never a file inside it: the directory itself is
/// renamed during a swap, so a lock held within it would not cover the swap.
///
/// This empty file is deliberately never removed. Unlinking it would let a waiter keep
/// its lock on the now-unreachable inode while the next process creates a fresh file and
/// locks that instead, so both would proceed at once.
fn lock_file_path(dir: &Path) -> PathBuf {
    dir.with_file_name(format!(
        "{}.lock",
        dir.file_name().unwrap().to_string_lossy()
    ))
}

/// Blocks until no other process is maintaining this cache directory.
///
/// The OS temp directory is shared, so two DuckDB processes can query the same URL at
/// once. Without this, their `{cache}.bak` swaps interleave and destroy each other's
/// state: one restores the other's backup mid-clone, or deletes the backup the other
/// still needs to roll back to, leaving a stale cache reported as freshly fetched.
///
/// Advisory locks (`flock` / `LockFileEx`) are released by the kernel when the holder
/// exits, so a crashed or killed installer cannot wedge the cache the way a
/// hand-rolled lockfile would. Waiting is therefore unbounded on purpose — the holder's
/// work is already bounded by the network timeouts above, and giving up early would only
/// return the caller to the unsynchronised behaviour this exists to prevent.
fn lock_cache_dir(dir: &Path) -> Result<Option<File>, Box<dyn Error>> {
    let parent = dir.parent().unwrap_or(dir);
    std::fs::create_dir_all(parent)?;

    let path = lock_file_path(dir);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        // The file is a lock token only; its contents are never read or written.
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("failed to open cache lock '{}': {e}", path.display()))?;

    match file.lock() {
        Ok(()) => Ok(Some(file)),
        // Targets without advisory locking keep the previous in-process-only behaviour
        // rather than losing remote URL support entirely.
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => Ok(None),
        Err(e) => Err(format!("failed to lock cache '{}': {e}", path.display()).into()),
    }
}

#[cfg(feature = "libgit-backend")]
mod libgit {
    use super::*;
    use git2::Repository;
    use std::sync::Once;

    const FETCH_REFSPECS: &[&str] = &["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"];

    pub(super) fn ensure_network_timeouts() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            // Global libgit2 opts; safe here because we run before any network I/O and
            // only set them once for the process.
            unsafe {
                let _ = git2::opts::set_server_connect_timeout_in_milliseconds(
                    CONNECT_TIMEOUT.as_millis() as i32,
                );
                let _ =
                    git2::opts::set_server_timeout_in_milliseconds(IDLE_TIMEOUT.as_millis() as i32);
            }
        });
    }

    pub(super) fn is_usable(dir: &Path) -> bool {
        Repository::open(dir).is_ok()
    }

    pub(super) fn clone_into(url: &str, dir: &Path) -> Result<(), Box<dyn Error>> {
        ensure_network_timeouts();
        let shown = redact_url(url);
        // init_bare + fetch avoids RepoBuilder looking up a default checkout branch
        // (which fails when the remote's HEAD name is not yet present locally).
        let repo = Repository::init_bare(dir)
            .map_err(|e| format!("failed to init bare clone of '{shown}': {e}"))?;
        {
            let _ = repo
                .remote_with_fetch("origin", url, FETCH_REFSPECS[0])
                .map_err(|e| format!("failed to create remote for '{shown}': {e}"))?;
            repo.remote_add_fetch("origin", FETCH_REFSPECS[1])
                .map_err(|e| format!("failed to configure fetch refspecs for '{shown}': {e}"))?;
        }

        let mut remote = repo
            .find_remote("origin")
            .map_err(|e| format!("failed to find origin for '{shown}': {e}"))?;
        remote
            .fetch(FETCH_REFSPECS, None, None)
            .map_err(|e| format!("failed to fetch from '{shown}': {e}"))?;
        // init_bare leaves HEAD at refs/heads/master; without this the clone can
        // succeed while HEAD dangles when the remote default is e.g. main.
        let head = remote
            .default_branch()
            .map_err(|e| format!("failed to determine default branch of '{shown}': {e}"))?;
        let name = head
            .as_str()
            .map_err(|_| format!("default branch of '{shown}' is not valid UTF-8"))?;
        repo.set_head(name)
            .map_err(|e| format!("failed to set HEAD to '{name}' for '{shown}': {e}"))?;

        Ok(())
    }

    pub(super) fn fetch(dir: &Path) -> Result<(), Box<dyn Error>> {
        ensure_network_timeouts();
        let repo = Repository::open(dir)?;
        let mut remote = repo.find_remote("origin")?;
        remote.fetch(FETCH_REFSPECS, None, None)?;
        Ok(())
    }
}

#[cfg(feature = "gix-backend")]
mod gix_remote {
    use super::*;
    use std::sync::Arc;

    pub(super) fn is_usable(dir: &Path) -> bool {
        gix::open(dir).is_ok()
    }

    fn http_transport_options() -> Box<dyn std::any::Any> {
        use gix::protocol::transport::client::blocking_io::http;

        // reqwest ignores Options.connect_timeout (it hardcodes 20s) but honors a
        // per-request timeout via its backend Options — the only bound we can set
        // against a peer that accepts and then stalls.
        Box::new(http::Options {
            connect_timeout: Some(CONNECT_TIMEOUT),
            backend: Some(Arc::new(std::sync::Mutex::new(http::reqwest::Options {
                configure_request: Some(Box::new(|req| {
                    *req.timeout_mut() = Some(GIX_REQUEST_TIMEOUT);
                    Ok(())
                })),
            }))),
            ..Default::default()
        })
    }

    pub(super) fn clone_into(url: &str, dir: &Path) -> Result<(), Box<dyn Error>> {
        let shown = redact_url(url);
        let interrupt = std::sync::atomic::AtomicBool::new(false);
        let mut fetch = gix::prepare_clone_bare(url, dir)
            .map_err(|e| format!("failed to prepare clone of '{shown}': {e}"))?
            .configure_remote(|mut remote| {
                use gix::bstr::ByteSlice;
                remote.replace_refspecs(
                    ["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]
                        .iter()
                        .map(|s| s.as_bytes().as_bstr()),
                    gix::remote::Direction::Fetch,
                )?;
                Ok(remote)
            })
            .configure_connection(|conn| {
                conn.set_transport_options(http_transport_options());
                Ok(())
            });
        let result = fetch
            .fetch_only(gix::progress::Discard, &interrupt)
            .map_err(|e| format!("failed to fetch from '{shown}': {e}"))?;
        drop(result);

        // gix 0.86 / gix-ref 0.66 always acquires per-ref locks during packed-refs-only
        // clone writes. Dropping those locks cleans empty parent dirs up to the gitdir,
        // which can remove the empty `refs/` tree that `gix::open` still requires.
        ensure_refs_dirs(dir)?;
        Ok(())
    }

    pub(super) fn fetch(dir: &Path) -> Result<(), Box<dyn Error>> {
        let repo = gix::open(dir)?;
        let remote = repo.find_remote("origin")?;
        let outcome = remote
            .connect(gix::remote::Direction::Fetch)?
            .with_transport_options(http_transport_options())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "libgit-backend")]
    const TEST_ENGINE: RemoteEngine = RemoteEngine::Libgit;
    #[cfg(all(not(feature = "libgit-backend"), feature = "gix-backend"))]
    const TEST_ENGINE: RemoteEngine = RemoteEngine::Gix;

    #[test]
    fn cache_dir_different_urls_differ() {
        let a = cache_dir_for("https://github.com/foo/bar.git", TEST_ENGINE);
        let b = cache_dir_for("https://github.com/foo/baz.git", TEST_ENGINE);
        assert_ne!(a, b);
    }

    #[test]
    fn cache_dir_normalized_variants() {
        let base = cache_dir_for("https://github.com/foo/bar", TEST_ENGINE);
        for variant in [
            "https://github.com/foo/bar",
            "https://github.com/foo/bar.git",
            "https://github.com/foo/bar/",
            "https://github.com/foo/bar.git/",
            "HTTPS://github.com/foo/bar",
            "https://GitHub.com/foo/bar",
            "HTTPS://GitHub.COM/foo/bar.git/",
            // userinfo must not fork the cache key (also rejected at the VTab layer)
            "https://user:token@github.com/foo/bar",
            "https://user@GitHub.com/foo/bar.git",
            // default HTTPS port, query, fragment, percent-encoding
            "https://github.com:443/foo/bar",
            "https://github.com/foo/bar?x=1",
            "https://github.com/foo/bar#frag",
            "https://github.com/foo/bar.git?x=1#frag",
            "https://github.com/foo/%62ar",
            "https://github.com/foo/%62%61r.git/",
            "HTTPS://GitHub.com:443/foo/%62ar?x=1#y",
        ] {
            assert_eq!(cache_dir_for(variant, TEST_ENGINE), base, "{variant}");
        }
    }

    #[test]
    fn normalize_url_canonicalizes_scheme_host_and_userinfo() {
        assert_eq!(
            normalize_url("HTTPS://GitHub.com/foo/bar.git/"),
            "https://github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("https://user:token@github.com/foo/bar"),
            "https://github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("https://github.com/foo/bar"),
            "https://github.com/foo/bar"
        );
    }

    #[test]
    fn normalize_url_strips_default_port_query_fragment_and_percent_encoding() {
        assert_eq!(
            normalize_url("https://github.com:443/foo/bar"),
            "https://github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("http://example.com:80/repo.git"),
            "http://example.com/repo"
        );
        assert_eq!(
            normalize_url("https://github.com/foo/bar?ref=main#readme"),
            "https://github.com/foo/bar"
        );
        assert_eq!(
            normalize_url("https://github.com/foo/%6dain"),
            "https://github.com/foo/main"
        );
        assert_eq!(
            normalize_url("https://github.com/foo/%6Dain.git/"),
            "https://github.com/foo/main"
        );
        // Non-default ports are preserved.
        assert_eq!(
            normalize_url("https://github.com:8443/foo/bar"),
            "https://github.com:8443/foo/bar"
        );
        // Bracketed IPv6 with default port.
        assert_eq!(
            normalize_url("https://[::1]:443/repo"),
            "https://[::1]/repo"
        );
    }

    #[test]
    fn normalize_url_keeps_path_case_and_http_https_distinct() {
        assert_ne!(
            normalize_url("https://github.com/Foo/Bar"),
            normalize_url("https://github.com/foo/bar")
        );
        assert_ne!(
            normalize_url("http://github.com/foo/bar"),
            normalize_url("https://github.com/foo/bar")
        );
    }

    #[test]
    fn cache_dir_contains_name_segment() {
        let dir = cache_dir_for("https://github.com/foo/bar.git", TEST_ENGINE);
        let name = dir.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("bar-"), "expected 'bar-...' got '{name}'");
    }

    #[test]
    fn cache_dir_under_temp() {
        let dir = cache_dir_for("https://example.com/repo", TEST_ENGINE);
        assert!(dir.starts_with(std::env::temp_dir().join("duckdb-git")));
    }

    #[cfg(all(feature = "libgit-backend", feature = "gix-backend"))]
    #[test]
    fn cache_dir_differs_per_engine() {
        let url = "https://github.com/foo/bar.git";
        assert_ne!(
            cache_dir_for(url, RemoteEngine::Libgit),
            cache_dir_for(url, RemoteEngine::Gix)
        );
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
    fn cleanup_orphan_tmp_dirs_removes_dead_pid_and_keeps_live() {
        let parent = tempfile::tempdir().unwrap();
        let cache = parent.path().join("repo-deadbeef");
        std::fs::create_dir_all(&cache).unwrap();

        let dead = parent.path().join("repo-deadbeef.tmp-4294967294");
        std::fs::create_dir_all(&dead).unwrap();

        // PID 1 is init/launchd and should still be running on unix.
        let live = parent.path().join("repo-deadbeef.tmp-1");
        std::fs::create_dir_all(&live).unwrap();

        cleanup_orphan_tmp_dirs(&cache);

        assert!(
            !dead.exists(),
            "tmp for a non-existent pid should be removed"
        );
        #[cfg(unix)]
        assert!(live.exists(), "tmp for a live pid should be kept");

        let own = parent
            .path()
            .join(format!("repo-deadbeef.tmp-{}", std::process::id()));
        std::fs::create_dir_all(&own).unwrap();
        let prepared = prepare_tmp_dir(&cache).unwrap();
        assert_eq!(prepared, own);
        assert!(!own.exists(), "prepare_tmp_dir clears our own leftover tmp");
    }

    #[test]
    fn lock_file_is_a_sibling_of_the_cache_dir() {
        let dir = cache_dir_for("https://github.com/foo/bar.git", TEST_ENGINE);
        let lock = lock_file_path(&dir);
        assert_eq!(lock.parent(), dir.parent());
        assert_eq!(
            lock.file_name().unwrap().to_string_lossy(),
            format!("{}.lock", dir.file_name().unwrap().to_string_lossy())
        );
    }

    #[test]
    fn lock_cache_dir_creates_missing_parents() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("libgit").join("repo-deadbeef");

        let guard = lock_cache_dir(&cache).unwrap();

        assert!(lock_file_path(&cache).exists());
        assert!(!cache.exists(), "the lock must not create the cache itself");
        drop(guard);
    }

    /// A second open file description stands in for a second DuckDB process: on unix and
    /// Windows these contend exactly as separate processes do.
    #[test]
    fn lock_cache_dir_excludes_other_holders() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("repo-deadbeef");

        let guard = lock_cache_dir(&cache)
            .unwrap()
            .expect("advisory locking is supported on all built targets");
        let other = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_file_path(&cache))
            .unwrap();
        assert!(
            matches!(other.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            "a held cache lock must block other holders"
        );

        drop(guard);
        assert!(
            other.try_lock().is_ok(),
            "the lock must be released with its guard"
        );
    }

    #[test]
    fn fnv1a_64_known_value() {
        assert_eq!(fnv1a_64(b""), 0xcbf29ce484222325);
        assert_ne!(fnv1a_64(b"a"), fnv1a_64(b"b"));
    }

    #[test]
    fn redact_url_masks_userinfo() {
        assert_eq!(
            redact_url("https://user:token@github.com/foo/bar.git"),
            "https://***@github.com/foo/bar.git"
        );
        assert_eq!(
            redact_url("https://user@example.com/repo"),
            "https://***@example.com/repo"
        );
        assert_eq!(
            redact_url("https://github.com/foo/bar.git"),
            "https://github.com/foo/bar.git"
        );
    }

    #[cfg(feature = "libgit-backend")]
    #[test]
    fn libgit_network_timeouts_are_configured() {
        libgit::ensure_network_timeouts();
        unsafe {
            assert_eq!(
                git2::opts::get_server_connect_timeout_in_milliseconds().unwrap(),
                CONNECT_TIMEOUT.as_millis() as i32
            );
            assert_eq!(
                git2::opts::get_server_timeout_in_milliseconds().unwrap(),
                IDLE_TIMEOUT.as_millis() as i32
            );
        }
    }

    #[test]
    fn cache_dir_hash_matches_e2e_remote_url() {
        // Keep in sync with test/sql/{libgit,gix}/param_repo_path.test cache path.
        let dir = cache_dir_for("https://github.com/proudust/duckdb-git.git", TEST_ENGINE);
        assert!(
            dir.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with("-dfd149313ea4b348"),
            "{}",
            dir.display()
        );
    }

    /// Replaces `config` with a bare-repo config that has no `origin`, which still
    /// opens but can never be fetched from.
    fn drop_origin_remote(dir: &Path) {
        std::fs::write(
            dir.join("config"),
            "[core]\n\trepositoryformatversion = 0\n\tbare = true\n",
        )
        .unwrap();
    }

    #[cfg(feature = "libgit-backend")]
    mod libgit_tests {
        use super::*;

        fn assert_usable_cache(url: &str, dir: &Path) {
            assert_eq!(dir, cache_dir_for(url, RemoteEngine::Libgit));
            assert!(
                dir.starts_with(std::env::temp_dir().join("duckdb-git").join("libgit")),
                "cache should be under duckdb-git/libgit: {}",
                dir.display()
            );
            let repo = git2::Repository::open(dir).unwrap();
            let commit = repo
                .head()
                .expect("HEAD must resolve")
                .peel_to_commit()
                .expect("HEAD must peel to a commit (not a dangling ref)");
            assert!(!commit.id().is_zero());
        }

        #[test]
        fn ensure_local_clone_file_url() {
            let project_root = std::env::current_dir().unwrap();
            let url = format!("file://{}", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert_usable_cache(&url, &local_path);

            // Second call exercises the fetch path against the existing cache
            let local_path2 = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert_eq!(local_path, local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }

        #[test]
        fn ensure_local_clone_recovers_from_missing_objects() {
            let project_root = std::env::current_dir().unwrap();
            // Distinct URL keeps the cache dir separate from the other tests
            let url = format!("file://{}/.git", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert!(local_path.join("HEAD").exists());

            std::fs::remove_dir_all(local_path.join("objects")).ok();

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert_eq!(local_path, local_path2);
            assert!(local_path.join("objects").exists());
            assert_usable_cache(&url, &local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }

        #[test]
        fn ensure_local_clone_recovers_from_missing_origin() {
            let project_root = std::env::current_dir().unwrap();
            let url = format!("file://{}/./.git", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            drop_origin_remote(&local_path);
            assert!(git2::Repository::open(&local_path)
                .unwrap()
                .find_remote("origin")
                .is_err());

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert_eq!(local_path, local_path2);
            assert!(git2::Repository::open(&local_path)
                .unwrap()
                .find_remote("origin")
                .is_ok());
            assert_usable_cache(&url, &local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }

        #[test]
        fn ensure_local_clone_restores_orphaned_backup() {
            let project_root = std::env::current_dir().unwrap();
            // Distinct from the other file:// URLs so the cache dir does not collide.
            let url = format!("file://{}/.", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            let bak = backup_dir(&local_path);
            std::fs::rename(&local_path, &bak).unwrap();
            assert!(!local_path.exists());
            assert!(bak.exists());

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Libgit).unwrap();
            assert_eq!(local_path, local_path2);
            assert!(local_path.exists());
            assert!(!bak.exists());
            assert_usable_cache(&url, &local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }
    }

    #[cfg(feature = "gix-backend")]
    mod gix_tests {
        use super::*;

        fn assert_usable_cache(url: &str, dir: &Path) {
            assert_eq!(dir, cache_dir_for(url, RemoteEngine::Gix));
            assert!(
                dir.starts_with(std::env::temp_dir().join("duckdb-git").join("gix")),
                "cache should be under duckdb-git/gix: {}",
                dir.display()
            );
            let repo = gix::open(dir).unwrap();
            assert!(
                !repo.head_id().expect("HEAD must resolve").is_null(),
                "HEAD must peel to a commit"
            );
        }

        #[test]
        fn ensure_local_clone_file_url() {
            let project_root = std::env::current_dir().unwrap();
            let url = format!("file://{}", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            assert_usable_cache(&url, &local_path);

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            assert_eq!(local_path, local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }

        #[test]
        fn ensure_local_clone_recovers_from_missing_objects() {
            let project_root = std::env::current_dir().unwrap();
            let url = format!("file://{}/.git", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            assert!(local_path.join("HEAD").exists());

            std::fs::remove_dir_all(local_path.join("objects")).ok();

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            assert_eq!(local_path, local_path2);
            assert!(local_path.join("objects").exists());
            assert_usable_cache(&url, &local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }

        #[test]
        fn ensure_local_clone_recovers_from_missing_origin() {
            let project_root = std::env::current_dir().unwrap();
            let url = format!("file://{}/./.git", project_root.display());

            let local_path = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            drop_origin_remote(&local_path);
            assert!(gix::open(&local_path)
                .unwrap()
                .find_remote("origin")
                .is_err());

            let local_path2 = ensure_local_clone(&url, RemoteEngine::Gix).unwrap();
            assert_eq!(local_path, local_path2);
            assert!(gix::open(&local_path)
                .unwrap()
                .find_remote("origin")
                .is_ok());
            assert_usable_cache(&url, &local_path2);

            std::fs::remove_dir_all(&local_path).ok();
        }
    }
}

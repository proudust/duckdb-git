use super::blob_ring::{record_lookup, BlobRing, LookupKind, PendingOlds};
use crate::git::model::{gitlink_numstat, same_oid_numstat, unable_to_read_object};
use crate::git::sink::{oid_hex, CommitSink, FileChangeRef};
use git2::Repository;

fn find_blob<'a>(repo: &'a Repository, oid: git2::Oid) -> Result<git2::Blob<'a>, git2::Error> {
    repo.find_blob(oid).map_err(|e| {
        if e.code() == git2::ErrorCode::NotFound {
            git2::Error::from_str(&unable_to_read_object(oid))
        } else {
            e
        }
    })
}

/// Blob size without inflating content. Missing or non-blob OIDs map to
/// [`unable_to_read_object`] (same observable as local [`find_blob`]).
fn blob_odb_size(repo: &Repository, oid: git2::Oid) -> Result<usize, git2::Error> {
    let (size, kind) = repo.odb()?.read_header(oid).map_err(|e| {
        if e.code() == git2::ErrorCode::NotFound {
            git2::Error::from_str(&unable_to_read_object(oid))
        } else {
            e
        }
    })?;
    if kind != git2::ObjectType::Blob {
        return Err(git2::Error::from_str(&unable_to_read_object(oid)));
    }
    Ok(size)
}

enum ResolvedBlob<'a> {
    Cached(&'a [u8]),
    Loaded(git2::Blob<'a>),
}

impl ResolvedBlob<'_> {
    fn content(&self) -> &[u8] {
        match self {
            Self::Cached(bytes) => bytes,
            Self::Loaded(blob) => blob.content(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Cached(bytes) => bytes.len(),
            Self::Loaded(blob) => blob.size(),
        }
    }
}

fn resolve_blob<'a>(
    ring: &'a BlobRing,
    repo: &'a Repository,
    oid: git2::Oid,
    kind: LookupKind,
) -> Result<Option<ResolvedBlob<'a>>, git2::Error> {
    if oid.is_zero() {
        return Ok(None);
    }
    if let Some(bytes) = ring.lookup(oid) {
        record_lookup(kind, true, bytes.len());
        return Ok(Some(ResolvedBlob::Cached(bytes)));
    }
    let blob = find_blob(repo, oid)?;
    record_lookup(kind, false, blob.size());
    Ok(Some(ResolvedBlob::Loaded(blob)))
}

fn note_old(
    pending: &mut PendingOlds,
    cache_path: Option<&[u8]>,
    oid: git2::Oid,
    blob: &Option<ResolvedBlob<'_>>,
) {
    let Some(path) = cache_path else {
        return;
    };
    if oid.is_zero() {
        return;
    }
    match blob {
        Some(ResolvedBlob::Cached(_)) => pending.record_hit(path.to_vec(), oid),
        Some(ResolvedBlob::Loaded(loaded)) => {
            pending.record_miss(path.to_vec(), oid, loaded.content().to_vec())
        }
        None => {}
    }
}

fn typechange_size(
    ring: &BlobRing,
    repo: &Repository,
    id: git2::Oid,
) -> Result<Option<i64>, git2::Error> {
    if id.is_zero() {
        return Ok(None);
    }
    let hit_len = ring.lookup(id).map(|bytes| bytes.len());
    if let Some(len) = hit_len {
        record_lookup(LookupKind::Typechange, true, len);
        return Ok(Some(len as i64));
    }
    let size = blob_odb_size(repo, id)?;
    record_lookup(LookupKind::Typechange, false, size);
    Ok(Some(size as i64))
}

pub(super) fn emit_file_changes(
    repo: &Repository,
    commit: &git2::Commit,
    ignore_all_space: bool,
    sink: &mut impl CommitSink,
    ring: &mut BlobRing,
) -> Result<(), git2::Error> {
    let pending = emit_file_changes_inner(repo, commit, ignore_all_space, sink, ring)?;
    ring.finish_commit(pending);
    Ok(())
}

fn emit_file_changes_inner(
    repo: &Repository,
    commit: &git2::Commit,
    ignore_all_space: bool,
    sink: &mut impl CommitSink,
    ring: &BlobRing,
) -> Result<PendingOlds, git2::Error> {
    let current_tree = commit.tree()?;
    let parent_tree = if commit.parent_count() == 0 {
        None
    } else {
        Some(commit.parent(0)?.tree()?)
    };

    let mut diff_options = git2::DiffOptions::new();
    diff_options.include_typechange(true);
    if ignore_all_space {
        diff_options.ignore_whitespace(true);
    }

    let mut diff = repo.diff_tree_to_tree(
        parent_tree.as_ref(),
        Some(&current_tree),
        Some(&mut diff_options),
    )?;

    // Root commits are all Added; rename detection needs A+D pairs.
    if parent_tree.is_some() {
        let mut find_opts = git2::DiffFindOptions::new();
        find_opts
            .renames(true)
            .rename_threshold(50)
            .ignore_whitespace(ignore_all_space);
        diff.find_similar(Some(&mut find_opts))?;
    }

    let num_deltas = diff.deltas().len();
    sink.begin_file_changes(num_deltas);

    let mut pending = PendingOlds::default();
    for i in 0..num_deltas {
        let delta = diff.get_delta(i).unwrap();

        let status = match delta.status() {
            git2::Delta::Added => "A",
            git2::Delta::Deleted => "D",
            git2::Delta::Modified => "M",
            git2::Delta::Renamed => "R",
            git2::Delta::Copied => "C",
            git2::Delta::Typechange => "T",
            other => {
                return Err(git2::Error::from_str(&format!(
                    "unexpected diff delta status in commit history: {other:?}"
                )));
            }
        };

        let file_path = delta
            .new_file()
            .path_bytes()
            .or_else(|| delta.old_file().path_bytes())
            .unwrap_or(b"unknown");

        let old_path = match delta.status() {
            git2::Delta::Renamed | git2::Delta::Copied => delta.old_file().path_bytes(),
            _ => None,
        };

        let is_gitlink = delta.new_file().mode() == git2::FileMode::Commit
            || delta.old_file().mode() == git2::FileMode::Commit;
        let is_typechange = delta.status() == git2::Delta::Typechange;

        // libgit2 DiffFile.size is often 0 for A/D; prefer blob object size.
        let (blob_hex, file_size, add_lines, del_lines) = if is_gitlink || is_typechange {
            let id = if delta.new_file().path_bytes().is_some() {
                delta.new_file().id()
            } else if delta.old_file().path_bytes().is_some() {
                delta.old_file().id()
            } else {
                git2::Oid::ZERO_SHA1
            };
            let file_size = if is_gitlink {
                None
            } else {
                typechange_size(ring, repo, id)?
            };
            let (add, del) = gitlink_numstat(status);
            let hex = if id.is_zero() {
                None
            } else {
                Some(oid_hex(id.as_bytes()))
            };
            (hex, file_size, Some(add), Some(del))
        } else {
            let old_id = delta.old_file().id();
            let new_id = delta.new_file().id();
            let cache_path = match delta.status() {
                git2::Delta::Renamed | git2::Delta::Copied => delta.old_file().path_bytes(),
                _ => Some(file_path),
            };

            // chmod / content-identical rename: same blob OID. Open one side
            // for binary+size; skip Myers (`git` `may_differ = !oideq`).
            if old_id == new_id && !old_id.is_zero() {
                let blob = resolve_blob(ring, repo, new_id, LookupKind::New)?;
                let loaded = blob.as_ref().expect("non-zero oid");
                let file_size = Some(loaded.size() as i64);
                let (add, del) = same_oid_numstat(loaded.content());
                note_old(&mut pending, cache_path, old_id, &blob);
                (Some(oid_hex(new_id.as_bytes())), file_size, add, del)
            } else {
                let old_blob = resolve_blob(ring, repo, old_id, LookupKind::Old)?;
                let new_blob = resolve_blob(ring, repo, new_id, LookupKind::New)?;

                let (blob_hex, file_size) = if new_blob.is_some() {
                    (
                        Some(oid_hex(new_id.as_bytes())),
                        Some(new_blob.as_ref().unwrap().size() as i64),
                    )
                } else if old_blob.is_some() {
                    (
                        Some(oid_hex(old_id.as_bytes())),
                        Some(old_blob.as_ref().unwrap().size() as i64),
                    )
                } else {
                    (None, Some(0))
                };

                let old_content = old_blob.as_ref().map(|b| b.content()).unwrap_or(&[]);
                let new_content = new_blob.as_ref().map(|b| b.content()).unwrap_or(&[]);
                let line_counts =
                    super::xdiff::diff_line_counts(old_content, new_content, ignore_all_space)
                        .map_err(|e| git2::Error::from_str(&e.to_string()))?;
                let (add, del) = match line_counts {
                    Some((a, d)) => (Some(a), Some(d)),
                    None => (None, None),
                };

                note_old(&mut pending, cache_path, old_id, &old_blob);

                (blob_hex, file_size, add, del)
            }
        };

        let unknown = b"unknown";
        let blob_id: &[u8] = match &blob_hex {
            Some(h) => h.as_ref(),
            None => unknown,
        };

        sink.file_change(FileChangeRef {
            path: file_path,
            old_path,
            status,
            blob_id,
            file_size,
            add_lines,
            del_lines,
        });
    }

    Ok(pending)
}

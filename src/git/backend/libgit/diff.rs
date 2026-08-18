use super::blob_ring::{record_lookup, BlobRing, LookupKind, PendingOlds};
use crate::git::model::{gitlink_numstat, unable_to_read_object};
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
    let hit_len = ring.lookup(oid).map(|bytes| bytes.len());
    if let Some(len) = hit_len {
        record_lookup(kind, true, len);
        return Ok(Some(ResolvedBlob::Cached(
            ring.lookup(oid).expect("hit_len implies present"),
        )));
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
    let size = find_blob(repo, id)?.size();
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
    let pending = PendingOlds::default();
    if commit.parent_count() == 0 {
        emit_root_file_changes(repo, commit, sink, ring)?;
        return Ok(pending);
    }

    let parent = commit.parent(0)?;
    let parent_tree = parent.tree()?;
    let current_tree = commit.tree()?;

    let mut diff_options = git2::DiffOptions::new();
    diff_options.include_typechange(true);
    if ignore_all_space {
        diff_options.ignore_whitespace(true);
    }

    let mut diff = repo.diff_tree_to_tree(
        Some(&parent_tree),
        Some(&current_tree),
        Some(&mut diff_options),
    )?;

    let mut find_opts = git2::DiffFindOptions::new();
    find_opts
        .renames(true)
        .rename_threshold(50)
        .ignore_whitespace(ignore_all_space);

    diff.find_similar(Some(&mut find_opts))?;

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

        // libgit2 DiffFile.size is often 0 for A/D; prefer blob object size (like root commits).
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

            let cache_path = match delta.status() {
                git2::Delta::Renamed | git2::Delta::Copied => delta.old_file().path_bytes(),
                _ => Some(file_path),
            };
            note_old(&mut pending, cache_path, old_id, &old_blob);

            (blob_hex, file_size, add, del)
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

fn emit_root_file_changes(
    repo: &Repository,
    commit: &git2::Commit,
    sink: &mut impl CommitSink,
    ring: &BlobRing,
) -> Result<(), git2::Error> {
    let tree = commit.tree()?;
    let mut walk_err: Option<git2::Error> = None;
    let mut path_buf = String::new();
    let walk_result = tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if walk_err.is_some() {
            return git2::TreeWalkResult::Abort;
        }
        if entry.kind() == Some(git2::ObjectType::Tree) {
            return git2::TreeWalkResult::Ok;
        }
        let Ok(name) = entry.name() else {
            return git2::TreeWalkResult::Ok;
        };
        let oid = entry.id();
        path_buf.clear();
        path_buf.push_str(root);
        path_buf.push_str(name);
        let hex = oid_hex(oid.as_bytes());
        if entry.kind() == Some(git2::ObjectType::Commit) {
            let (add_lines, del_lines) = gitlink_numstat("A");
            sink.file_change(FileChangeRef {
                path: path_buf.as_bytes(),
                old_path: None,
                status: "A",
                blob_id: &hex,
                file_size: None,
                add_lines: Some(add_lines),
                del_lines: Some(del_lines),
            });
        } else {
            match resolve_blob(ring, repo, oid, LookupKind::New) {
                Ok(Some(blob)) => {
                    let content = blob.content();
                    let (add_lines, del_lines) = if super::xdiff::is_binary_content(content) {
                        (None, None)
                    } else {
                        (Some(super::xdiff::count_lines(content)), Some(0))
                    };
                    sink.file_change(FileChangeRef {
                        path: path_buf.as_bytes(),
                        old_path: None,
                        status: "A",
                        blob_id: &hex,
                        file_size: Some(blob.size() as i64),
                        add_lines,
                        del_lines,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    walk_err = Some(e);
                    return git2::TreeWalkResult::Abort;
                }
            }
        }
        git2::TreeWalkResult::Ok
    });
    // Prefer the blob error over tree-walk's generic Abort (-7).
    if let Some(e) = walk_err {
        return Err(e);
    }
    walk_result?;
    Ok(())
}

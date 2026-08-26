use crate::git::model::{gitlink_numstat, same_oid_numstat, unable_to_read_object};
use crate::git::sink::{oid_hex, CommitSink, FileChangeRef};
use std::ops::ControlFlow;

fn is_typechange(
    previous: gix::objs::tree::EntryMode,
    current: gix::objs::tree::EntryMode,
) -> bool {
    use gix::objs::tree::EntryKind;
    let class = |mode: gix::objs::tree::EntryMode| match mode.kind() {
        EntryKind::Blob | EntryKind::BlobExecutable => 0u8,
        EntryKind::Link => 1,
        EntryKind::Commit => 2,
        EntryKind::Tree => 3,
    };
    class(previous) != class(current)
}

/// When old and new blob OIDs are equal (chmod / content-identical rename),
/// `git log --numstat` skips Myers. `Id<'_>` is not `Eq`; compare `detach()`.
fn same_oid_for_numstat(
    change: &gix::object::tree::diff::Change<'_, '_, '_>,
) -> Option<gix::ObjectId> {
    use gix::object::tree::diff::Change;
    match change {
        Change::Modification {
            previous_id, id, ..
        } => {
            let prev = previous_id.detach();
            let cur = id.detach();
            (prev == cur && !prev.is_null()).then_some(cur)
        }
        Change::Rewrite { source_id, id, .. } => {
            let src = source_id.detach();
            let cur = id.detach();
            (src == cur && !src.is_null()).then_some(cur)
        }
        _ => None,
    }
}

/// OIDs whose blob content `git log --numstat` would need to read for this change.
fn blob_oids_for_numstat(
    change: &gix::object::tree::diff::Change<'_, '_, '_>,
) -> Vec<gix::ObjectId> {
    use gix::object::tree::diff::Change;
    match change {
        Change::Addition { id, .. } | Change::Deletion { id, .. } => vec![id.detach()],
        Change::Modification {
            previous_id, id, ..
        } => vec![previous_id.detach(), id.detach()],
        Change::Rewrite { source_id, id, .. } => vec![source_id.detach(), id.detach()],
    }
}

fn ensure_blobs_readable(
    repo: &gix::Repository,
    oids: &[gix::ObjectId],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for oid in oids {
        if oid.is_null() {
            continue;
        }
        if repo.try_find_object(*oid)?.is_none() {
            return Err(unable_to_read_object(oid).into());
        }
    }
    Ok(())
}

pub(super) fn emit_file_changes(
    repo: &gix::Repository,
    commit: &gix::Commit,
    sink: &mut impl CommitSink,
) -> Result<(), Box<dyn std::error::Error>> {
    let tree_id = commit.tree_id()?.detach();
    let parent_tree_id = if commit.parent_ids().count() == 0 {
        None
    } else {
        let parent_id = commit.parent_ids().next().unwrap().detach();
        Some(repo.find_commit(parent_id)?.tree_id()?.detach())
    };
    emit_file_changes_trees(repo, tree_id, parent_tree_id, sink)
}

/// Diff by tree OIDs so emit can skip `find_commit`. `parent_tree_id == None` means root
/// (`empty_tree`); never pass merge-skipped commits here.
pub(crate) fn emit_file_changes_trees(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    parent_tree_id: Option<gix::ObjectId>,
    sink: &mut impl CommitSink,
) -> Result<(), Box<dyn std::error::Error>> {
    let current_tree = repo.find_tree(tree_id)?;
    let parent_tree = match parent_tree_id {
        Some(id) => repo.find_tree(id)?,
        None => repo.empty_tree(),
    };

    let mut resource_cache = repo.diff_resource_cache_for_tree_diff()?;
    // Captured outside the callback so we can surface git's phrasing without
    // gix wrapping it as "The user-provided callback failed: …".
    let mut missing_blob: Option<String> = None;

    let walk = parent_tree.changes()?.for_each_to_obtain_tree(
        &current_tree,
        |change| -> Result<ControlFlow<()>, Box<dyn std::error::Error + Send + Sync>> {
            use gix::object::tree::diff::Change;

            // Directories are reported as their own Addition/Deletion/Modification
            // entries alongside their recursed-into children; skip them so only
            // blob-level changes are returned, matching the git2 backend.
            let (entry_mode, previous_mode) = match &change {
                Change::Addition { entry_mode, .. } => (*entry_mode, None),
                Change::Deletion { entry_mode, .. } => (*entry_mode, None),
                Change::Modification {
                    entry_mode,
                    previous_entry_mode,
                    ..
                } => (*entry_mode, Some(*previous_entry_mode)),
                Change::Rewrite {
                    entry_mode,
                    source_entry_mode,
                    ..
                } => (*entry_mode, Some(*source_entry_mode)),
            };
            if entry_mode.is_tree() {
                return Ok(ControlFlow::Continue(()));
            }

            let typechange = previous_mode.is_some_and(|prev| is_typechange(prev, entry_mode));

            let location = change.location();
            let (status, old_path): (&'static str, Option<&[u8]>) = match &change {
                Change::Addition { .. } => ("A", None),
                Change::Deletion { .. } => ("D", None),
                Change::Modification { .. } if typechange => ("T", None),
                Change::Modification { .. } => ("M", None),
                Change::Rewrite {
                    copy: true,
                    source_location,
                    ..
                } => ("C", Some(source_location.as_ref())),
                Change::Rewrite {
                    copy: false,
                    source_location,
                    ..
                } => ("R", Some(source_location.as_ref())),
            };

            let id = change.id();
            let is_gitlink = entry_mode.is_commit() || previous_mode.is_some_and(|m| m.is_commit());

            let file_size = if is_gitlink {
                None
            } else {
                id.try_header().ok().flatten().map(|h| h.size() as i64)
            };

            let (add_lines, del_lines) = if is_gitlink || typechange {
                let (a, d) = gitlink_numstat(status);
                (Some(a), Some(d))
            } else if let Some(oid) = same_oid_for_numstat(&change) {
                if let Err(e) = ensure_blobs_readable(repo, &[oid]) {
                    missing_blob = Some(e.to_string());
                    return Err(e);
                }
                let obj = repo.find_object(oid)?;
                same_oid_numstat(obj.data.as_ref())
            } else {
                let oids = blob_oids_for_numstat(&change);
                if let Err(e) = ensure_blobs_readable(repo, &oids) {
                    missing_blob = Some(e.to_string());
                    return Err(e);
                }
                let mut platform = change.diff(&mut resource_cache)?;
                match platform.line_counts()? {
                    Some(counts) => (Some(counts.insertions as i32), Some(counts.removals as i32)),
                    None => (None, None),
                }
            };

            resource_cache.clear_resource_cache_keep_allocation();

            let hex = oid_hex(id.as_bytes());
            sink.file_change(FileChangeRef {
                path: location.as_ref(),
                old_path,
                status,
                blob_id: &hex,
                file_size,
                add_lines,
                del_lines,
            });

            Ok(ControlFlow::Continue(()))
        },
    );

    if let Some(msg) = missing_blob {
        return Err(msg.into());
    }
    walk?;

    Ok(())
}

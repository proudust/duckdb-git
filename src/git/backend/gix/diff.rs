use crate::git::model::{gitlink_numstat, FileChange};
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

pub(super) fn collect_file_changes(
    repo: &gix::Repository,
    commit: &gix::Commit,
) -> Result<Vec<FileChange>, Box<dyn std::error::Error>> {
    let mut file_changes = Vec::new();
    let current_tree = commit.tree()?;

    let parent_tree = if commit.parent_ids().count() == 0 {
        repo.empty_tree()
    } else {
        let parent_id = commit.parent_ids().next().unwrap().detach();
        repo.find_commit(parent_id)?.tree()?
    };

    let mut resource_cache = repo.diff_resource_cache_for_tree_diff()?;

    parent_tree.changes()?.for_each_to_obtain_tree(
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

            let location = change.location().to_string();
            let (status, old_path): (&'static str, Option<String>) = match &change {
                Change::Addition { .. } => ("A", None),
                Change::Deletion { .. } => ("D", None),
                Change::Modification { .. } if typechange => ("T", None),
                Change::Modification { .. } => ("M", None),
                Change::Rewrite {
                    copy: true,
                    source_location,
                    ..
                } => (
                    "C",
                    Some(String::from_utf8_lossy(source_location).into_owned()),
                ),
                Change::Rewrite {
                    copy: false,
                    source_location,
                    ..
                } => (
                    "R",
                    Some(String::from_utf8_lossy(source_location).into_owned()),
                ),
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
            } else {
                let mut platform = change.diff(&mut resource_cache)?;
                match platform.line_counts()? {
                    Some(counts) => (Some(counts.insertions as i32), Some(counts.removals as i32)),
                    None => (None, None),
                }
            };

            resource_cache.clear_resource_cache_keep_allocation();

            file_changes.push(FileChange {
                path: location,
                old_path,
                status,
                blob_id: id.to_string(),
                file_size,
                add_lines,
                del_lines,
            });

            Ok(ControlFlow::Continue(()))
        },
    )?;

    Ok(file_changes)
}

use crate::git_log::types::{gitlink_numstat, FileChange};

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

    parent_tree
        .changes()?
        .for_each_to_obtain_tree(&current_tree, |change| {
            use gix::object::tree::diff::Change;

            // Directories are reported as their own Addition/Deletion/Modification
            // entries alongside their recursed-into children; skip them so only
            // blob-level changes are returned, matching the git2 backend.
            let entry_mode = match &change {
                Change::Addition { entry_mode, .. } => *entry_mode,
                Change::Deletion { entry_mode, .. } => *entry_mode,
                Change::Modification { entry_mode, .. } => *entry_mode,
                Change::Rewrite { entry_mode, .. } => *entry_mode,
            };
            if entry_mode.is_tree() {
                return Ok::<_, std::convert::Infallible>(
                    gix::object::tree::diff::Action::Continue(()),
                );
            }

            let location = change.location().to_string();
            let (status, old_path): (&'static str, Option<String>) = match &change {
                Change::Addition { .. } => ("A", None),
                Change::Deletion { .. } => ("D", None),
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
            let is_gitlink = entry_mode.is_commit();

            let file_size = if is_gitlink {
                None
            } else {
                id.try_header().ok().flatten().map(|h| h.size() as i64)
            };

            let (add_lines, del_lines) = if is_gitlink {
                gitlink_numstat(status)
            } else {
                change
                    .diff(&mut resource_cache)
                    .ok()
                    .and_then(|mut platform| platform.line_counts().ok())
                    .flatten()
                    .map(|counts| (counts.insertions as i32, counts.removals as i32))
                    .unwrap_or((0, 0))
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

            Ok::<_, std::convert::Infallible>(gix::object::tree::diff::Action::Continue(()))
        })?;

    Ok(file_changes)
}

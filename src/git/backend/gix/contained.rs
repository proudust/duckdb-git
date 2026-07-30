use crate::git::options::DecorateFormat;
use crate::git::ref_index::{ContainedIndex, RefBits};
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub(crate) fn build_contained_index(
    repo: &gix::Repository,
    format: DecorateFormat,
    need_branches: bool,
    need_tags: bool,
    wanted: &HashSet<gix::ObjectId>,
) -> Result<ContainedIndex<gix::ObjectId>, Box<dyn Error>> {
    let platform = repo.references()?;

    let collect = |iter: gix::reference::iter::Iter<'_, '_>,
                   out: &mut Vec<(gix::ObjectId, String)>|
     -> Result<(), Box<dyn Error>> {
        for reference in iter {
            let mut reference = reference.map_err(|e| e.to_string())?;
            if matches!(reference.inner.target, gix::refs::Target::Symbolic(_)) {
                continue;
            }
            let name = match format {
                DecorateFormat::Short => reference.name().shorten().to_string(),
                DecorateFormat::Full => reference.name().as_bstr().to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(commit) = reference.peel_to_commit() {
                out.push((commit.id, name));
            }
        }
        Ok(())
    };

    let mut branch_refs = Vec::new();
    if need_branches {
        collect(platform.local_branches()?, &mut branch_refs)?;
        collect(platform.remote_branches()?, &mut branch_refs)?;
    }

    let mut tag_refs = Vec::new();
    if need_tags {
        collect(platform.tags()?, &mut tag_refs)?;
    }

    if branch_refs.is_empty() && tag_refs.is_empty() {
        return Ok(ContainedIndex::empty());
    }

    let mut branch_names: Vec<String> = branch_refs.iter().map(|(_, name)| name.clone()).collect();
    branch_names.sort_unstable();
    branch_names.dedup();

    let mut tag_names: Vec<String> = tag_refs.iter().map(|(_, name)| name.clone()).collect();
    tag_names.sort_unstable();
    tag_names.dedup();

    let branch_words = branch_names.len().div_ceil(64);
    let tag_words = tag_names.len().div_ceil(64);
    let total_words = branch_words + tag_words;

    if wanted.is_empty() {
        return Ok(ContainedIndex {
            branch_names,
            tag_names,
            branch_words,
            bits: HashMap::new(),
        });
    }

    let mut pending: HashMap<gix::ObjectId, RefBits> = HashMap::new();
    for (tip, name) in &branch_refs {
        let bit = branch_names
            .binary_search(name)
            .expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
    }
    for (tip, name) in &tag_refs {
        let bit = branch_words * 64 + tag_names.binary_search(name).expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
    }

    let tips: Vec<gix::ObjectId> = pending.keys().copied().collect();
    let mut parents_of: HashMap<gix::ObjectId, gix::traverse::commit::ParentIds> = HashMap::new();
    let mut child_count: HashMap<gix::ObjectId, usize> = HashMap::new();
    for info in repo.rev_walk(tips).all()? {
        let info = info?;
        child_count.entry(info.id).or_insert(0);
        for parent in &info.parent_ids {
            *child_count.entry(*parent).or_insert(0) += 1;
        }
        parents_of.insert(info.id, info.parent_ids);
    }

    let mut queue: Vec<gix::ObjectId> = child_count
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut bits: HashMap<gix::ObjectId, RefBits> = HashMap::new();
    while let Some(id) = queue.pop() {
        let cur_bits = pending.remove(&id);
        for parent in parents_of
            .get(&id)
            .map(|p| p.as_slice())
            .unwrap_or_default()
        {
            if let Some(cur_bits) = cur_bits.as_ref() {
                pending
                    .entry(*parent)
                    .or_insert_with(|| RefBits::new(total_words))
                    .or_assign(cur_bits);
            }
            // A parent becomes ready once every child has propagated into it.
            if let Some(count) = child_count.get_mut(parent) {
                *count -= 1;
                if *count == 0 {
                    queue.push(*parent);
                }
            }
        }
        let Some(cur_bits) = cur_bits else {
            continue;
        };
        if wanted.contains(&id) {
            bits.insert(id, cur_bits);
            if bits.len() == wanted.len() {
                break;
            }
        }
    }

    Ok(ContainedIndex {
        branch_names,
        tag_names,
        branch_words,
        bits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn contained_index_tags_marks_all_descendant_tags() {
        let repo = gix::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [tagged_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, false, true, &wanted).unwrap();
        let names: Vec<&str> = index.tags_of(&tagged_oid).collect();
        assert!(
            names.windows(2).all(|w| w[0] <= w[1]),
            "tags must be sorted, got {names:?}"
        );
        // Every release tag descends from v0.1.1's commit. Membership only, so a
        // new release tag does not break this; exclusion is covered exactly by
        // contained_index_is_exact_for_merge_fan_in.
        for tag in ["v0.1.1", "v0.1.2", "v0.2.0", "v0.3.0", "v0.4.0"] {
            assert!(names.contains(&tag), "{tag} missing from {names:?}");
        }
    }

    #[test]
    fn contained_index_tags_full_format() {
        let repo = gix::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [tagged_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Full, false, true, &wanted).unwrap();
        assert!(index.tags_of(&tagged_oid).any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn contained_index_branches_marks_ancestor() {
        let repo = gix::open(".").unwrap();
        let head_name = repo.head_name().unwrap().unwrap();
        let head_name = head_name.shorten().to_string();
        let second_oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [second_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&second_oid).any(|n| n == head_name));
        // Remote-tracking branches are included too.
        assert!(index.branches_of(&second_oid).any(|n| n == "origin/main"));
    }

    #[test]
    fn contained_index_branches_skips_symbolic_head_alias() {
        let repo = gix::open(".").unwrap();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &HashSet::new())
                .unwrap();
        assert!(!index.branch_names.iter().any(|n| n == "origin/HEAD"));
    }

    #[test]
    fn contained_index_branches_self_inclusive() {
        let repo = gix::open(".").unwrap();
        let head_oid = repo.head_id().unwrap().detach();
        let head_name = repo.head_name().unwrap().unwrap();
        let head_name = head_name.shorten().to_string();
        let wanted: HashSet<gix::ObjectId> = [head_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&head_oid).any(|n| n == head_name));
    }

    fn init_fan_in_repo() -> (tempfile::TempDir, [gix::ObjectId; 4]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let sig = gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(0, 0),
        };
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();

        // Written via write_object rather than commit_as: the latter requires the
        // updated ref to already point at the first parent, which C (branching off A
        // while the ref is at B) would violate.
        let commit = |message: &str, parents: Vec<gix::ObjectId>| {
            repo.write_object(&gix::objs::Commit {
                tree,
                parents: parents.into(),
                author: sig.clone(),
                committer: sig.clone(),
                encoding: None,
                message: message.into(),
                extra_headers: Vec::new(),
            })
            .unwrap()
            .detach()
        };

        let a_id = commit("A", vec![]);
        let b_id = commit("B", vec![a_id]);
        let c_id = commit("C", vec![a_id]);
        let d_id = commit("D", vec![b_id, c_id]);

        use gix::refs::transaction::PreviousValue;
        repo.tag_reference("left", b_id, PreviousValue::Any)
            .unwrap();
        repo.tag_reference("right", c_id, PreviousValue::Any)
            .unwrap();
        repo.tag_reference("merged", d_id, PreviousValue::Any)
            .unwrap();

        (dir, [a_id, b_id, c_id, d_id])
    }

    #[test]
    fn contained_index_is_exact_for_merge_fan_in() {
        let (dir, [a_id, b_id, c_id, d_id]) = init_fan_in_repo();
        let repo = gix::open(dir.path()).unwrap();

        let expected = [
            (d_id, vec!["merged"]),
            (b_id, vec!["left", "merged"]),
            (c_id, vec!["merged", "right"]),
            (a_id, vec!["left", "merged", "right"]),
        ];

        let all: HashSet<gix::ObjectId> = [a_id, b_id, c_id, d_id].into_iter().collect();
        let index = build_contained_index(&repo, DecorateFormat::Short, false, true, &all).unwrap();
        for (oid, want) in &expected {
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        // Asking for one commit at a time lets the walk stop early; the answer
        // must not change.
        for (oid, want) in &expected {
            let wanted: HashSet<gix::ObjectId> = [*oid].into_iter().collect();
            let index =
                build_contained_index(&repo, DecorateFormat::Short, false, true, &wanted).unwrap();
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        let index =
            build_contained_index(&repo, DecorateFormat::Short, false, true, &HashSet::new())
                .unwrap();
        assert_eq!(index.tag_names, vec!["left", "merged", "right"]);
        assert_eq!(index.tags_of(&d_id).count(), 0);
    }

    const MULTI_WORD_N: usize = 70;

    fn ref_names(prefix: char, step: usize) -> Vec<String> {
        (0..MULTI_WORD_N)
            .step_by(step)
            .map(|i| format!("{prefix}{i:03}"))
            .collect()
    }

    fn init_multi_word_repo() -> (tempfile::TempDir, [gix::ObjectId; 2]) {
        let dir = tempfile::tempdir().unwrap();
        let mut repo = gix::init(dir.path()).unwrap();
        // Creating refs/heads/* writes a reflog, which needs a committer identity.
        // CI has no global git config, so set one on the test repo itself.
        let mut config = repo.config_snapshot_mut();
        config
            .set_raw_value(gix::config::tree::User::NAME, "Test")
            .unwrap();
        config
            .set_raw_value(gix::config::tree::User::EMAIL, "test@example.com")
            .unwrap();
        drop(config);
        let sig = gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(0, 0),
        };
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();

        let commit = |message: &str, parents: Vec<gix::ObjectId>| {
            repo.write_object(&gix::objs::Commit {
                tree,
                parents: parents.into(),
                author: sig.clone(),
                committer: sig.clone(),
                encoding: None,
                message: message.into(),
                extra_headers: Vec::new(),
            })
            .unwrap()
            .detach()
        };

        let root_id = commit("root", vec![]);
        let tip_id = commit("tip", vec![root_id]);

        use gix::refs::transaction::PreviousValue;
        for i in 0..MULTI_WORD_N {
            let target = if i % 2 == 0 { tip_id } else { root_id };
            repo.reference(
                format!("refs/heads/b{i:03}"),
                target,
                PreviousValue::Any,
                "test",
            )
            .unwrap();
            repo.tag_reference(format!("t{i:03}"), target, PreviousValue::Any)
                .unwrap();
        }

        (dir, [root_id, tip_id])
    }

    #[test]
    fn contained_index_multi_word_refbits() {
        let (dir, [root_id, tip_id]) = init_multi_word_repo();
        let repo = gix::open(dir.path()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [root_id, tip_id].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, true, &wanted).unwrap();

        assert!(
            index.branch_words > 1,
            "test must exercise RefBits::Words, not Inline"
        );

        // Even-numbered refs point at the tip, odd ones at the root, so the root
        // (an ancestor of both) is contained in all of them.
        for (label, actual, expected) in [
            (
                "tip branches",
                index
                    .branches_of(&tip_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('b', 2),
            ),
            (
                "tip tags",
                index.tags_of(&tip_id).map(String::from).collect::<Vec<_>>(),
                ref_names('t', 2),
            ),
            (
                "root branches",
                index
                    .branches_of(&root_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('b', 1),
            ),
            (
                "root tags",
                index
                    .tags_of(&root_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('t', 1),
            ),
        ] {
            assert_eq!(actual, expected, "{label}");
        }
    }
}

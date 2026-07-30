use crate::git::options::DecorateFormat;
use crate::git::ref_index::{ContainedIndex, RefBits};
use git2::Repository;
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub(crate) fn build_contained_index(
    repo: &Repository,
    format: DecorateFormat,
    need_branches: bool,
    need_tags: bool,
    wanted: &HashSet<git2::Oid>,
) -> Result<ContainedIndex<git2::Oid>, Box<dyn Error>> {
    let mut branch_refs = Vec::new();
    if need_branches {
        for branch in repo.branches(None)? {
            let (branch, _branch_type) = branch?;
            let reference = branch.get();
            if reference.kind() != Some(git2::ReferenceType::Direct) {
                continue;
            }
            let name = match format {
                DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
                DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(tip) = reference.peel_to_commit() {
                branch_refs.push((tip.id(), name));
            }
        }
    }

    let mut tag_refs = Vec::new();
    if need_tags {
        for reference in repo.references()? {
            let reference = reference?;
            if !reference.is_tag() {
                continue;
            }
            let name = match format {
                DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
                DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(tip) = reference.peel_to_commit() {
                tag_refs.push((tip.id(), name));
            }
        }
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

    let mut revwalk = repo.revwalk()?;
    revwalk.set_sorting(git2::Sort::TOPOLOGICAL)?;

    let mut pending: HashMap<git2::Oid, RefBits> = HashMap::new();
    for (tip, name) in &branch_refs {
        let bit = branch_names
            .binary_search(name)
            .expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
        revwalk.push(*tip)?;
    }
    for (tip, name) in &tag_refs {
        let bit = branch_words * 64 + tag_names.binary_search(name).expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
        revwalk.push(*tip)?;
    }

    let mut bits: HashMap<git2::Oid, RefBits> = HashMap::new();
    for oid in revwalk {
        let oid = oid?;
        let Some(cur_bits) = pending.remove(&oid) else {
            continue;
        };
        let commit = repo.find_commit(oid)?;
        for parent in commit.parent_ids() {
            pending
                .entry(parent)
                .or_insert_with(|| RefBits::new(total_words))
                .or_assign(&cur_bits);
        }
        if wanted.contains(&oid) {
            bits.insert(oid, cur_bits);
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
        let repo = Repository::open(".").unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [tagged_oid].into_iter().collect();
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
        let repo = Repository::open(".").unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [tagged_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Full, false, true, &wanted).unwrap();
        assert!(index.tags_of(&tagged_oid).any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn contained_index_branches_marks_ancestor() {
        let repo = Repository::open(".").unwrap();
        let head = repo.head().unwrap();
        let head_name = head.shorthand().unwrap().to_string();
        let second_oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [second_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&second_oid).any(|n| n == head_name));
        // Remote-tracking branches are included too.
        assert!(index.branches_of(&second_oid).any(|n| n == "origin/main"));
    }

    #[test]
    fn contained_index_branches_skips_symbolic_head_alias() {
        let repo = Repository::open(".").unwrap();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &HashSet::new())
                .unwrap();
        assert!(!index.branch_names.iter().any(|n| n == "origin/HEAD"));
    }

    #[test]
    fn contained_index_branches_self_inclusive() {
        let repo = Repository::open(".").unwrap();
        let head = repo.head().unwrap();
        let head_oid = head.peel_to_commit().unwrap().id();
        let head_name = head.shorthand().unwrap().to_string();
        let wanted: HashSet<git2::Oid> = [head_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&head_oid).any(|n| n == head_name));
    }

    fn init_fan_in_repo() -> (tempfile::TempDir, [git2::Oid; 4]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();

        let a_id = repo.commit(None, &sig, &sig, "A", &tree, &[]).unwrap();
        let a = repo.find_commit(a_id).unwrap();
        let b_id = repo.commit(None, &sig, &sig, "B", &tree, &[&a]).unwrap();
        let b = repo.find_commit(b_id).unwrap();
        let c_id = repo.commit(None, &sig, &sig, "C", &tree, &[&a]).unwrap();
        let c = repo.find_commit(c_id).unwrap();
        let d_id = repo
            .commit(None, &sig, &sig, "D", &tree, &[&b, &c])
            .unwrap();
        let d = repo.find_commit(d_id).unwrap();

        repo.tag_lightweight("left", b.as_object(), false).unwrap();
        repo.tag_lightweight("right", c.as_object(), false).unwrap();
        repo.tag_lightweight("merged", d.as_object(), false)
            .unwrap();

        (dir, [a_id, b_id, c_id, d_id])
    }

    #[test]
    fn contained_index_is_exact_for_merge_fan_in() {
        let (dir, [a_id, b_id, c_id, d_id]) = init_fan_in_repo();
        let repo = Repository::open(dir.path()).unwrap();

        let expected = [
            (d_id, vec!["merged"]),
            (b_id, vec!["left", "merged"]),
            (c_id, vec!["merged", "right"]),
            (a_id, vec!["left", "merged", "right"]),
        ];

        let all: HashSet<git2::Oid> = [a_id, b_id, c_id, d_id].into_iter().collect();
        let index = build_contained_index(&repo, DecorateFormat::Short, false, true, &all).unwrap();
        for (oid, want) in &expected {
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        // Asking for one commit at a time lets the walk stop early; the answer
        // must not change.
        for (oid, want) in &expected {
            let wanted: HashSet<git2::Oid> = [*oid].into_iter().collect();
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

    fn init_multi_word_repo() -> (tempfile::TempDir, [git2::Oid; 2]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();

        let root_id = repo.commit(None, &sig, &sig, "root", &tree, &[]).unwrap();
        let root = repo.find_commit(root_id).unwrap();
        let tip_id = repo
            .commit(None, &sig, &sig, "tip", &tree, &[&root])
            .unwrap();
        let tip = repo.find_commit(tip_id).unwrap();

        for i in 0..MULTI_WORD_N {
            let target = if i % 2 == 0 { &tip } else { &root };
            repo.branch(&format!("b{i:03}"), target, false).unwrap();
            repo.tag_lightweight(&format!("t{i:03}"), target.as_object(), false)
                .unwrap();
        }

        (dir, [root_id, tip_id])
    }

    #[test]
    fn contained_index_multi_word_refbits() {
        let (dir, [root_id, tip_id]) = init_multi_word_repo();
        let repo = Repository::open(dir.path()).unwrap();
        let wanted: HashSet<git2::Oid> = [root_id, tip_id].into_iter().collect();
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

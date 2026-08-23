use crate::git::options::DecorateFormat;
use git2::Repository;
use std::collections::HashMap;
use std::error::Error;

pub(crate) fn collect_refs(
    repo: &Repository,
    format: DecorateFormat,
) -> Result<HashMap<git2::Oid, Vec<String>>, Box<dyn Error>> {
    let mut refs_map: HashMap<git2::Oid, Vec<String>> = HashMap::new();
    for reference in repo.references()? {
        let reference = reference?;
        let name = match format {
            DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
            DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
        };
        if name.is_empty() {
            continue;
        }
        if let Ok(commit) = reference.peel_to_commit() {
            refs_map.entry(commit.id()).or_default().push(name);
        }
    }
    // `references()` does not list the repo-root HEAD; add it like `git log --decorate`.
    // Err / unborn / peel failure: skip (do not fail the whole decorate map).
    // Short and Full both use the literal "HEAD" (never head.name() / refs/HEAD).
    if let Ok(head) = repo.head() {
        if let Ok(commit) = head.peel_to_commit() {
            let names = refs_map.entry(commit.id()).or_default();
            if !names.iter().any(|n| n == "HEAD") {
                names.push("HEAD".to_string());
            }
        }
    }
    Ok(refs_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use git2::{Oid, Repository, Signature};

    const PARITY: &str = "test/fixtures/parity.git";
    /// Annotated tag `v1` peels to this commit.
    const V1_COMMIT: &str = "ff09a62b129cc936f13bc67c5e2dba84f397c64b";
    /// Reachable commit that is not a decorate tip.
    const NON_TIP: &str = "c536dab42a2d4c0468b8a72f83276e751e826f60";
    /// Current HEAD of parity.git (`master` / `padded-author` / `amended`).
    const HEAD_TIP: &str = "abe0518abd296b9b9a45755e12ec6ebec26c17a7";

    #[test]
    fn collect_refs_peels_annotated_tag_to_commit() {
        let repo = Repository::open(PARITY).unwrap();
        let tagged_oid = Oid::from_str(V1_COMMIT).unwrap();
        let non_tip = Oid::from_str(NON_TIP).unwrap();

        for (format, expected) in [
            (DecorateFormat::Short, "v1"),
            (DecorateFormat::Full, "refs/tags/v1"),
        ] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs
                .get(&tagged_oid)
                .expect("tagged commit should have refs");
            assert!(names.iter().any(|n| n == expected), "{format:?}");
            assert!(!refs.contains_key(&non_tip), "{format:?}");
        }
    }

    #[test]
    fn collect_refs_includes_literal_head_on_attached_tip() {
        let repo = Repository::open(PARITY).unwrap();
        let head_tip = Oid::from_str(HEAD_TIP).unwrap();
        let non_tip = Oid::from_str(NON_TIP).unwrap();

        for format in [DecorateFormat::Short, DecorateFormat::Full] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs.get(&head_tip).expect("HEAD tip should have refs");
            assert_eq!(
                names.iter().filter(|n| *n == "HEAD").count(),
                1,
                "{format:?}: expected exactly one literal HEAD, got {names:?}"
            );
            assert!(
                !names.iter().any(|n| n == "refs/HEAD"),
                "{format:?}: must not use refs/HEAD"
            );
            assert!(!refs.contains_key(&non_tip), "{format:?}");
        }
    }

    fn empty_tree_commit(repo: &Repository, msg: &str, parents: &[&git2::Commit<'_>]) -> Oid {
        let sig = Signature::now("Test", "test@example.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(None, &sig, &sig, msg, &tree, parents)
            .unwrap()
    }

    #[test]
    fn collect_refs_includes_head_when_detached() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        let a = empty_tree_commit(&repo, "A", &[]);
        let a_commit = repo.find_commit(a).unwrap();
        let b = empty_tree_commit(&repo, "B", &[&a_commit]);
        let b_obj = repo.find_commit(b).unwrap().into_object();
        repo.tag_lightweight("other", &b_obj, false).unwrap();
        repo.set_head_detached(a).unwrap();

        for format in [DecorateFormat::Short, DecorateFormat::Full] {
            let refs = collect_refs(&repo, format).unwrap();
            assert_eq!(
                refs.get(&a).cloned().map(|mut n| {
                    n.sort();
                    n
                }),
                Some(vec!["HEAD".to_string()]),
                "{format:?}"
            );
            let other = refs.get(&b).expect("other tip");
            assert!(other.iter().any(|n| n == "other" || n == "refs/tags/other"));
            assert!(!other.iter().any(|n| n == "HEAD"), "{format:?}");
        }
    }

    #[test]
    fn collect_refs_skips_unborn_head_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        let oid = empty_tree_commit(&repo, "alone", &[]);
        let obj = repo.find_commit(oid).unwrap().into_object();
        repo.tag_lightweight("keep", &obj, false).unwrap();
        // Unborn: symbolic HEAD to a branch that does not exist.
        repo.set_head("refs/heads/does-not-exist").unwrap();
        assert!(repo.head().is_err());

        for format in [DecorateFormat::Short, DecorateFormat::Full] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs.get(&oid).expect("tag tip should remain");
            assert!(names.iter().any(|n| n == "keep" || n == "refs/tags/keep"));
            assert!(!names.iter().any(|n| n == "HEAD"), "{format:?}");
        }
    }
}

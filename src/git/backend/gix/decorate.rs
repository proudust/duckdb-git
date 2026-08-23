use crate::git::options::DecorateFormat;
use std::collections::HashMap;
use std::error::Error;

pub(crate) fn collect_refs(
    repo: &gix::Repository,
    format: DecorateFormat,
) -> Result<HashMap<gix::ObjectId, Vec<String>>, Box<dyn Error>> {
    let mut refs_map: HashMap<gix::ObjectId, Vec<String>> = HashMap::new();

    let platform = repo.references()?;
    for reference in platform.all()? {
        let mut reference = reference.map_err(|e| e.to_string())?;
        let name = match format {
            DecorateFormat::Short => reference.name().shorten().to_string(),
            DecorateFormat::Full => reference.name().as_bstr().to_string(),
        };
        if name.is_empty() {
            continue;
        }
        if let Ok(commit) = reference.peel_to_commit() {
            refs_map.entry(commit.id).or_default().push(name);
        }
    }
    // `references().all()` does not list the repo-root HEAD; add it like `git log --decorate`.
    // Err / unborn / peel failure: skip (do not fail the whole decorate map).
    // Short and Full both use the literal "HEAD" (never head.name() / refs/HEAD).
    if let Ok(mut head) = repo.head() {
        if let Ok(commit) = head.peel_to_commit() {
            let names = refs_map.entry(commit.id).or_default();
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
    use gix::refs::transaction::{Change, PreviousValue, RefEdit};
    use gix::refs::Target;

    const PARITY: &str = "test/fixtures/parity.git";
    /// Annotated tag `v1` peels to this commit.
    const V1_COMMIT: &str = "ff09a62b129cc936f13bc67c5e2dba84f397c64b";
    /// Reachable commit that is not a decorate tip.
    const NON_TIP: &str = "c536dab42a2d4c0468b8a72f83276e751e826f60";
    /// Current HEAD of parity.git (`master` / `padded-author` / `amended`).
    const HEAD_TIP: &str = "abe0518abd296b9b9a45755e12ec6ebec26c17a7";

    #[test]
    fn collect_refs_peels_annotated_tag_to_commit() {
        let repo = gix::open(PARITY).unwrap();
        let tagged_oid = gix::ObjectId::from_hex(V1_COMMIT.as_bytes()).unwrap();
        let non_tip = gix::ObjectId::from_hex(NON_TIP.as_bytes()).unwrap();

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
        let repo = gix::open(PARITY).unwrap();
        let head_tip = gix::ObjectId::from_hex(HEAD_TIP.as_bytes()).unwrap();
        let non_tip = gix::ObjectId::from_hex(NON_TIP.as_bytes()).unwrap();

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

    fn init_test_repo(dir: &std::path::Path) -> gix::Repository {
        let mut repo = gix::init(dir).unwrap();
        // Editing HEAD writes a reflog, which needs a committer identity.
        // CI has no global git config, so set one on the test repo itself.
        let mut config = repo.config_snapshot_mut();
        config
            .set_raw_value(gix::config::tree::User::NAME, "Test")
            .unwrap();
        config
            .set_raw_value(gix::config::tree::User::EMAIL, "test@example.com")
            .unwrap();
        drop(config);
        repo
    }

    fn write_commit(repo: &gix::Repository, message: &str, parents: Vec<gix::ObjectId>) -> gix::ObjectId {
        let sig = gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(0, 0),
        };
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();
        repo.write_object(&gix::objs::Commit {
            tree,
            parents: parents.into(),
            author: sig.clone(),
            committer: sig,
            encoding: None,
            message: message.into(),
            extra_headers: Vec::new(),
        })
        .unwrap()
        .detach()
    }

    fn set_head_target(repo: &gix::Repository, new: Target) {
        repo.edit_reference(RefEdit {
            change: Change::Update {
                log: Default::default(),
                expected: PreviousValue::Any,
                new,
            },
            name: "HEAD".try_into().unwrap(),
            deref: false,
        })
        .unwrap();
    }

    #[test]
    fn collect_refs_includes_head_when_detached() {
        let dir = tempfile::tempdir().unwrap();
        let repo = init_test_repo(dir.path());
        let a = write_commit(&repo, "A", vec![]);
        let b = write_commit(&repo, "B", vec![a]);
        repo.tag_reference("other", b, PreviousValue::Any).unwrap();
        set_head_target(&repo, Target::Object(a));

        for format in [DecorateFormat::Short, DecorateFormat::Full] {
            let refs = collect_refs(&repo, format).unwrap();
            assert_eq!(
                refs.get(&a).map(|n| {
                    let mut v = n.clone();
                    v.sort();
                    v
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
        let repo = init_test_repo(dir.path());
        let oid = write_commit(&repo, "alone", vec![]);
        repo.tag_reference("keep", oid, PreviousValue::Any).unwrap();
        set_head_target(
            &repo,
            Target::Symbolic("refs/heads/does-not-exist".try_into().unwrap()),
        );

        for format in [DecorateFormat::Short, DecorateFormat::Full] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs.get(&oid).expect("tag tip should remain");
            assert!(names.iter().any(|n| n == "keep" || n == "refs/tags/keep"));
            assert!(!names.iter().any(|n| n == "HEAD"), "{format:?}");
        }
    }
}

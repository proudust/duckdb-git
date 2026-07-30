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
    Ok(refs_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn collect_refs_peels_annotated_tag_to_commit() {
        let repo = gix::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let second_oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();

        for (format, expected) in [
            (DecorateFormat::Short, "v0.1.1"),
            (DecorateFormat::Full, "refs/tags/v0.1.1"),
        ] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs
                .get(&tagged_oid)
                .expect("tagged commit should have refs");
            assert!(names.iter().any(|n| n == expected), "{format:?}");
            // A commit that is not a ref tip gets no entry at all.
            assert!(!refs.contains_key(&second_oid), "{format:?}");
        }
    }
}

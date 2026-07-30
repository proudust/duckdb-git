use std::error::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RevisionTerm {
    pub spec: String,
    pub negate: bool,
    pub origin: String,
}

pub(crate) fn unresolved_revision_error(origin: &str) -> String {
    if origin.starts_with('^') {
        format!("bad revision '{origin}'")
    } else {
        format!("ambiguous argument '{origin}': unknown revision or path not in the working tree.")
    }
}

pub(crate) fn parse_revision_terms(tokens: &[String]) -> Result<Vec<RevisionTerm>, Box<dyn Error>> {
    let mut terms = Vec::new();
    for token in tokens {
        let (negate, rest) = match token.strip_prefix('^') {
            Some(rest) => (true, rest),
            None => (false, token.as_str()),
        };
        if rest.is_empty() {
            return Err(format!("bad revision '{token}'").into());
        }
        if rest.contains("...") {
            return Err(format!(
                "symmetric difference ('{rest}') is not supported in revision; see git-log(1)"
            )
            .into());
        }
        if let Some(idx) = rest.find("..") {
            if negate {
                return Err(format!("bad revision '{token}'").into());
            }
            let from = &rest[..idx];
            let to = &rest[idx + 2..];
            let from = if from.is_empty() { "HEAD" } else { from };
            let to = if to.is_empty() { "HEAD" } else { to };
            terms.push(RevisionTerm {
                spec: to.to_string(),
                negate: false,
                origin: token.clone(),
            });
            terms.push(RevisionTerm {
                spec: from.to_string(),
                negate: true,
                origin: token.clone(),
            });
        } else {
            terms.push(RevisionTerm {
                spec: rest.to_string(),
                negate,
                origin: token.clone(),
            });
        }
    }
    Ok(terms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(spec: &str, negate: bool, origin: &str) -> RevisionTerm {
        RevisionTerm {
            spec: spec.to_string(),
            negate,
            origin: origin.to_string(),
        }
    }

    #[test]
    fn parse_revision_terms_single_spec() {
        let terms = parse_revision_terms(&["main".to_string()]).unwrap();
        assert_eq!(terms, vec![term("main", false, "main")]);
    }

    #[test]
    fn parse_revision_terms_excludes_with_caret() {
        let terms = parse_revision_terms(&["dev".to_string(), "^main".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![term("dev", false, "dev"), term("main", true, "^main")]
        );
    }

    #[test]
    fn parse_revision_terms_range_pushes_to_and_hides_from() {
        let terms = parse_revision_terms(&["main..dev".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![
                term("dev", false, "main..dev"),
                term("main", true, "main..dev"),
            ]
        );
    }

    #[test]
    fn parse_revision_terms_range_defaults_missing_side_to_head() {
        let terms = parse_revision_terms(&["main..".to_string()]).unwrap();
        assert_eq!(
            terms,
            vec![term("HEAD", false, "main.."), term("main", true, "main.."),]
        );
    }

    #[test]
    fn parse_revision_terms_rejects_bad_specs() {
        for (spec, expected) in [
            ("^main..dev", "bad revision"),
            ("main...dev", "symmetric difference"),
            ("^", "bad revision '^'"),
        ] {
            let err = parse_revision_terms(&[spec.to_string()]).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "{spec}: got {err}, want it to contain {expected:?}"
            );
        }
    }

    #[test]
    fn unresolved_revision_error_matches_git_wording() {
        for (spec, expected) in [
            (
                "nonexistent-ref",
                "ambiguous argument 'nonexistent-ref': unknown revision or path not in the working tree.",
            ),
            ("^nonexistent-ref", "bad revision '^nonexistent-ref'"),
            (
                "main..typo",
                "ambiguous argument 'main..typo': unknown revision or path not in the working tree.",
            ),
        ] {
            assert_eq!(unresolved_revision_error(spec), expected, "{spec}");
        }
    }
}

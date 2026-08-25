use crate::git::options::DecorateFormat;

/// `git branch -a` style short names; full refnames unchanged.
pub fn branch_display_name(refname: &str, format: DecorateFormat) -> String {
    match format {
        DecorateFormat::Full => refname.to_string(),
        DecorateFormat::Short => {
            if let Some(rest) = refname.strip_prefix("refs/heads/") {
                rest.to_string()
            } else if let Some(rest) = refname.strip_prefix("refs/remotes/") {
                format!("remotes/{rest}")
            } else {
                refname.to_string()
            }
        }
    }
}

pub fn tag_display_name(refname: &str, format: DecorateFormat) -> String {
    match format {
        DecorateFormat::Full => refname.to_string(),
        DecorateFormat::Short => refname
            .strip_prefix("refs/tags/")
            .unwrap_or(refname)
            .to_string(),
    }
}

/// Strip a trailing OpenPGP signature block from an annotated tag message.
pub fn tag_message_without_signature(message: &str) -> String {
    const BEGIN: &str = "\n-----BEGIN PGP SIGNATURE-----";
    if let Some(pos) = message.find(BEGIN) {
        message[..pos].trim_end().to_string()
    } else {
        message.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_display_short() {
        assert_eq!(
            branch_display_name("refs/heads/master", DecorateFormat::Short),
            "master"
        );
        assert_eq!(
            branch_display_name("refs/remotes/origin/main", DecorateFormat::Short),
            "remotes/origin/main"
        );
        assert_eq!(
            branch_display_name("refs/remotes/origin/HEAD", DecorateFormat::Short),
            "remotes/origin/HEAD"
        );
    }

    #[test]
    fn tag_display_short() {
        assert_eq!(
            tag_display_name("refs/tags/v1", DecorateFormat::Short),
            "v1"
        );
    }

    #[test]
    fn strips_pgp_signature_from_message() {
        let msg = "Release v1\n\n-----BEGIN PGP SIGNATURE-----\nabc\n-----END PGP SIGNATURE-----";
        assert_eq!(tag_message_without_signature(msg), "Release v1");
    }
}

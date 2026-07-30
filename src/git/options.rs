use std::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecorateFormat {
    Short,
    Full,
}

impl DecorateFormat {
    pub fn default() -> Self {
        Self::Short
    }

    pub fn parse(s: &str) -> Result<Self, Box<dyn Error>> {
        match s.to_lowercase().as_str() {
            "short" => Ok(Self::Short),
            "full" => Ok(Self::Full),
            "no" => Err("decorate='no' is not supported; omit the decorate column instead".into()),
            other => Err(format!(
                "unknown decorate format: '{other}' (expected 'short' or 'full')"
            )
            .into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiffMerges {
    Off,
    FirstParent,
}

impl DiffMerges {
    pub fn default() -> Self {
        Self::Off
    }

    pub fn parse(s: &str) -> Result<Self, Box<dyn Error>> {
        match s.to_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "first_parent" | "first-parent" => Ok(Self::FirstParent),
            other => Err(format!(
                "unknown diff_merges format: '{other}' (expected 'off', 'first_parent', or 'first-parent')"
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_decorate() {
        assert_eq!(
            DecorateFormat::parse("short").unwrap(),
            DecorateFormat::Short
        );
        assert_eq!(
            DecorateFormat::parse("SHORT").unwrap(),
            DecorateFormat::Short
        );

        assert_eq!(DecorateFormat::parse("full").unwrap(), DecorateFormat::Full);
        assert_eq!(DecorateFormat::parse("FULL").unwrap(), DecorateFormat::Full);

        assert!(DecorateFormat::parse("no")
            .unwrap_err()
            .to_string()
            .contains("not supported"));

        assert!(DecorateFormat::parse("unknown").is_err());

        assert_eq!(DecorateFormat::default(), DecorateFormat::Short);
    }

    #[test]
    fn parse_diff_merges() {
        assert_eq!(DiffMerges::parse("off").unwrap(), DiffMerges::Off);
        assert_eq!(DiffMerges::parse("OFF").unwrap(), DiffMerges::Off);

        assert_eq!(
            DiffMerges::parse("first_parent").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("FIRST_PARENT").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("first-parent").unwrap(),
            DiffMerges::FirstParent
        );
        assert_eq!(
            DiffMerges::parse("FIRST-PARENT").unwrap(),
            DiffMerges::FirstParent
        );

        assert!(DiffMerges::parse("unknown").is_err());

        assert_eq!(DiffMerges::default(), DiffMerges::Off);
    }
}

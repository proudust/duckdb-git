use std::error::Error;

/// Author/committer fields parsed like `git log` pretty formats (`%an`, `%ae`, `%at`).
///
/// Leading spaces in the name are preserved; trailing spaces before `<email>` are stripped.
/// libgit2's `git_signature__parse` trims both sides, so callers should prefer this over
/// backend signature APIs when matching git.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedIdent {
    pub name: Vec<u8>,
    pub email: Vec<u8>,
    pub seconds: i64,
}

/// Parse the first `author` or `committer` line from a commit header (bytes before `\n\n`).
pub fn parse_ident(header: &[u8], field: &[u8]) -> Result<ParsedIdent, Box<dyn Error>> {
    let prefix = {
        let mut p = field.to_vec();
        p.push(b' ');
        p
    };

    for line in header.split(|&b| b == b'\n') {
        if !line.starts_with(&prefix) {
            continue;
        }
        let rest = &line[prefix.len()..];
        return parse_ident_line(rest);
    }

    Err(format!(
        "commit header missing '{} ' line",
        String::from_utf8_lossy(field)
    )
    .into())
}

fn parse_ident_line(line: &[u8]) -> Result<ParsedIdent, Box<dyn Error>> {
    let email_start = line
        .iter()
        .rposition(|&b| b == b'<')
        .ok_or("malformed ident: missing '<'")?;
    let email_end = line
        .iter()
        .rposition(|&b| b == b'>')
        .ok_or("malformed ident: missing '>'")?;
    if email_end <= email_start {
        return Err("malformed ident: email brackets".into());
    }

    let mut name = line[..email_start].to_vec();
    while name.last().is_some_and(|b| b.is_ascii_whitespace()) {
        name.pop();
    }
    let email = line[email_start + 1..email_end].to_vec();

    let after = line.get(email_end + 1..).unwrap_or_default();
    let after = trim_ascii_start(after);
    let seconds = if after.is_empty() {
        0
    } else {
        let ts_end = after
            .iter()
            .position(|&b| b.is_ascii_whitespace())
            .unwrap_or(after.len());
        std::str::from_utf8(&after[..ts_end])
            .map_err(|_| "malformed ident: timestamp")?
            .parse::<i64>()
            .map_err(|_| "malformed ident: timestamp")?
    };

    Ok(ParsedIdent {
        name,
        email,
        seconds,
    })
}

fn trim_ascii_start(s: &[u8]) -> &[u8] {
    let i = s
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(s.len());
    &s[i..]
}

/// Header portion of a commit object (everything before the message's `\n\n`).
pub fn commit_header(data: &[u8]) -> &[u8] {
    data.windows(2)
        .position(|w| w == b"\n\n")
        .map(|i| &data[..i])
        .unwrap_or(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_leading_space_in_name() {
        let header =
            b"tree abc\nauthor  Pad Name  <pad@ex.com> 1700000000 +0000\ncommitter C <c@c> 1 +0000";
        let author = parse_ident(header, b"author").unwrap();
        assert_eq!(author.name, b" Pad Name");
        assert_eq!(author.email, b"pad@ex.com");
        assert_eq!(author.seconds, 1_700_000_000);
    }
}

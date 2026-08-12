/// git diff --numstat convention for gitlink (submodule) entries.
pub fn gitlink_numstat(status: &str) -> (i32, i32) {
    match status {
        "A" => (1, 0),
        "D" => (0, 1),
        "M" | "R" | "T" | "C" => (1, 1),
        _ => (0, 0),
    }
}

/// Match `git log --numstat` when a blob object is missing (`diff.c`: `unable to read %s`).
pub fn unable_to_read_object(oid: impl std::fmt::Display) -> String {
    format!("unable to read {oid}")
}

/// Which commit metadata fields must be retained at walk inspect for later emit.
///
/// Walk always needs committer seconds + parent OIDs; this only gates emit payload.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetaProjection {
    pub commit_id: bool,
    pub author: bool,
    pub committer: bool,
    pub message: bool,
    pub parents: bool,
}

impl MetaProjection {
    /// True when inspect must retain emit payload (not just walk seconds/parents).
    pub fn needs_emit_cache(self) -> bool {
        self.author || self.committer || self.message || self.parents
    }
}

#[derive(Clone, Debug)]
pub struct BranchRow {
    pub name: String,
    pub refname: String,
    pub is_head: bool,
    pub commit_id: String,
    pub subject: String,
    pub author: String,
    pub author_email: String,
    pub author_timestamp: i64,
    pub committer: String,
    pub committer_email: String,
    pub committer_timestamp: i64,
    pub upstream: Option<String>,
    pub upstream_ahead: Option<i64>,
    pub upstream_behind: Option<i64>,
    pub upstream_gone: Option<bool>,
    pub push: Option<String>,
    pub symref_target: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TagRow {
    pub name: String,
    pub refname: String,
    pub object_id: String,
    pub object_type: String,
    pub commit_id: Option<String>,
    pub is_annotated: bool,
    pub tagger: Option<String>,
    pub tagger_email: Option<String>,
    pub tagger_timestamp: Option<i64>,
    pub message: Option<String>,
    pub signature: Option<String>,
}

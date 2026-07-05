use super::GitBackend;
use crate::types::{CommitData, FileChange};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

pub struct GitCliBackend {
    repo_path: String,
}

impl GitCliBackend {
    pub fn new(repo_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(GitCliBackend {
            repo_path: repo_path.to_string(),
        })
    }

    fn git(&self) -> Command {
        let mut cmd = Command::new("git");
        cmd.args(["-C", &self.repo_path]);
        cmd
    }

    fn run_with_stdin(
        &self,
        args: &[&str],
        stdin_data: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut cmd = self.git();
        cmd.args(args);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take();
        let mut stdout = child.stdout.take().expect("stdout piped");
        let mut stderr = child.stderr.take().expect("stderr piped");
        let stdin_data = stdin_data.to_vec();

        let (stdout_buf, stderr_buf) = std::thread::scope(
            |s| -> Result<(Vec<u8>, Vec<u8>), std::io::Error> {
                let stdin_handle = s.spawn(move || -> Result<(), std::io::Error> {
                    if let Some(mut stdin) = stdin {
                        stdin.write_all(&stdin_data)?;
                    }
                    Ok(())
                });

                let stdout_handle = s.spawn(move || -> Result<Vec<u8>, std::io::Error> {
                    let mut buf = Vec::new();
                    stdout.read_to_end(&mut buf)?;
                    Ok(buf)
                });

                let stderr_handle = s.spawn(move || -> Result<Vec<u8>, std::io::Error> {
                    let mut buf = Vec::new();
                    stderr.read_to_end(&mut buf)?;
                    Ok(buf)
                });

                stdin_handle.join().unwrap()?;
                let stdout_buf = stdout_handle.join().unwrap()?;
                let stderr_buf = stderr_handle.join().unwrap()?;
                Ok((stdout_buf, stderr_buf))
            },
        )?;

        let status = child.wait()?;
        if !status.success() {
            let stderr = String::from_utf8_lossy(&stderr_buf);
            return Err(format!("git {} failed: {}", args[0], stderr).into());
        }
        Ok(stdout_buf)
    }

    fn fetch_metadata(
        &self,
        oids: &[String],
    ) -> Result<HashMap<String, CommitData>, Box<dyn std::error::Error>> {
        let stdin_data = oids.join("\n");
        let stdout = self.run_with_stdin(
            &[
                "log",
                "--no-walk",
                "--stdin",
                "--format=%x01%H%x00%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%P%x00%B%x00",
            ],
            stdin_data.as_bytes(),
        )?;

        let mut map = HashMap::new();
        for record in stdout.split(|&b| b == 0x01) {
            if record.is_empty() {
                continue;
            }

            let fields: Vec<&[u8]> = record.splitn(9, |&b| b == 0x00).collect();
            if fields.len() < 9 {
                continue;
            }

            let hash = std::str::from_utf8(fields[0])?.to_string();
            let author_name = fields[1].to_vec();
            let author_email = fields[2].to_vec();
            let author_timestamp: i64 = std::str::from_utf8(fields[3])?.parse()?;
            let committer_name = fields[4].to_vec();
            let committer_email = fields[5].to_vec();
            let committer_timestamp: i64 = std::str::from_utf8(fields[6])?.parse()?;

            let parents_str = std::str::from_utf8(fields[7])?;
            let parents: Vec<String> = if parents_str.is_empty() {
                Vec::new()
            } else {
                parents_str.split(' ').map(|s| s.to_string()).collect()
            };

            let mut message = fields[8].to_vec();
            // %B adds a trailing newline; the format adds \x00 after it.
            // Trim trailing \x00 and the newline added by git after the last record.
            while message.last() == Some(&b'\n') || message.last() == Some(&0x00) {
                message.pop();
            }
            // Restore the trailing newline that %B includes (matches libgit2 behavior)
            message.push(b'\n');

            map.insert(
                hash,
                CommitData {
                    author_name,
                    author_email,
                    author_timestamp,
                    committer_name,
                    committer_email,
                    committer_timestamp,
                    message,
                    parents,
                    file_changes: Vec::new(),
                },
            );
        }

        Ok(map)
    }

    fn fetch_file_changes(
        &self,
        oids: &[String],
        ignore_all_space: bool,
    ) -> Result<HashMap<String, Vec<FileChange>>, Box<dyn std::error::Error>> {
        let stdin_data = oids.join("\n");
        let base_args = vec![
            "log",
            "--no-walk",
            "--stdin",
            "--format=%x01%H",
            "--root",
            "-m",
            "--first-parent",
            "-z",
        ];

        // Pass 1: --raw for status, blob_id, path
        let mut raw_args = base_args.clone();
        raw_args.extend(["--raw", "--no-abbrev"]);
        let raw_stdout = self.run_with_stdin(&raw_args, stdin_data.as_bytes())?;
        let raw_map = Self::parse_raw_sections(&raw_stdout)?;

        // Pass 2: --numstat for add_lines, del_lines
        let mut numstat_args = base_args;
        numstat_args.push("--numstat");
        if ignore_all_space {
            numstat_args.push("-w");
        }
        let numstat_stdout = self.run_with_stdin(&numstat_args, stdin_data.as_bytes())?;
        let numstat_map = Self::parse_numstat_sections(&numstat_stdout)?;

        // Merge raw + numstat by path, then batch-fetch blob sizes
        let mut all_blob_ids: Vec<String> = Vec::new();
        let mut all_changes: HashMap<String, Vec<FileChange>> = HashMap::new();

        for (hash, raw_entries) in &raw_map {
            let numstats = numstat_map.get(hash.as_str());
            let mut file_changes = Vec::new();

            for (status, path, blob_id, is_submodule) in raw_entries {
                all_blob_ids.push(blob_id.clone());
                let (add_lines, del_lines) = if *is_submodule {
                    (0, 0)
                } else {
                    numstats
                        .and_then(|ns| ns.get(path))
                        .copied()
                        .unwrap_or((0, 0))
                };
                file_changes.push(FileChange {
                    path: path.clone(),
                    status: match status.as_str() {
                        "A" => "A",
                        "D" => "D",
                        "M" => "M",
                        "R" => "R",
                        "C" => "C",
                        "T" => "T",
                        _ => "U",
                    },
                    blob_id: blob_id.clone(),
                    file_size: 0,
                    add_lines,
                    del_lines,
                });
            }
            all_changes.insert(hash.clone(), file_changes);
        }

        if !all_blob_ids.is_empty() {
            let sizes = self.fetch_blob_sizes(&all_blob_ids)?;
            for changes in all_changes.values_mut() {
                for change in changes.iter_mut() {
                    if let Some(&size) = sizes.get(&change.blob_id) {
                        change.file_size = size;
                    }
                }
            }
        }

        Ok(all_changes)
    }

    /// Parse `--raw -z` output into per-commit entries: (status, path, blob_id, is_submodule)
    fn parse_raw_sections(
        stdout: &[u8],
    ) -> Result<HashMap<String, Vec<(String, String, String, bool)>>, Box<dyn std::error::Error>>
    {
        let mut map = HashMap::new();
        for section in stdout.split(|&b| b == 0x01) {
            if section.is_empty() {
                continue;
            }
            let nul_pos = match section.iter().position(|&b| b == 0x00) {
                Some(pos) => pos,
                None => continue,
            };
            let hash = std::str::from_utf8(&section[..nul_pos])?.to_string();
            let diff_data = &section[nul_pos + 1..];
            let segments: Vec<&[u8]> = diff_data.split(|&b| b == 0x00).collect();

            let mut entries = Vec::new();
            let mut i = 0;
            while i < segments.len() {
                let trimmed = segments[i].strip_prefix(&[b'\n']).unwrap_or(segments[i]);
                if !trimmed.starts_with(b":") {
                    i += 1;
                    continue;
                }
                // :old_mode new_mode old_sha new_sha status
                let header = std::str::from_utf8(trimmed)?;
                let parts: Vec<&str> = header.splitn(6, ' ').collect();
                if parts.len() < 5 {
                    i += 1;
                    continue;
                }
                let is_submodule = parts[1] == "160000" || parts[0] == ":160000";
                let status_char = &parts[4][..1];
                let is_rename = status_char == "R" || status_char == "C";

                if is_rename && i + 2 < segments.len() {
                    let path = String::from_utf8_lossy(segments[i + 2]).to_string();
                    entries.push((status_char.to_string(), path, parts[3].to_string(), is_submodule));
                    i += 3;
                } else if i + 1 < segments.len() {
                    let path = String::from_utf8_lossy(segments[i + 1]).to_string();
                    let blob_id = if status_char == "D" { parts[2] } else { parts[3] };
                    entries.push((status_char.to_string(), path, blob_id.to_string(), is_submodule));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            map.insert(hash, entries);
        }
        Ok(map)
    }

    /// Parse `--numstat -z` output into per-commit path -> (add_lines, del_lines)
    fn parse_numstat_sections(
        stdout: &[u8],
    ) -> Result<HashMap<String, HashMap<String, (i32, i32)>>, Box<dyn std::error::Error>> {
        let mut map = HashMap::new();
        for section in stdout.split(|&b| b == 0x01) {
            if section.is_empty() {
                continue;
            }
            let nul_pos = match section.iter().position(|&b| b == 0x00) {
                Some(pos) => pos,
                None => continue,
            };
            let hash = std::str::from_utf8(&section[..nul_pos])?.to_string();
            let diff_data = &section[nul_pos + 1..];
            let segments: Vec<&[u8]> = diff_data.split(|&b| b == 0x00).collect();

            let mut entries = HashMap::new();
            let mut i = 0;
            while i < segments.len() {
                let trimmed = segments[i].strip_prefix(&[b'\n']).unwrap_or(segments[i]);
                if trimmed.is_empty() {
                    i += 1;
                    continue;
                }
                let s = std::str::from_utf8(trimmed)?;
                let tab_parts: Vec<&str> = s.splitn(3, '\t').collect();
                if tab_parts.len() < 2 {
                    i += 1;
                    continue;
                }
                let add: i32 = tab_parts[0].parse().unwrap_or(0);
                let del: i32 = tab_parts[1].parse().unwrap_or(0);
                let path_field = tab_parts.get(2).copied().unwrap_or("");

                let path = if path_field.is_empty() && i + 2 < segments.len() {
                    // Rename/copy: path is split across next two NUL-separated segments
                    String::from_utf8_lossy(segments[i + 2]).to_string()
                } else {
                    path_field.to_string()
                };

                // Keep first occurrence per path (matches prior Vec::find behavior).
                entries.entry(path).or_insert((add, del));

                if path_field.is_empty() && i + 2 < segments.len() {
                    i += 3;
                } else {
                    i += 1;
                }
            }
            map.insert(hash, entries);
        }
        Ok(map)
    }

    fn fetch_blob_sizes(
        &self,
        blob_ids: &[String],
    ) -> Result<HashMap<String, i64>, Box<dyn std::error::Error>> {
        let unique_ids: Vec<&String> = {
            let mut seen = std::collections::HashSet::new();
            blob_ids.iter().filter(|id| seen.insert(id.as_str())).collect()
        };

        let null_oid = "0000000000000000000000000000000000000000";
        let stdin_data = unique_ids
            .iter()
            .filter(|id| id.as_str() != null_oid)
            .cloned()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");

        if stdin_data.is_empty() {
            return Ok(HashMap::new());
        }

        let stdout = self.run_with_stdin(
            &["cat-file", "--batch-check=%(objectname) %(objectsize)"],
            stdin_data.as_bytes(),
        )?;

        let mut sizes = HashMap::new();
        let output_str = std::str::from_utf8(&stdout)?;
        for line in output_str.lines() {
            let parts: Vec<&str> = line.splitn(2, ' ').collect();
            if parts.len() == 2 {
                if let Ok(size) = parts[1].parse::<i64>() {
                    sizes.insert(parts[0].to_string(), size);
                }
            }
        }

        Ok(sizes)
    }
}

impl GitBackend for GitCliBackend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut cmd = self.git();
        cmd.arg("rev-list");
        if let Some(count) = max_count {
            cmd.arg(format!("--max-count={}", count));
        }
        cmd.arg(revision.unwrap_or("HEAD"));

        let output = cmd.output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("git rev-list failed: {}", stderr).into());
        }

        let stdout = std::str::from_utf8(&output.stdout)?;
        Ok(stdout.lines().map(|s| s.to_string()).collect())
    }

    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
        let mut cmd = self.git();
        cmd.args([
            "for-each-ref",
            "--format=%(objectname) %(*objectname) %(refname:short)",
        ]);

        let output = cmd.output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("git for-each-ref failed: {}", stderr).into());
        }

        let mut refs_map: HashMap<String, Vec<String>> = HashMap::new();
        let stdout = std::str::from_utf8(&output.stdout)?;
        for line in stdout.lines() {
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            if parts.len() < 3 {
                continue;
            }
            let oid = if parts[1].is_empty() {
                parts[0]
            } else {
                parts[1]
            };
            let name = parts[2];
            if !name.is_empty() {
                refs_map
                    .entry(oid.to_string())
                    .or_default()
                    .push(name.to_string());
            }
        }
        Ok(refs_map)
    }

    fn get_commits(
        &mut self,
        oids: &[String],
        ignore_all_space: bool,
        need_file_changes: bool,
    ) -> Result<Vec<CommitData>, Box<dyn std::error::Error>> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }

        let mut metadata = self.fetch_metadata(oids)?;

        if need_file_changes {
            let file_changes = self.fetch_file_changes(oids, ignore_all_space)?;
            for (hash, changes) in file_changes {
                if let Some(commit) = metadata.get_mut(&hash) {
                    commit.file_changes = changes;
                }
            }
        }

        oids.iter()
            .map(|oid| {
                metadata
                    .remove(oid)
                    .ok_or_else(|| format!("commit {} not found in git log output", oid).into())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn skip_file_changes_returns_empty() {
        let mut backend = GitCliBackend::new(".").unwrap();
        let commits = backend
            .get_commits(&[SECOND_COMMIT.to_string()], false, false)
            .unwrap();
        assert!(commits[0].file_changes.is_empty());
    }

    #[test]
    fn no_skip_returns_file_changes() {
        let mut backend = GitCliBackend::new(".").unwrap();
        let commits = backend
            .get_commits(&[SECOND_COMMIT.to_string()], false, true)
            .unwrap();
        assert!(!commits[0].file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let backend = GitCliBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        let names = refs
            .get(TAGGED_COMMIT)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "v0.1.1"));
    }

    #[test]
    fn get_refs_returns_empty_for_commit_without_refs() {
        let backend = GitCliBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        assert!(!refs.contains_key(SECOND_COMMIT));
    }
}

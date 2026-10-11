//! Per-source bearer credentials with overlapping rotation and last-good reloads.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::RwLock,
};

use secrecy::{ExposeSecret, SecretString};
use subtle::{Choice, ConstantTimeEq};

const MAX_TOKEN_FILE_BYTES: u64 = 1024;

/// A malformed or unreadable credential file.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CredentialError {
    #[error("cannot read credential file: {0}")]
    Read(#[from] std::io::Error),
    #[error("credential file is larger than {MAX_TOKEN_FILE_BYTES} bytes")]
    TooLarge,
    #[error("credential must be one 32 to 256 character printable ASCII token without spaces")]
    Malformed,
    #[error("credential is also active for source {0:?}; each source needs its own token")]
    Shared(String),
}

/// Reads and validates one bearer token file.
pub(crate) fn read_token(path: &Path) -> Result<SecretString, CredentialError> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAX_TOKEN_FILE_BYTES {
        return Err(CredentialError::TooLarge);
    }
    let token = text.trim();
    if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(CredentialError::Malformed);
    }
    Ok(SecretString::from(token))
}

/// Result of polling a source's credential files.
#[derive(Debug, Default)]
pub(crate) struct ReloadReport {
    /// Files whose current content is valid and active.
    pub valid: usize,
    /// Files whose update was rejected; their last-good token stays active.
    pub failures: Vec<(PathBuf, CredentialError)>,
}

/// A configured source's credential files.
struct SourceFiles {
    source: String,
    files: Vec<PathBuf>,
}

/// The accepted tokens of every source, one slot per configured file.
///
/// Every slot of the addressed source is compared on each request, so two files
/// can overlap during rotation. A file that becomes unreadable or malformed keeps
/// its last valid token until a valid replacement appears or a rollout removes
/// the file. No token is ever active for two sources: a replacement that would
/// be shared is rejected like an invalid file.
pub(crate) struct Credentials {
    sources: Vec<SourceFiles>,
    /// Active tokens, indexed like `sources` and their files.
    slots: RwLock<Vec<Vec<SecretString>>>,
}

impl Credentials {
    /// Loads every file; all must be valid and no token may serve two sources.
    pub(crate) fn load(
        sources: impl IntoIterator<Item = (String, Vec<PathBuf>)>,
    ) -> Result<Self, (PathBuf, CredentialError)> {
        let sources: Vec<SourceFiles> = sources
            .into_iter()
            .map(|(source, files)| SourceFiles { source, files })
            .collect();
        let mut owners: BTreeMap<String, &str> = BTreeMap::new();
        let mut slots = Vec::with_capacity(sources.len());
        for entry in &sources {
            let mut tokens = Vec::with_capacity(entry.files.len());
            for path in &entry.files {
                let token = read_token(path).map_err(|error| (path.clone(), error))?;
                match owners.insert(token.expose_secret().to_owned(), &entry.source) {
                    Some(owner) if owner != entry.source => {
                        return Err((path.clone(), CredentialError::Shared(owner.to_owned())));
                    }
                    _ => {}
                }
                tokens.push(token);
            }
            slots.push(tokens);
        }
        Ok(Self {
            sources,
            slots: RwLock::new(slots),
        })
    }

    /// Whether `source` is configured.
    pub(crate) fn knows(&self, source: &str) -> bool {
        self.position(source).is_some()
    }

    fn position(&self, source: &str) -> Option<usize> {
        self.sources.iter().position(|entry| entry.source == source)
    }

    /// Checks a presented token against every slot of `source` without early exit.
    pub(crate) fn verify(&self, source: &str, presented: &[u8]) -> bool {
        let (Some(index), Ok(slots)) = (self.position(source), self.slots.read()) else {
            return false;
        };
        let matched = slots[index].iter().fold(Choice::from(0), |matched, token| {
            matched | token.expose_secret().as_bytes().ct_eq(presented)
        });
        bool::from(matched)
    }

    /// Rereads every source's files and applies the valid, unambiguous updates.
    ///
    /// All sources are evaluated together and swapped under one lock, so a token
    /// moving between sources is never briefly accepted by both. This performs
    /// blocking file I/O and belongs on a blocking thread.
    pub(crate) fn reload(&self) -> BTreeMap<String, ReloadReport> {
        let Ok(current) = self.slots.read().map(|slots| slots.clone()) else {
            return BTreeMap::new();
        };
        let mut reports: Vec<ReloadReport> = self
            .sources
            .iter()
            .map(|_| ReloadReport::default())
            .collect();
        // Candidate set: a valid file's new token, otherwise its last-good token.
        let mut candidate = current.clone();
        let mut replaced: Vec<Vec<bool>> = Vec::with_capacity(self.sources.len());
        for (index, entry) in self.sources.iter().enumerate() {
            let mut changed = vec![false; entry.files.len()];
            for (slot, path) in entry.files.iter().enumerate() {
                match read_token(path) {
                    Ok(token) => {
                        changed[slot] =
                            token.expose_secret() != current[index][slot].expose_secret();
                        candidate[index][slot] = token;
                    }
                    Err(error) => reports[index].failures.push((path.clone(), error)),
                }
            }
            replaced.push(changed);
        }
        // Last-good tokens never collide, so reverting colliding replacements
        // always converges to an unambiguous set.
        while let Some((index, slot, owner)) = self.shared_replacement(&candidate, &replaced) {
            candidate[index][slot] = current[index][slot].clone();
            replaced[index][slot] = false;
            let path = self.sources[index].files[slot].clone();
            reports[index]
                .failures
                .push((path, CredentialError::Shared(owner)));
        }
        for (index, report) in reports.iter_mut().enumerate() {
            report.valid = self.sources[index].files.len() - report.failures.len();
        }
        if let Ok(mut slots) = self.slots.write() {
            *slots = candidate;
        }
        self.sources
            .iter()
            .map(|entry| entry.source.clone())
            .zip(reports)
            .collect()
    }

    /// Finds a replaced slot whose token is also a candidate of another source.
    fn shared_replacement(
        &self,
        candidate: &[Vec<SecretString>],
        replaced: &[Vec<bool>],
    ) -> Option<(usize, usize, String)> {
        let mut holders: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
        for (index, tokens) in candidate.iter().enumerate() {
            for token in tokens {
                holders
                    .entry(token.expose_secret())
                    .or_default()
                    .insert(index);
            }
        }
        for (index, tokens) in candidate.iter().enumerate() {
            for (slot, token) in tokens.iter().enumerate() {
                if !replaced[index][slot] {
                    continue;
                }
                let other = holders[token.expose_secret()]
                    .iter()
                    .find(|&&holder| holder != index);
                if let Some(&other) = other {
                    return Some((index, slot, self.sources[other].source.clone()));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "synthetic-old-token-not-a-live-secret-0123456789";
    const NEW: &str = "synthetic-new-token-not-a-live-secret-9876543210";
    const OTHER: &str = "synthetic-other-token-not-a-live-secret-5555555";

    fn one(path: &Path) -> Credentials {
        Credentials::load([("a".to_owned(), vec![path.to_path_buf()])]).unwrap()
    }

    #[test]
    fn overlapping_files_accept_both_tokens_during_rotation() {
        let directory = tempfile::tempdir().unwrap();
        let (old, new) = (directory.path().join("old"), directory.path().join("new"));
        std::fs::write(&old, OLD).unwrap();
        std::fs::write(&new, format!("{NEW}\n")).unwrap();
        let credentials = Credentials::load([("a".to_owned(), vec![old, new])]).unwrap();
        assert!(credentials.verify("a", OLD.as_bytes()) && credentials.verify("a", NEW.as_bytes()));
    }

    #[test]
    fn invalid_update_keeps_last_good_token_and_reports_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, OLD).unwrap();
        let credentials = one(&path);
        std::fs::write(&path, "short").unwrap();
        let report = credentials.reload().remove("a").unwrap();
        assert_eq!(
            (
                report.valid,
                report.failures.len(),
                credentials.verify("a", OLD.as_bytes())
            ),
            (0, 1, true)
        );
    }

    #[test]
    fn valid_update_replaces_the_previous_token() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, OLD).unwrap();
        let credentials = one(&path);
        std::fs::write(&path, NEW).unwrap();
        credentials.reload();
        assert_eq!(
            (
                credentials.verify("a", OLD.as_bytes()),
                credentials.verify("a", NEW.as_bytes())
            ),
            (false, true)
        );
    }

    fn two_sources(directory: &Path) -> (PathBuf, PathBuf, Credentials) {
        let (a, b) = (directory.join("a"), directory.join("b"));
        std::fs::write(&a, OLD).unwrap();
        std::fs::write(&b, NEW).unwrap();
        let credentials = Credentials::load([
            ("a".to_owned(), vec![a.clone()]),
            ("b".to_owned(), vec![b.clone()]),
        ])
        .unwrap();
        (a, b, credentials)
    }

    #[test]
    fn reload_cannot_make_two_sources_share_a_token() {
        let directory = tempfile::tempdir().unwrap();
        let (_a, b, credentials) = two_sources(directory.path());
        std::fs::write(&b, OLD).unwrap();
        let report = credentials.reload().remove("b").unwrap();
        assert!(
            !credentials.verify("b", OLD.as_bytes())
                && credentials.verify("b", NEW.as_bytes())
                && credentials.verify("a", OLD.as_bytes())
                && matches!(report.failures[..], [(_, CredentialError::Shared(ref owner))] if owner == "a"),
            "{report:?}"
        );
    }

    #[test]
    fn retained_last_good_token_still_blocks_another_sources_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let (a, b, credentials) = two_sources(directory.path());
        std::fs::write(&a, "broken").unwrap();
        std::fs::write(&b, OLD).unwrap();
        credentials.reload();
        assert_eq!(
            (
                credentials.verify("a", OLD.as_bytes()),
                credentials.verify("b", OLD.as_bytes()),
                credentials.verify("b", NEW.as_bytes())
            ),
            (true, false, true)
        );
    }

    #[test]
    fn token_moving_between_sources_in_one_reload_has_a_single_owner() {
        let directory = tempfile::tempdir().unwrap();
        let (a, b, credentials) = two_sources(directory.path());
        std::fs::write(&a, OTHER).unwrap();
        std::fs::write(&b, OLD).unwrap();
        credentials.reload();
        assert_eq!(
            (
                credentials.verify("a", OLD.as_bytes()),
                credentials.verify("a", OTHER.as_bytes()),
                credentials.verify("b", OLD.as_bytes())
            ),
            (false, true, true)
        );
    }

    #[test]
    fn startup_rejects_a_token_shared_by_two_sources() {
        let directory = tempfile::tempdir().unwrap();
        let (a, b) = (directory.path().join("a"), directory.path().join("b"));
        std::fs::write(&a, OLD).unwrap();
        std::fs::write(&b, OLD).unwrap();
        let loaded = Credentials::load([("a".to_owned(), vec![a]), ("b".to_owned(), vec![b])]);
        assert!(matches!(loaded, Err((_, CredentialError::Shared(_)))));
    }

    #[test]
    fn projected_secret_symlink_swap_is_picked_up() {
        // Kubernetes projected volumes swap a `..data` symlink atomically.
        let directory = tempfile::tempdir().unwrap();
        let (first, second) = (directory.path().join("v1"), directory.path().join("v2"));
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        std::fs::write(first.join("token"), OLD).unwrap();
        std::fs::write(second.join("token"), NEW).unwrap();
        let data = directory.path().join("..data");
        std::os::unix::fs::symlink(&first, &data).unwrap();
        let link = directory.path().join("token");
        std::os::unix::fs::symlink("..data/token", &link).unwrap();
        let credentials = one(&link);
        let swap = directory.path().join("..data_tmp");
        std::os::unix::fs::symlink(&second, &swap).unwrap();
        std::fs::rename(&swap, &data).unwrap();
        credentials.reload();
        assert!(
            credentials.verify("a", NEW.as_bytes()) && !credentials.verify("a", OLD.as_bytes())
        );
    }
}

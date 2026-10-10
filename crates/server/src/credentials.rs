//! Per-source bearer credentials with overlapping rotation and last-good reloads.

use std::{
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
    /// Files whose current content is valid.
    pub valid: usize,
    /// Files whose update was rejected; their last-good token stays active.
    pub failures: Vec<(PathBuf, CredentialError)>,
}

/// The accepted tokens of one source, one slot per configured file.
///
/// Every slot is compared on each request, so two files can overlap during
/// rotation. A file that becomes unreadable or malformed keeps its last valid
/// token until a valid replacement appears or a rollout removes the file.
pub(crate) struct CredentialSet {
    files: Vec<PathBuf>,
    slots: RwLock<Vec<SecretString>>,
}

impl CredentialSet {
    /// Loads every configured file; all must be valid at startup.
    pub(crate) fn load(files: Vec<PathBuf>) -> Result<Self, (PathBuf, CredentialError)> {
        let slots = files
            .iter()
            .map(|path| read_token(path).map_err(|error| (path.clone(), error)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            files,
            slots: RwLock::new(slots),
        })
    }

    /// Checks a presented token against every slot without early exit.
    pub(crate) fn verify(&self, presented: &[u8]) -> bool {
        let Ok(slots) = self.slots.read() else {
            return false;
        };
        let matched = slots.iter().fold(Choice::from(0), |matched, token| {
            matched | token.expose_secret().as_bytes().ct_eq(presented)
        });
        bool::from(matched)
    }

    /// Rereads the files, replacing only slots whose new content is valid.
    ///
    /// This performs blocking file I/O and belongs on a blocking thread.
    pub(crate) fn reload(&self) -> ReloadReport {
        let mut report = ReloadReport::default();
        let fresh: Vec<Option<SecretString>> = self
            .files
            .iter()
            .map(|path| match read_token(path) {
                Ok(token) => {
                    report.valid += 1;
                    Some(token)
                }
                Err(error) => {
                    report.failures.push((path.clone(), error));
                    None
                }
            })
            .collect();
        if let Ok(mut slots) = self.slots.write() {
            for (slot, token) in slots.iter_mut().zip(fresh) {
                if let Some(token) = token {
                    *slot = token;
                }
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "synthetic-old-token-not-a-live-secret-0123456789";
    const NEW: &str = "synthetic-new-token-not-a-live-secret-9876543210";

    #[test]
    fn overlapping_files_accept_both_tokens_during_rotation() {
        let directory = tempfile::tempdir().unwrap();
        let (old, new) = (directory.path().join("old"), directory.path().join("new"));
        std::fs::write(&old, OLD).unwrap();
        std::fs::write(&new, format!("{NEW}\n")).unwrap();
        let set = CredentialSet::load(vec![old, new]).unwrap();
        assert!(set.verify(OLD.as_bytes()) && set.verify(NEW.as_bytes()));
    }

    #[test]
    fn invalid_update_keeps_last_good_token_and_reports_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, OLD).unwrap();
        let set = CredentialSet::load(vec![path.clone()]).unwrap();
        std::fs::write(&path, "short").unwrap();
        let report = set.reload();
        assert_eq!(
            (
                report.valid,
                report.failures.len(),
                set.verify(OLD.as_bytes())
            ),
            (0, 1, true)
        );
    }

    #[test]
    fn valid_update_replaces_the_previous_token() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, OLD).unwrap();
        let set = CredentialSet::load(vec![path.clone()]).unwrap();
        std::fs::write(&path, NEW).unwrap();
        set.reload();
        assert_eq!(
            (set.verify(OLD.as_bytes()), set.verify(NEW.as_bytes())),
            (false, true)
        );
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
        let set = CredentialSet::load(vec![link]).unwrap();
        let swap = directory.path().join("..data_tmp");
        std::os::unix::fs::symlink(&second, &swap).unwrap();
        std::fs::rename(&swap, &data).unwrap();
        set.reload();
        assert!(set.verify(NEW.as_bytes()) && !set.verify(OLD.as_bytes()));
    }
}

//! Configured, deterministic severity policy.

use std::collections::{BTreeMap, BTreeSet};

use crate::Action;

/// Maximum number of distinct configured severity values.
pub const MAX_SEVERITIES: usize = 32;

/// Why a policy proposal was chosen. Values are stable and bounded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyReason {
    /// A trusted severity on the critical floor; bypasses assessment.
    TrustedCriticalFloor,
    /// A trusted severity mapped explicitly by configuration.
    ConfiguredSeverity,
    /// Missing or unmapped severity from a trusted source.
    UnknownSeverityFallback,
    /// The source is not trusted to supply severity labels.
    UntrustedSeverityFallback,
}

impl PolicyReason {
    /// Stable serialized name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrustedCriticalFloor => "trusted_critical_floor",
            Self::ConfiguredSeverity => "configured_severity",
            Self::UnknownSeverityFallback => "unknown_severity_conservative_fallback",
            Self::UntrustedSeverityFallback => "untrusted_severity_fallback",
        }
    }
}

/// A notification proposal for one firing observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Proposal {
    /// Proposed action before lifecycle rules apply.
    pub action: Action,
    /// Why the action was proposed.
    pub reason: PolicyReason,
    /// The proposal comes from a trusted floor and must not wait for assessment.
    pub floor: bool,
}

/// Rejected policy configuration.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyError {
    /// Too many severity mappings.
    #[error("at most {MAX_SEVERITIES} severity mappings are supported")]
    TooManySeverities,
    /// A severity value is empty or overlong.
    #[error("severity {0:?} must be 1 to 64 printable ASCII characters")]
    InvalidSeverity(String),
    /// A floor names a severity without a mapping.
    #[error("critical floor severity {0:?} has no [policy.severity] mapping")]
    UnmappedFloor(String),
    /// A floor would record without notifying.
    #[error("critical floor severity {0:?} must map to quiet_post or ping, not record")]
    SilentFloor(String),
}

/// Severity policy: explicit mappings, an explicit fallback and trusted floors.
///
/// ```
/// use criers_core::{Action, Policy};
/// let policy = Policy::default();
/// let proposal = policy.propose(Some("critical"), true);
/// assert_eq!((proposal.action, proposal.floor), (Action::Ping, true));
/// assert_eq!(policy.propose(Some("critical"), false).floor, false);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Policy {
    severities: BTreeMap<String, Action>,
    unknown: Action,
    floors: BTreeSet<String>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            severities: BTreeMap::from([
                ("critical".to_owned(), Action::Ping),
                ("warning".to_owned(), Action::QuietPost),
                ("info".to_owned(), Action::Record),
            ]),
            unknown: Action::Ping,
            floors: BTreeSet::from(["critical".to_owned()]),
        }
    }
}

impl Policy {
    /// Builds a validated policy.
    ///
    /// # Errors
    /// Rejects malformed severities, floors without a notifying mapping, and
    /// oversized tables.
    pub fn new(
        severities: BTreeMap<String, Action>,
        unknown: Action,
        floors: BTreeSet<String>,
    ) -> Result<Self, PolicyError> {
        if severities.len() > MAX_SEVERITIES {
            return Err(PolicyError::TooManySeverities);
        }
        if let Some(invalid) = severities.keys().find(|severity| {
            severity.is_empty()
                || severity.len() > 64
                || !severity.bytes().all(|byte| byte.is_ascii_graphic())
        }) {
            return Err(PolicyError::InvalidSeverity(invalid.clone()));
        }
        for floor in &floors {
            match severities.get(floor) {
                None => return Err(PolicyError::UnmappedFloor(floor.clone())),
                Some(Action::Record) => return Err(PolicyError::SilentFloor(floor.clone())),
                Some(Action::QuietPost | Action::Ping) => {}
            }
        }
        Ok(Self {
            severities,
            unknown,
            floors,
        })
    }

    /// Proposes an action for a firing observation's source severity.
    ///
    /// An untrusted source's severity is ignored and never reaches a floor.
    pub fn propose(&self, severity: Option<&str>, trusted: bool) -> Proposal {
        if !trusted {
            return Proposal {
                action: self.unknown,
                reason: PolicyReason::UntrustedSeverityFallback,
                floor: false,
            };
        }
        match severity.and_then(|value| self.severities.get_key_value(value)) {
            Some((value, &action)) if self.floors.contains(value) => Proposal {
                action,
                reason: PolicyReason::TrustedCriticalFloor,
                floor: true,
            },
            Some((_, &action)) => Proposal {
                action,
                reason: PolicyReason::ConfiguredSeverity,
                floor: false,
            },
            None => Proposal {
                action: self.unknown,
                reason: PolicyReason::UnknownSeverityFallback,
                floor: false,
            },
        }
    }

    /// Whether any outcome of this policy can request a mention.
    pub fn can_ping(&self) -> bool {
        self.unknown == Action::Ping || self.severities.values().any(|&a| a == Action::Ping)
    }

    /// Canonical text form used to fingerprint the policy for decision provenance.
    ///
    /// Severity names may contain any printable character, so each one is
    /// length-prefixed; no name can be mistaken for another field or entry.
    /// Entries follow the severity map's sorted order.
    pub fn canonical(&self) -> String {
        use std::fmt::Write;
        let mut text = format!("v2;unknown={};", self.unknown.as_str());
        for (severity, action) in &self.severities {
            let floor = u8::from(self.floors.contains(severity));
            // Writing to a String cannot fail.
            let _ = write!(
                text,
                "{}:{severity}={};floor={floor};",
                severity.len(),
                action.as_str()
            );
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_form_distinguishes_a_floor_from_a_severity_ending_in_its_marker() {
        let floor = Policy::new(
            BTreeMap::from([("critical".to_owned(), Action::Ping)]),
            Action::Ping,
            BTreeSet::from(["critical".to_owned()]),
        )
        .unwrap();
        let lookalike = Policy::new(
            BTreeMap::from([("critical!".to_owned(), Action::Ping)]),
            Action::Ping,
            BTreeSet::new(),
        )
        .unwrap();
        assert_ne!(floor.canonical(), lookalike.canonical());
    }

    #[test]
    fn canonical_form_distinguishes_delimiters_inside_severity_names() {
        let joined = Policy::new(
            BTreeMap::from([("a=ping;b".to_owned(), Action::Record)]),
            Action::Ping,
            BTreeSet::new(),
        )
        .unwrap();
        let split = Policy::new(
            BTreeMap::from([
                ("a".to_owned(), Action::Ping),
                ("b".to_owned(), Action::Record),
            ]),
            Action::Ping,
            BTreeSet::new(),
        )
        .unwrap();
        assert_ne!(joined.canonical(), split.canonical());
    }

    #[test]
    fn canonical_form_is_stable_for_equal_policies() {
        assert_eq!(Policy::default().canonical(), Policy::default().canonical());
    }
}

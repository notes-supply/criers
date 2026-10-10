//! Bounded Alertmanager version 4 messages and conservative default policy.

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

/// Normalized supported fields from an Alertmanager webhook.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Webhook {
    /// Wire version; only `4` is admitted.
    pub version: String,
    /// Alertmanager grouping identity; not an occurrence identifier.
    pub group_key: String,
    /// Group status; individual alert status governs lifecycle changes.
    pub status: Status,
    /// Upstream receiver name.
    pub receiver: String,
    /// Alerts omitted by Alertmanager, signaling a coverage gap.
    #[serde(default)]
    pub truncated_alerts: u32,
    /// Individual alert events; absence never implies resolution.
    pub alerts: Vec<Alert>,
}

/// One firing or resolved occurrence observation from the trusted source.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    /// Individual lifecycle status.
    pub status: Status,
    /// Source-provided stable 16-character hexadecimal identity.
    pub fingerprint: String,
    /// Detector labels, including alertname and source severity.
    pub labels: BTreeMap<String, String>,
    /// Untrusted diagnostic text; never executable instructions.
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    /// Occurrence start, included in durable identity.
    pub starts_at: DateTime<Utc>,
    /// Resolution time or estimated expiry; firing expiry does not imply resolution.
    pub ends_at: DateTime<Utc>,
}

/// The upstream lifecycle status of an observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Detector currently reports the occurrence as firing.
    Firing,
    /// Detector reports the occurrence resolved.
    Resolved,
}

/// A policy proposal, separate from assessment and actual delivery.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Persist the observation without notifying anyone.
    Record,
    /// Post a notification without a mention.
    QuietPost,
    /// Notify with an explicitly configured recipient mention.
    Ping,
}

impl Action {
    /// Stable serialized name for stored policy decisions.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::QuietPost => "quiet_post",
            Self::Ping => "ping",
        }
    }

    /// Monotonic priority for notification escalation within an occurrence.
    pub fn rank(self) -> u8 {
        match self {
            Self::Record => 0,
            Self::QuietPost => 1,
            Self::Ping => 2,
        }
    }
}

impl Alert {
    /// Canonical UTC, nanosecond-precision occurrence key.
    pub fn start_key(&self) -> String {
        self.starts_at.to_rfc3339_opts(SecondsFormat::Nanos, true)
    }

    /// Selects the conservative default action without consulting a model.
    ///
    /// Unknown severity cannot quietly downgrade a potentially urgent alert.
    /// This requires a source explicitly trusted to supply severity labels.
    ///
    /// ```
    /// use alert_router::model::{Action, Webhook};
    /// let webhook: Webhook = serde_json::from_str(include_str!("../examples/critical.json"))?;
    /// webhook.validate()?;
    /// assert_eq!(webhook.alerts[0].policy().0, Action::Ping);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn policy(&self) -> (Action, &'static str) {
        match self.labels.get("severity").map(String::as_str) {
            Some("critical") => (Action::Ping, "trusted_critical_floor"),
            Some("warning") => (Action::QuietPost, "warning_fallback"),
            Some("info") => (Action::Record, "informational"),
            _ => (Action::Ping, "unknown_severity_conservative_fallback"),
        }
    }
}

impl Webhook {
    /// Validates supported fields and admission bounds without truncation.
    ///
    /// # Errors
    /// Rejects unsupported versions, empty or oversized batches, malformed
    /// identities, invalid occurrence times, and overlong diagnostic fields.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != "4" {
            return Err("only Alertmanager webhook version 4 is supported");
        }
        if self.alerts.is_empty() || self.alerts.len() > 512 {
            return Err("batch must contain 1 to 512 alerts; no alerts are silently dropped");
        }
        if self.group_key.len() > 4096 || self.receiver.len() > 256 {
            return Err("group metadata exceeds bounds");
        }
        for alert in &self.alerts {
            if alert.fingerprint.len() != 16
                || !alert
                    .fingerprint
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("fingerprint must contain exactly 16 hexadecimal characters");
            }
            if !(1970..=9999).contains(&chrono::Datelike::year(&alert.starts_at)) {
                return Err("startsAt must have a supported nonzero occurrence timestamp");
            }
            if alert.status == Status::Resolved && alert.ends_at < alert.starts_at {
                return Err("resolved endsAt cannot precede startsAt");
            }
            if alert.labels.get("alertname").is_none_or(String::is_empty) {
                return Err("alertname is required");
            }
            for fields in [&alert.labels, &alert.annotations] {
                if fields.len() > 64
                    || fields
                        .iter()
                        .any(|(key, value)| key.len() > 256 || value.len() > 8192)
                {
                    return Err("alert fields exceed bounds");
                }
            }
        }
        Ok(())
    }
}

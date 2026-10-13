//! Pure occurrence lifecycle rules: deduplication, escalation and recovery.

use crate::{Action, Proposal, Status};

/// Durable occurrence state before an observation is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Prior {
    /// The occurrence was just created by this observation.
    New,
    /// Firing, with the highest action already taken for the target destination:
    /// the highest recorded intent there, or `Digest` if the occurrence is already
    /// listed for the summary, otherwise `Record`.
    Firing(Action),
    /// Already resolved.
    Resolved,
    /// Replaced by a newer occurrence of the same fingerprint.
    Superseded,
}

/// Whether anyone may already have been notified about the occurrence.
///
/// Only real delivery attempts count. A shadow or unattempted intent is not
/// a notification, so it can never justify a recovery message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Notified {
    /// No live delivery was attempted or succeeded.
    No,
    /// A live delivery succeeded, may have succeeded, or is still in progress.
    Possibly,
}

/// Why a lifecycle decision was made. Values are stable and bounded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    /// A new or escalated firing proposal; the policy reason explains the action.
    Policy(crate::PolicyReason),
    /// A newer occurrence with the same fingerprint already exists.
    SupersededOccurrence,
    /// The occurrence was resolved earlier.
    AlreadyResolved,
    /// A resolution for an occurrence somebody may have been notified about.
    Recovery,
    /// A resolution for an occurrence nobody was notified about.
    ResolutionWithoutNotification,
    /// A firing observation arrived after the resolution.
    LateFiringAfterResolution,
    /// A repeat or lower-priority firing observation.
    UnchangedOrLowerPriority,
}

impl Reason {
    /// Stable serialized name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Policy(reason) => reason.as_str(),
            Self::SupersededOccurrence => "superseded_occurrence",
            Self::AlreadyResolved => "already_resolved",
            Self::Recovery => "recovery",
            Self::ResolutionWithoutNotification => "resolution_without_notification",
            Self::LateFiringAfterResolution => "late_firing_after_resolution",
            Self::UnchangedOrLowerPriority => "unchanged_or_lower_priority",
        }
    }
}

/// A durable notification intent produced by a transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Intent {
    /// Announce a firing occurrence with this action.
    Notify(Action),
    /// Announce resolution of a possibly notified occurrence.
    Recovery,
}

/// Unattempted live intents that a transition makes obsolete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cancel {
    /// Keep every intent.
    Nothing,
    /// Cancel unattempted notify intents ranked below this action.
    Below(Action),
    /// Cancel every unattempted intent: the occurrence resolved before delivery.
    All,
}

/// The complete effect of applying one observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Transition {
    /// Proposed action recorded in decision history.
    pub action: Action,
    /// Why the decision was made.
    pub reason: Reason,
    /// The occurrence becomes resolved.
    pub resolve: bool,
    /// New highest action, if it rises.
    pub escalate_to: Option<Action>,
    /// Intent to create, if any.
    pub intent: Option<Intent>,
    /// Obsolete unattempted intents.
    pub cancel: Cancel,
}

impl Transition {
    fn record(reason: Reason) -> Self {
        Self {
            action: Action::Record,
            reason,
            resolve: false,
            escalate_to: None,
            intent: None,
            cancel: Cancel::Nothing,
        }
    }
}

/// Applies an observation to an occurrence.
///
/// * `prior` - durable occurrence state before the observation
/// * `status` - the individual alert's lifecycle status
/// * `proposal` - the policy proposal for a firing observation
/// * `notified` - whether live delivery may already have reached someone
///
/// Escalation is monotonic per destination: only a strictly higher action than
/// an intent already recorded for that destination produces another intent.
/// Pending and in-flight intents count. A digested occurrence has no intent; it
/// still escalates when a later proposal notifies immediately. Resolution never
/// reopens, and recovery requires a possible prior notification rather than
/// merely a recorded intent.
///
/// ```
/// use criers_core::{Action, Cancel, Intent, Notified, Policy, Prior, Status, apply};
/// let ping = Policy::default().propose(Some("critical"), true);
/// let first = apply(Prior::New, Status::Firing, ping, Notified::No);
/// assert_eq!(first.intent, Some(Intent::Notify(Action::Ping)));
/// let repeat = apply(Prior::Firing(Action::Ping), Status::Firing, ping, Notified::No);
/// assert_eq!(repeat.intent, None);
/// let early = apply(Prior::Firing(Action::Ping), Status::Resolved, ping, Notified::No);
/// assert_eq!((early.intent, early.cancel), (None, Cancel::All));
/// ```
pub fn apply(prior: Prior, status: Status, proposal: Proposal, notified: Notified) -> Transition {
    match (status, prior) {
        (_, Prior::Superseded) => Transition::record(Reason::SupersededOccurrence),
        (Status::Resolved, Prior::Resolved) => Transition::record(Reason::AlreadyResolved),
        (Status::Firing, Prior::Resolved) => Transition::record(Reason::LateFiringAfterResolution),
        (Status::Resolved, Prior::New | Prior::Firing(_)) => {
            let (action, reason, intent) = match notified {
                Notified::Possibly => (Action::QuietPost, Reason::Recovery, Some(Intent::Recovery)),
                Notified::No => (Action::Record, Reason::ResolutionWithoutNotification, None),
            };
            Transition {
                action,
                reason,
                resolve: true,
                escalate_to: None,
                intent,
                cancel: Cancel::All,
            }
        }
        (Status::Firing, Prior::New | Prior::Firing(_)) => {
            // A new occurrence always gets its policy decision, even a record-only one;
            // only an already-firing occurrence can repeat or fall below its highest action.
            if let Prior::Firing(highest) = prior
                && proposal.action.rank() <= highest.rank()
            {
                return Transition::record(Reason::UnchangedOrLowerPriority);
            }
            // Record and digest create no delivery intent; a digested occurrence is
            // reported by the next summary from its stored history.
            let intent = proposal
                .action
                .is_immediate()
                .then_some(Intent::Notify(proposal.action));
            Transition {
                action: proposal.action,
                reason: Reason::Policy(proposal.reason),
                resolve: false,
                escalate_to: Some(proposal.action),
                intent,
                cancel: Cancel::Below(proposal.action),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Policy;

    fn proposal(severity: &str) -> Proposal {
        Policy::default().propose(Some(severity), true)
    }

    #[test]
    fn first_record_only_observation_keeps_its_policy_decision() {
        let first = apply(Prior::New, Status::Firing, proposal("info"), Notified::No);
        let repeat = apply(
            Prior::Firing(Action::Record),
            Status::Firing,
            proposal("info"),
            Notified::No,
        );
        assert_eq!(
            (first.reason, repeat.reason),
            (
                Reason::Policy(crate::PolicyReason::ConfiguredSeverity),
                Reason::UnchangedOrLowerPriority
            )
        );
    }

    #[test]
    fn info_records_without_an_intent() {
        let transition = apply(Prior::New, Status::Firing, proposal("info"), Notified::No);
        assert_eq!(
            (transition.action, transition.intent),
            (Action::Record, None)
        );
    }

    fn digest_policy(severity: &str) -> Proposal {
        Policy::new(
            [
                ("critical".to_owned(), Action::Ping),
                ("warning".to_owned(), Action::Digest),
            ]
            .into(),
            Action::Ping,
            ["critical".to_owned()].into(),
        )
        .unwrap()
        .propose(Some(severity), true)
    }

    #[test]
    fn digested_warning_records_a_decision_without_an_intent() {
        let transition = apply(
            Prior::New,
            Status::Firing,
            digest_policy("warning"),
            Notified::No,
        );
        assert_eq!(
            (transition.action, transition.intent, transition.escalate_to),
            (Action::Digest, None, Some(Action::Digest))
        );
    }

    #[test]
    fn repeated_digested_warning_is_compacted() {
        let transition = apply(
            Prior::Firing(Action::Digest),
            Status::Firing,
            digest_policy("warning"),
            Notified::No,
        );
        assert_eq!(transition.reason, Reason::UnchangedOrLowerPriority);
    }

    #[test]
    fn digested_warning_that_turns_critical_pings_at_once() {
        let transition = apply(
            Prior::Firing(Action::Digest),
            Status::Firing,
            digest_policy("critical"),
            Notified::No,
        );
        assert_eq!(transition.intent, Some(Intent::Notify(Action::Ping)));
    }

    #[test]
    fn digested_warning_resolves_without_a_recovery_message() {
        let transition = apply(
            Prior::Firing(Action::Digest),
            Status::Resolved,
            digest_policy("warning"),
            Notified::No,
        );
        assert_eq!(
            (transition.resolve, transition.intent, transition.reason),
            (true, None, Reason::ResolutionWithoutNotification)
        );
    }

    #[test]
    fn warning_to_critical_escalates_and_cancels_the_lower_unattempted_post() {
        let transition = apply(
            Prior::Firing(Action::QuietPost),
            Status::Firing,
            proposal("critical"),
            Notified::Possibly,
        );
        assert_eq!(
            (transition.intent, transition.cancel),
            (
                Some(Intent::Notify(Action::Ping)),
                Cancel::Below(Action::Ping)
            )
        );
    }

    #[test]
    fn critical_to_warning_never_downgrades() {
        let transition = apply(
            Prior::Firing(Action::Ping),
            Status::Firing,
            proposal("warning"),
            Notified::Possibly,
        );
        assert_eq!(transition.reason, Reason::UnchangedOrLowerPriority);
    }

    #[test]
    fn resolution_after_possible_delivery_creates_a_recovery() {
        let transition = apply(
            Prior::Firing(Action::Ping),
            Status::Resolved,
            proposal("critical"),
            Notified::Possibly,
        );
        assert_eq!(transition.intent, Some(Intent::Recovery));
    }

    #[test]
    fn superseded_occurrence_ignores_resolution() {
        let transition = apply(
            Prior::Superseded,
            Status::Resolved,
            proposal("critical"),
            Notified::Possibly,
        );
        assert_eq!((transition.resolve, transition.intent), (false, None));
    }
}

//! Alertmanager wire types and deterministic notification rules.
//!
//! The crate has no I/O. A policy action is a proposed notification, not proof
//! of delivery; source severity, assessed impact and delivery outcome remain
//! separate concepts.

#![deny(missing_docs)]

mod lifecycle;
mod model;
mod policy;

pub use lifecycle::{Cancel, Intent, Notified, Prior, Reason, Transition, apply};
pub use model::{Action, Alert, MAX_ALERTS, Status, Webhook};
pub use policy::{MAX_SEVERITIES, Policy, PolicyError, PolicyReason, Proposal};

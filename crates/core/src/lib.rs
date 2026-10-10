//! Alertmanager wire types and deterministic routing policy.
//!
//! The accompanying binary durably records decisions in shadow mode.
//! A policy action is a proposed notification, not proof of delivery.

#![deny(missing_docs)]

mod model;

pub use model::{Action, Alert, Status, Webhook};

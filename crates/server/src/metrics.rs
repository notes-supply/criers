//! Prometheus metrics with bounded labels: configured IDs and static reason names only.

use std::sync::atomic::AtomicU64;

use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct SourceOutcome {
    pub source: String,
    pub outcome: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct SourceStatus {
    pub source: String,
    pub status: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct DecisionLabels {
    pub action: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct DestinationOutcome {
    pub destination: String,
    pub outcome: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct DestinationLabels {
    pub destination: String,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct SourceLabels {
    pub source: String,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct CoverageLabels {
    pub source: String,
    pub state: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct OutcomeLabels {
    pub outcome: &'static str,
}

#[derive(Clone, Debug, EncodeLabelSet, Eq, Hash, PartialEq)]
pub(crate) struct TableLabels {
    pub table: &'static str,
}

type FloatGauge = Gauge<f64, AtomicU64>;

/// Every exported metric. Handles are cheap to clone and lock-free to update.
pub(crate) struct Metrics {
    registry: Registry,
    pub ingest_requests: Family<SourceOutcome, Counter>,
    pub ingest_duration: Histogram,
    pub alerts: Family<SourceStatus, Counter>,
    pub decisions: Family<DecisionLabels, Counter>,
    pub heartbeats: Family<SourceLabels, Counter>,
    pub source_last_event_age: Family<SourceLabels, FloatGauge>,
    pub source_heartbeat_age: Family<SourceLabels, FloatGauge>,
    pub source_coverage: Family<CoverageLabels, Gauge>,
    pub delivery_attempts: Family<DestinationOutcome, Counter>,
    pub delivery_pending: Family<DestinationLabels, Gauge>,
    pub delivery_oldest_pending_age: Family<DestinationLabels, FloatGauge>,
    pub assessments: Family<OutcomeLabels, Counter>,
    pub assessment_queue: Gauge,
    pub storage_used_bytes: Gauge,
    pub storage_limit_bytes: Gauge,
    pub retention_deleted: Family<TableLabels, Counter>,
    pub credential_files_valid: Family<SourceLabels, Gauge>,
    pub credential_reload_failures: Family<SourceLabels, Counter>,
    pub summary_window_end: FloatGauge,
    pub ready: Gauge,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let mut registry = Registry::with_prefix("criers");
        let metrics = Self {
            ingest_requests: Family::default(),
            ingest_duration: Histogram::new([
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
            alerts: Family::default(),
            decisions: Family::default(),
            heartbeats: Family::default(),
            source_last_event_age: Family::default(),
            source_heartbeat_age: Family::default(),
            source_coverage: Family::default(),
            delivery_attempts: Family::default(),
            delivery_pending: Family::default(),
            delivery_oldest_pending_age: Family::default(),
            assessments: Family::default(),
            assessment_queue: Gauge::default(),
            storage_used_bytes: Gauge::default(),
            storage_limit_bytes: Gauge::default(),
            retention_deleted: Family::default(),
            credential_files_valid: Family::default(),
            credential_reload_failures: Family::default(),
            summary_window_end: FloatGauge::default(),
            ready: Gauge::default(),
            registry: Registry::default(),
        };
        let m = &metrics;
        registry.register(
            "ingest_requests",
            "Webhook requests by configured source and outcome",
            m.ingest_requests.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Accepted webhook handling time including the durable commit",
            m.ingest_duration.clone(),
        );
        registry.register(
            "alerts_observed",
            "Alert observations by source and status, excluding heartbeats",
            m.alerts.clone(),
        );
        registry.register(
            "decisions",
            "Recorded policy decisions by action and reason",
            m.decisions.clone(),
        );
        registry.register(
            "heartbeats",
            "Accepted heartbeat observations",
            m.heartbeats.clone(),
        );
        registry.register(
            "source_last_event_age_seconds",
            "Seconds since the source's last non-heartbeat webhook",
            m.source_last_event_age.clone(),
        );
        registry.register(
            "source_heartbeat_age_seconds",
            "Seconds since the source's last heartbeat",
            m.source_heartbeat_age.clone(),
        );
        registry.register(
            "source_coverage",
            "1 for the source's current coverage state",
            m.source_coverage.clone(),
        );
        registry.register(
            "delivery_attempts",
            "Destination send attempts by outcome",
            m.delivery_attempts.clone(),
        );
        registry.register(
            "delivery_pending",
            "Live deliveries waiting or in flight",
            m.delivery_pending.clone(),
        );
        registry.register(
            "delivery_oldest_pending_age_seconds",
            "Age of the oldest live delivery waiting or in flight",
            m.delivery_oldest_pending_age.clone(),
        );
        registry.register(
            "assessments",
            "Completed shadow assessments by outcome",
            m.assessments.clone(),
        );
        registry.register(
            "assessment_queue",
            "Assessments waiting or in flight",
            m.assessment_queue.clone(),
        );
        registry.register(
            "storage_used_bytes",
            "Bytes in use by SQLite pages, excluding the free list",
            m.storage_used_bytes.clone(),
        );
        registry.register(
            "storage_limit_bytes",
            "Configured SQLite size cap",
            m.storage_limit_bytes.clone(),
        );
        registry.register(
            "retention_deleted_rows",
            "Rows removed by retention",
            m.retention_deleted.clone(),
        );
        registry.register(
            "credential_files_valid",
            "Source credential files whose current content is valid",
            m.credential_files_valid.clone(),
        );
        registry.register(
            "credential_reload_failures",
            "Rejected credential file updates; last-good tokens stay active",
            m.credential_reload_failures.clone(),
        );
        registry.register(
            "summary_window_end_seconds",
            "Unix time at which the latest reported summary window ended",
            m.summary_window_end.clone(),
        );
        registry.register("ready", "1 when storage is writable", m.ready.clone());
        Self {
            registry,
            ..metrics
        }
    }

    /// Renders the OpenMetrics text exposition.
    pub(crate) fn render(&self) -> Result<String, std::fmt::Error> {
        let mut text = String::with_capacity(16_384);
        encode(&mut text, &self.registry)?;
        Ok(text)
    }
}

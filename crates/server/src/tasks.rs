//! Periodic background work: storage observation, retention, coverage and credential reloads.
//!
//! Each task performs one bounded transaction per tick and stops on cancellation.

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    credentials::Credentials,
    metrics::{
        CoverageLabels, DestinationLabels, Metrics, OutcomeLabels, SourceLabels, TableLabels,
    },
    store::{Stats, Store},
};

const COVERAGE_STATES: [&str; 4] = ["unknown", "healthy", "stale", "never_seen"];

async fn tick(cancel: &CancellationToken, period: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(period) => true,
    }
}

/// Probes storage writability and refreshes gauges.
pub(crate) async fn observe(
    store: Store,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    window_ms: i64,
    cancel: CancellationToken,
) {
    let period = config.storage.health_interval;
    loop {
        match store.probe().await {
            Ok(stats) => publish(&config, &metrics, &stats),
            Err(error) => tracing::error!(%error, "storage probe failed"),
        }
        let ready = store.health().ready(crate::now_ms(), window_ms).is_ok();
        metrics.ready.set(i64::from(ready));
        if !tick(&cancel, period).await {
            break;
        }
    }
}

fn publish(config: &Config, metrics: &Metrics, stats: &Stats) {
    let now = crate::now_ms();
    let seconds = |at: i64| (now - at).max(0) as f64 / 1000.0;
    metrics.storage_used_bytes.set(stats.used_bytes);
    metrics.storage_limit_bytes.set(stats.limit_bytes);
    metrics.assessment_queue.set(stats.assessment_queue);
    for destination in config.destinations.keys() {
        let labels = DestinationLabels {
            destination: destination.clone(),
        };
        let pending = stats
            .pending
            .iter()
            .find(|pending| pending.destination == *destination);
        metrics
            .delivery_pending
            .get_or_create(&labels)
            .set(pending.map_or(0, |pending| pending.count));
        metrics
            .delivery_oldest_pending_age
            .get_or_create(&labels)
            .set(pending.map_or(0.0, |pending| seconds(pending.oldest_created_at)));
    }
    for freshness in &stats.sources {
        if !config.sources.contains_key(&freshness.source) {
            continue;
        }
        let labels = SourceLabels {
            source: freshness.source.clone(),
        };
        if let Some(at) = freshness.last_event_at {
            metrics
                .source_last_event_age
                .get_or_create(&labels)
                .set(seconds(at));
        }
        // Without a heartbeat timestamp there is no age to report; a zero would
        // claim a fresh heartbeat, so the series is removed instead.
        match freshness.last_heartbeat_at {
            Some(at) => {
                metrics
                    .source_heartbeat_age
                    .get_or_create(&labels)
                    .set(seconds(at));
            }
            None => {
                metrics.source_heartbeat_age.remove(&labels);
            }
        }
        for state in COVERAGE_STATES {
            metrics
                .source_coverage
                .get_or_create(&CoverageLabels {
                    source: freshness.source.clone(),
                    state,
                })
                .set(i64::from(freshness.coverage == state));
        }
    }
}

/// Deletes expired history in bounded batches, catching up within each tick.
pub(crate) async fn retention(
    store: Store,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let retention = i64::try_from(config.storage.retention.as_millis()).unwrap_or(i64::MAX);
    let batch = config.storage.retention_batch;
    while tick(&cancel, config.storage.retention_interval).await {
        for _ in 0..20 {
            let cutoff = crate::now_ms().saturating_sub(retention);
            let deleted = match store.retention_step(cutoff, batch).await {
                Ok(deleted) => deleted,
                Err(error) => {
                    tracing::error!(%error, "retention step failed");
                    break;
                }
            };
            if deleted.assessments_closed > 0 {
                metrics
                    .assessments
                    .get_or_create(&OutcomeLabels {
                        outcome: "skipped_closed",
                    })
                    .inc_by(deleted.assessments_closed as u64);
            }
            for (table, count) in [
                ("decisions", deleted.decisions),
                ("events", deleted.events),
                ("occurrences", deleted.occurrences),
                ("outbox", deleted.deliveries),
                ("assessments", deleted.assessments),
            ] {
                if count > 0 {
                    metrics
                        .retention_deleted
                        .get_or_create(&TableLabels { table })
                        .inc_by(count as u64);
                }
            }
            let saturated = [
                deleted.assessments_closed,
                deleted.decisions,
                deleted.events,
                deleted.occurrences,
                deleted.deliveries,
                deleted.assessments,
            ]
            .iter()
            .any(|&count| count >= batch as usize);
            if !saturated || cancel.is_cancelled() {
                break;
            }
            // Yield the connection to ingestion between catch-up batches.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Turns heartbeat freshness into durable coverage notifications.
pub(crate) async fn coverage(
    store: Store,
    config: Arc<Config>,
    wake: Arc<Notify>,
    started_at: i64,
    cancel: CancellationToken,
) {
    // The first pass runs at startup even with no heartbeat configured, so
    // coverage persisted under an earlier configuration is reconciled. Without
    // monitored sources the task retires only after that pass has committed.
    let monitored = config
        .sources
        .values()
        .any(|source| source.heartbeat.is_some());
    let mut failures = 0u32;
    loop {
        match store
            .coverage_tick(Arc::clone(&config), crate::now_ms(), started_at)
            .await
        {
            Ok(changes) => {
                failures = 0;
                for change in &changes {
                    tracing::warn!(source = %change.source, state = change.state, notified = change.notified, "source coverage changed");
                }
                if changes.iter().any(|change| change.notified) {
                    wake.notify_waiters();
                }
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                tracing::error!(%error, "coverage evaluation failed");
            }
        }
        let period = match (monitored, failures) {
            (true, _) => Duration::from_secs(5),
            (false, 0) => break,
            (false, failures) => crate::delivery::backoff(
                failures,
                Duration::from_millis(200),
                Duration::from_secs(5),
            ),
        };
        if !tick(&cancel, period).await {
            break;
        }
    }
}

/// Polls credential files so rotations apply without rereading secrets per request.
pub(crate) async fn credentials(
    credentials: Arc<Credentials>,
    metrics: Arc<Metrics>,
    period: Duration,
    cancel: CancellationToken,
) {
    while tick(&cancel, period).await {
        let credentials = Arc::clone(&credentials);
        let reports = tokio::task::spawn_blocking(move || credentials.reload()).await;
        let Ok(reports) = reports else {
            tracing::error!("credential reload worker failed");
            continue;
        };
        for (source, report) in reports {
            let labels = SourceLabels { source };
            metrics
                .credential_files_valid
                .get_or_create(&labels)
                .set(report.valid as i64);
            for (path, error) in &report.failures {
                tracing::warn!(source = %labels.source, path = %path.display(), %error, "credential update rejected; keeping last-good token");
                metrics
                    .credential_reload_failures
                    .get_or_create(&labels)
                    .inc();
            }
        }
    }
}

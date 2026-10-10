//! Transactional SQLite store: the single durable owner of alert state.
//!
//! One connection serves every operation on blocking threads, so writes are
//! serialized and bounded. An exclusive lock file fences a second Criers
//! process on the same host from opening the same database.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use criers_core::{Action, Alert, Cancel, Intent, Notified, Prior, Reason, Status, Webhook, apply};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    assessment::{self, Answers, QUESTIONS_VERSION},
    config::{Config, Mode, Source},
    render::{self, Subject},
};

const SCHEMA_VERSION: i64 = 1;
const SUMMARY_CHARS: usize = 512;

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("database worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error("database lock poisoned")]
    Poisoned,
    #[error("encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("cannot open lock file {path}: {source}")]
    LockFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("another Criers process owns {0}; run exactly one replica per database")]
    Locked(PathBuf),
    #[error("database schema version {0} is newer than this Criers build supports")]
    FutureSchema(i64),
    #[error("source {0:?} is not configured")]
    UnknownSource(String),
}

/// Write-path health observed by real transactions and the periodic probe.
///
/// Readiness reads these atomics only, so it never waits behind ingestion.
#[derive(Debug, Default)]
pub(crate) struct Health {
    last_ok: AtomicI64,
    last_failure: AtomicI64,
}

impl Health {
    fn ok(&self, now: i64) {
        self.last_ok.fetch_max(now, Ordering::Relaxed);
    }

    fn failed(&self, now: i64) {
        self.last_failure.fetch_max(now, Ordering::Relaxed);
    }

    /// Ready when the latest write outcome succeeded within `window_ms`.
    pub(crate) fn ready(&self, now: i64, window_ms: i64) -> Result<(), &'static str> {
        let ok = self.last_ok.load(Ordering::Relaxed);
        let failure = self.last_failure.load(Ordering::Relaxed);
        if failure > 0 && failure >= ok {
            Err("latest storage write failed")
        } else if now - ok > window_ms {
            Err("no successful storage write within the readiness window")
        } else {
            Ok(())
        }
    }
}

struct Inner {
    connection: Mutex<Connection>,
    health: Health,
    page_size: i64,
    max_pages: i64,
    _lock: File,
}

#[derive(Clone)]
pub(crate) struct Store {
    inner: Arc<Inner>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Receipt {
    pub event_id: Option<i64>,
    pub mode: &'static str,
    pub coverage_gap: bool,
    pub heartbeat: bool,
    pub decisions: Vec<Decision>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Decision {
    pub occurrence_id: i64,
    pub action: Action,
    pub reason: &'static str,
    pub intent_created: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct Snapshot {
    pub events: i64,
    pub occurrences: i64,
    pub firing: i64,
    pub intents: i64,
    pub live_open: i64,
    pub last_event_at: Option<i64>,
    pub last_heartbeat_at: Option<i64>,
    pub truncated_alerts: i64,
    pub coverage: String,
}

/// Rows claimed together for one destination message.
#[derive(Debug)]
pub(crate) struct Batch {
    pub ids: Vec<i64>,
    pub lines: Vec<String>,
    pub ping: bool,
    pub mention_allowed: bool,
    pub possibly_duplicate: bool,
    pub max_failures: u32,
}

#[derive(Debug)]
pub(crate) enum Claim {
    Batch(Batch),
    /// Nothing is due; retry at the given time, or on wake-up if `None`.
    Wait(Option<i64>),
}

/// Destination-specific claim bounds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ClaimLimits {
    pub max_rows: u32,
    pub budget_chars: usize,
    pub mention_cooldown_ms: i64,
}

/// The outcome of one destination send applied to every row of its batch.
#[derive(Clone, Debug)]
pub(crate) enum Completion {
    Sent {
        remote_id: Option<String>,
        status: u16,
        mentioned: bool,
    },
    RateLimited {
        retry_at: i64,
        status: u16,
    },
    Retry {
        retry_at: i64,
        status: Option<u16>,
        error: String,
        ambiguous: bool,
    },
    Permanent {
        status: Option<u16>,
        error: String,
    },
}

#[derive(Debug)]
pub(crate) struct AssessmentJob {
    pub id: i64,
    pub context: String,
    pub attempts: u32,
}

/// A failed assessment attempt.
#[derive(Debug)]
pub(crate) struct AssessmentFailure {
    pub outcome: &'static str,
    pub error: String,
    /// When to retry, if the failure is transient.
    pub retry_at: Option<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct Retention {
    pub decisions: usize,
    pub events: usize,
    pub occurrences: usize,
    pub deliveries: usize,
    pub assessments: usize,
}

#[derive(Debug)]
pub(crate) struct PendingDeliveries {
    pub destination: String,
    pub count: i64,
    pub oldest_created_at: i64,
}

#[derive(Debug)]
pub(crate) struct SourceFreshness {
    pub source: String,
    pub last_event_at: Option<i64>,
    pub last_heartbeat_at: Option<i64>,
    pub coverage: String,
}

/// Storage and backlog figures gathered by the periodic probe.
#[derive(Debug)]
pub(crate) struct Stats {
    pub used_bytes: i64,
    pub limit_bytes: i64,
    pub pending: Vec<PendingDeliveries>,
    pub assessment_queue: i64,
    pub sources: Vec<SourceFreshness>,
}

/// A coverage state change and whether it created a notification intent.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CoverageChange {
    pub source: String,
    pub state: &'static str,
    pub notified: bool,
}

fn storage_failure(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::Database(_) | StoreError::Worker(_) | StoreError::Poisoned
    )
}

impl Store {
    /// Opens or creates the database, migrating the scaffold schema if present.
    ///
    /// # Errors
    /// Fails when another process holds the lock file, the parent directory is
    /// missing or unwritable, or the schema is from a newer release.
    pub(crate) async fn open(path: PathBuf, max_bytes: u64) -> Result<Self, StoreError> {
        let inner = tokio::task::spawn_blocking(move || open_blocking(&path, max_bytes)).await??;
        inner.health.ok(crate::now_ms());
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub(crate) fn health(&self) -> &Health {
        &self.inner.health
    }

    pub(crate) async fn with_connection<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut connection = inner.connection.lock().map_err(|_| StoreError::Poisoned)?;
            operation(&mut connection)
        })
        .await?
    }

    /// Runs a write whose failure counts against readiness.
    ///
    /// Success does not prove health: a claim or retention pass may write nothing.
    async fn write<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let result = self.with_connection(operation).await;
        if let Err(error) = &result
            && storage_failure(error)
        {
            self.inner.health.failed(crate::now_ms());
        }
        result
    }

    /// Runs a write whose success proves the database accepts new data.
    ///
    /// Only ingestion commits and the allocation probe qualify.
    async fn proving_write<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let result = self.write(operation).await;
        if result.is_ok() {
            self.inner.health.ok(crate::now_ms());
        }
        result
    }

    /// Records a webhook and its decisions and intents in one transaction.
    ///
    /// The caller acknowledges the webhook only after this returns `Ok`.
    /// The admission `permit` is held until the blocking transaction finishes, so
    /// a cancelled request cannot free capacity while its write is still queued.
    pub(crate) async fn ingest(
        &self,
        config: Arc<Config>,
        source: String,
        webhook: Webhook,
        permit: OwnedSemaphorePermit,
    ) -> Result<Receipt, StoreError> {
        self.proving_write(move |connection| {
            let _permit = permit;
            let now = crate::now_ms();
            let transaction = connection.transaction()?;
            let receipt = ingest_transaction(&transaction, &config, &source, &webhook, now)?;
            transaction.commit()?;
            Ok(receipt)
        })
        .await
    }

    pub(crate) async fn snapshot(&self, source: String) -> Result<Snapshot, StoreError> {
        self.with_connection(move |connection| {
            let status = connection
                .query_row(
                    "SELECT last_event_at, last_heartbeat_at, truncated_alerts, coverage
                     FROM source_status WHERE source = ?1",
                    [source.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let count = |query| connection.query_row(query, [source.as_str()], |row| row.get(0));
            let (last_event_at, last_heartbeat_at, truncated_alerts, coverage) =
                status.unwrap_or((None, None, 0, "unknown".to_owned()));
            Ok(Snapshot {
                events: count("SELECT count(*) FROM events WHERE source = ?1")?,
                occurrences: count("SELECT count(*) FROM occurrences WHERE source = ?1")?,
                firing: count(
                    "SELECT count(*) FROM occurrences WHERE source = ?1 AND status = 'firing'",
                )?,
                intents: count("SELECT count(*) FROM outbox WHERE source = ?1")?,
                live_open: count(
                    "SELECT count(*) FROM outbox WHERE source = ?1
                     AND state IN ('pending', 'in_flight')",
                )?,
                last_event_at,
                last_heartbeat_at,
                truncated_alerts,
                coverage,
            })
        })
        .await
    }

    /// Claims the next due batch for a destination and marks it in flight.
    pub(crate) async fn claim_deliveries(
        &self,
        destination: String,
        now: i64,
        limits: ClaimLimits,
    ) -> Result<Claim, StoreError> {
        self.write(move |connection| {
            let transaction = connection.transaction()?;
            let claim = claim_transaction(&transaction, &destination, now, limits)?;
            transaction.commit()?;
            Ok(claim)
        })
        .await
    }

    /// Applies a send outcome to the in-flight rows of a batch.
    pub(crate) async fn complete_deliveries(
        &self,
        destination: String,
        ids: Vec<i64>,
        completion: Completion,
        max_attempts: u32,
    ) -> Result<(), StoreError> {
        self.write(move |connection| {
            let now = crate::now_ms();
            let transaction = connection.transaction()?;
            complete_transaction(
                &transaction,
                &destination,
                &ids,
                &completion,
                max_attempts,
                now,
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn claim_assessments(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<AssessmentJob>, StoreError> {
        self.write(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE assessments SET state = 'done', outcome = 'skipped_closed', completed_at = ?1
                 WHERE id IN (SELECT a.id FROM assessments a JOIN occurrences o ON o.id = a.occurrence_id
                    WHERE a.state IN ('pending', 'in_flight') AND a.state = 'pending'
                    AND o.status != 'firing' LIMIT 100)",
                [now],
            )?;
            let jobs = {
                let mut statement = transaction.prepare_cached(
                    // The repeated state term lets SQLite use the partial index.
                    "SELECT id, context, attempts FROM assessments
                     WHERE state IN ('pending', 'in_flight') AND state = 'pending'
                     AND due_at <= ?1 ORDER BY due_at, id LIMIT ?2",
                )?;
                statement
                    .query_map(params![now, limit as i64], |row| {
                        Ok(AssessmentJob {
                            id: row.get(0)?,
                            context: row.get(1)?,
                            attempts: row.get::<_, u32>(2)? + 1,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for job in &jobs {
                transaction.execute(
                    "UPDATE assessments SET state = 'in_flight', attempts = attempts + 1 WHERE id = ?1",
                    [job.id],
                )?;
            }
            transaction.commit()?;
            Ok(jobs)
        })
        .await
    }

    /// Records an assessment attempt; returns the final outcome, or `None` on retry.
    ///
    /// A result for an occurrence that resolved or was superseded meanwhile is
    /// kept as `stale`; shadow assessment never creates notifications.
    pub(crate) async fn record_assessment(
        &self,
        id: i64,
        result: Result<Answers, AssessmentFailure>,
        now: i64,
        max_attempts: u32,
    ) -> Result<Option<&'static str>, StoreError> {
        self.write(move |connection| {
            let transaction = connection.transaction()?;
            let (attempts, status): (u32, String) = transaction.query_row(
                "SELECT a.attempts, o.status FROM assessments a
                 JOIN occurrences o ON o.id = a.occurrence_id WHERE a.id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let outcome = match result {
                Ok(answers) => {
                    let outcome = if status == "firing" {
                        "assessed"
                    } else {
                        "stale"
                    };
                    transaction.execute(
                        "UPDATE assessments SET state = 'done', outcome = ?2, model = ?3,
                            impact_probability = ?4, urgency_score = ?5, urgency_probabilities = ?6,
                            confidence = ?7, input_tokens = ?8, latency_ms = ?9, completed_at = ?10,
                            error = NULL
                         WHERE id = ?1",
                        params![
                            id,
                            outcome,
                            answers.model,
                            answers.impact_probability,
                            answers.urgency_score,
                            answers.urgency_probabilities,
                            answers.confidence,
                            answers.input_tokens,
                            answers.latency_ms,
                            now
                        ],
                    )?;
                    Some(outcome)
                }
                Err(AssessmentFailure {
                    outcome,
                    error,
                    retry_at: Some(retry_at),
                }) if attempts < max_attempts => {
                    transaction.execute(
                        "UPDATE assessments SET state = 'pending', due_at = ?2, error = ?3,
                            outcome = ?4 WHERE id = ?1",
                        params![id, retry_at, error, outcome],
                    )?;
                    None
                }
                Err(AssessmentFailure { outcome, error, .. }) => {
                    transaction.execute(
                        "UPDATE assessments SET state = 'done', outcome = ?2, error = ?3,
                            completed_at = ?4 WHERE id = ?1",
                        params![id, outcome, error, now],
                    )?;
                    Some(outcome)
                }
            };
            transaction.commit()?;
            Ok(outcome)
        })
        .await
    }

    /// Evaluates heartbeat freshness and records durable coverage transitions.
    pub(crate) async fn coverage_tick(
        &self,
        config: Arc<Config>,
        now: i64,
        started_at: i64,
    ) -> Result<Vec<CoverageChange>, StoreError> {
        self.write(move |connection| {
            let transaction = connection.transaction()?;
            let changes = coverage_transaction(&transaction, &config, now, started_at)?;
            transaction.commit()?;
            Ok(changes)
        })
        .await
    }

    /// Deletes at most `batch` rows per table older than `cutoff`.
    ///
    /// Unresolved occurrences, open deliveries and their occurrences are kept.
    pub(crate) async fn retention_step(
        &self,
        cutoff: i64,
        batch: u32,
    ) -> Result<Retention, StoreError> {
        self.write(move |connection| {
            let transaction = connection.transaction()?;
            // Decisions go first so deleting their events cascades to nothing,
            // keeping every statement within `batch` rows.
            let decisions = transaction.execute(
                "DELETE FROM decisions WHERE id IN
                    (SELECT d.id FROM events e JOIN decisions d ON d.event_id = e.id
                     WHERE e.received_at < ?1 ORDER BY e.received_at LIMIT ?2)",
                params![cutoff, batch],
            )?;
            let deleted = Retention {
                decisions,
                events: transaction.execute(
                    "DELETE FROM events WHERE id IN
                            (SELECT e.id FROM events e WHERE e.received_at < ?1
                             AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.event_id = e.id)
                             ORDER BY e.received_at LIMIT ?2)",
                    params![cutoff, batch],
                )?,
                occurrences: transaction.execute(
                    "DELETE FROM occurrences WHERE id IN
                        (SELECT o.id FROM occurrences o WHERE o.closed_at < ?1
                         AND NOT EXISTS (SELECT 1 FROM outbox b WHERE b.occurrence_id = o.id
                            AND b.state IN ('pending', 'in_flight'))
                         ORDER BY o.closed_at LIMIT ?2)",
                    params![cutoff, batch],
                )?,
                // Deliveries of a still-firing occurrence are kept: its recovery
                // depends on knowing that the firing message went out.
                deliveries: transaction.execute(
                    "DELETE FROM outbox WHERE id IN
                        (SELECT b.id FROM outbox b WHERE b.completed_at < ?1
                         AND b.state NOT IN ('pending', 'in_flight')
                         AND NOT EXISTS (SELECT 1 FROM occurrences o
                            WHERE o.id = b.occurrence_id AND o.status = 'firing')
                         ORDER BY b.completed_at LIMIT ?2)",
                    params![cutoff, batch],
                )?,
                assessments: transaction.execute(
                    "DELETE FROM assessments WHERE id IN
                        (SELECT id FROM assessments WHERE state = 'done' AND completed_at < ?1
                         ORDER BY completed_at LIMIT ?2)",
                    params![cutoff, batch],
                )?,
            };
            transaction.commit()?;
            Ok(deleted)
        })
        .await
    }

    /// Ends open live deliveries whose destination is no longer configured.
    ///
    /// Without this, no worker would ever own them, and later intents of the same
    /// occurrence would wait behind them forever.
    pub(crate) async fn abandon_unconfigured(
        &self,
        destinations: Vec<String>,
    ) -> Result<usize, StoreError> {
        self.write(move |connection| {
            Ok(connection.execute(
                "UPDATE outbox SET completed_at = ?2, last_error = 'destination_not_configured',
                    state = CASE WHEN possibly_delivered = 1 THEN 'ambiguous' ELSE 'failed' END
                 WHERE state IN ('pending', 'in_flight')
                 AND destination NOT IN (SELECT value FROM json_each(?1))",
                params![serde_json::to_string(&destinations)?, crate::now_ms()],
            )?)
        })
        .await
    }

    /// Proves writability and page allocation, then gathers operational figures.
    pub(crate) async fn probe(&self) -> Result<Stats, StoreError> {
        let (page_size, max_pages) = (self.inner.page_size, self.inner.max_pages);
        self.proving_write(move |connection| {
            let now = crate::now_ms();
            connection.execute(
                "INSERT INTO meta(key, value) VALUES ('probe_at', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [now.to_string()],
            )?;
            // A rolled-back allocation detects a capped or full database.
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO probe(payload) VALUES (zeroblob(?1))",
                [page_size * 2],
            )?;
            transaction.rollback()?;
            stats(connection, page_size, max_pages)
        })
        .await
    }
}

fn open_blocking(path: &Path, max_bytes: u64) -> Result<Inner, StoreError> {
    let lock_path = path.with_extension("lock");
    let lock = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|source| StoreError::LockFile {
            path: lock_path.clone(),
            source,
        })?;
    if lock.try_lock().is_err() {
        return Err(StoreError::Locked(path.to_path_buf()));
    }
    let mut connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA foreign_keys = ON;
         PRAGMA journal_size_limit = 67108864;",
    )?;
    migrate(&mut connection)?;
    let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let max_pages = i64::try_from(max_bytes).unwrap_or(i64::MAX) / page_size;
    connection.pragma_update(None, "max_page_count", max_pages)?;
    let now = crate::now_ms();
    // A crash may have interrupted sends; their outcome is unknown, so retries are
    // marked as possible duplicates rather than assumed undelivered.
    connection.execute(
        "UPDATE outbox SET state = 'pending', possibly_delivered = 1, due_at = min(due_at, ?1),
            last_error = 'interrupted_by_restart'
         WHERE state = 'in_flight'",
        [now],
    )?;
    connection.execute(
        "UPDATE assessments SET state = 'pending' WHERE state = 'in_flight'",
        [],
    )?;
    Ok(Inner {
        connection: Mutex::new(connection),
        health: Health::default(),
        page_size,
        max_pages,
        _lock: lock,
    })
}

fn migrate(connection: &mut Connection) -> Result<(), StoreError> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    match version {
        SCHEMA_VERSION => return Ok(()),
        0 => {}
        newer => return Err(StoreError::FutureSchema(newer)),
    }
    let transaction = connection.transaction()?;
    let scaffold: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'events')",
        [],
        |row| row.get(0),
    )?;
    if scaffold {
        tracing::info!("migrating scaffold database to schema version 1");
        for table in [
            "events",
            "occurrences",
            "decisions",
            "outbox",
            "source_status",
        ] {
            transaction.execute_batch(&format!(
                "CREATE TEMP TABLE legacy_{table} AS SELECT * FROM main.{table};"
            ))?;
        }
        transaction.execute_batch(
            "DROP TABLE decisions; DROP TABLE outbox; DROP TABLE occurrences;
             DROP TABLE events; DROP TABLE source_status;",
        )?;
    }
    transaction.execute_batch(include_str!("store/schema.sql"))?;
    if scaffold {
        transaction.execute_batch(include_str!("store/migrate_v0.sql"))?;
    }
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

struct AlertContext<'a> {
    config: &'a Config,
    source_id: &'a str,
    source: &'a Source,
    destination: &'a str,
    event_id: i64,
    now: i64,
}

/// Remaining assessment queue capacity, counted lazily once per webhook.
struct AssessmentBudget(Option<i64>);

fn ingest_transaction(
    transaction: &Transaction<'_>,
    config: &Config,
    source_id: &str,
    webhook: &Webhook,
    now: i64,
) -> Result<Receipt, StoreError> {
    let source = config
        .sources
        .get(source_id)
        .ok_or_else(|| StoreError::UnknownSource(source_id.to_owned()))?;
    let heartbeat = source
        .heartbeat
        .as_ref()
        .map(|heartbeat| heartbeat.alertname.as_str());
    let is_heartbeat = |alert: &Alert| heartbeat == Some(alert.alertname());
    let heartbeat_seen = webhook
        .alerts
        .iter()
        .any(|alert| is_heartbeat(alert) && alert.status == Status::Firing);
    let observed = webhook
        .alerts
        .iter()
        .filter(|alert| !is_heartbeat(alert))
        .count();
    let mut decisions = Vec::with_capacity(observed);
    let event_id = if observed > 0 {
        transaction.execute(
            "INSERT INTO events(source, received_at, receiver, group_key, alerts, truncated_alerts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                source_id,
                now,
                webhook.receiver,
                webhook.group_key,
                webhook.alerts.len() as i64,
                webhook.truncated_alerts
            ],
        )?;
        let context = AlertContext {
            config,
            source_id,
            source,
            destination: source.destination_for(&webhook.receiver),
            event_id: transaction.last_insert_rowid(),
            now,
        };
        let mut budget = AssessmentBudget(None);
        for alert in webhook.alerts.iter().filter(|alert| !is_heartbeat(alert)) {
            decisions.push(process_alert(transaction, &context, alert, &mut budget)?);
        }
        Some(context.event_id)
    } else {
        None
    };
    transaction.execute(
        "INSERT INTO source_status(source, last_event_at, truncated_alerts, last_heartbeat_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(source) DO UPDATE SET
            last_event_at = coalesce(excluded.last_event_at, last_event_at),
            truncated_alerts = excluded.truncated_alerts,
            last_heartbeat_at = coalesce(excluded.last_heartbeat_at, last_heartbeat_at)",
        params![
            source_id,
            event_id.map(|_| now),
            webhook.truncated_alerts,
            heartbeat_seen.then_some(now)
        ],
    )?;
    Ok(Receipt {
        event_id,
        mode: config.mode.as_str(),
        coverage_gap: webhook.truncated_alerts > 0,
        heartbeat: heartbeat_seen,
        decisions,
    })
}

fn process_alert(
    transaction: &Transaction<'_>,
    context: &AlertContext<'_>,
    alert: &Alert,
    budget: &mut AssessmentBudget,
) -> Result<Decision, StoreError> {
    let AlertContext {
        config,
        source_id,
        source,
        destination,
        event_id,
        now,
    } = *context;
    let observed = upsert_occurrence(transaction, context, alert)?;
    let id = observed.id;
    let recovery_destination: Option<String> = if alert.status == Status::Resolved
        && matches!(observed.prior, Prior::New | Prior::Firing(_))
    {
        transaction
            .prepare_cached(
                "SELECT destination FROM outbox WHERE occurrence_id = ?1 AND mode = 'live'
                 AND kind = 'notify'
                 AND (state IN ('sent', 'ambiguous', 'in_flight') OR possibly_delivered = 1)
                 ORDER BY id DESC LIMIT 1",
            )?
            .query_row([id], |row| row.get(0))
            .optional()?
    } else {
        None
    };
    let notified = if recovery_destination.is_some() {
        Notified::Possibly
    } else {
        Notified::No
    };
    let proposal = config
        .policy
        .propose(alert.severity(), source.trust_severity);
    let transition = apply(observed.prior, alert.status, proposal, notified);
    let firing_update = alert.status == Status::Firing && observed.prior != Prior::Superseded;
    transaction
        .prepare_cached(
            "UPDATE occurrences SET last_seen_at = ?2, observations = observations + 1,
                labels = CASE WHEN ?3 THEN ?4 ELSE labels END,
                summary = CASE WHEN ?3 THEN ?5 ELSE summary END,
                severity = CASE WHEN ?3 THEN ?6 ELSE severity END,
                status = CASE WHEN ?7 THEN 'resolved' ELSE status END,
                closed_at = CASE WHEN ?7 THEN ?2 ELSE closed_at END,
                highest_action = max(highest_action, ?8)
             WHERE id = ?1",
        )?
        .execute(params![
            id,
            now,
            firing_update,
            observed.labels,
            observed.summary,
            alert.severity(),
            transition.resolve,
            transition.escalate_to.map_or(0, Action::rank)
        ])?;
    match transition.cancel {
        Cancel::Nothing | Cancel::Below(Action::Record | Action::QuietPost) => {}
        Cancel::Below(Action::Ping) => {
            supersede_unattempted(transaction, id, Some("quiet_post"), "escalated", now)?;
        }
        Cancel::All => {
            supersede_unattempted(transaction, id, None, "resolved_before_delivery", now)?;
        }
    }
    let subject = Subject {
        source: source_id,
        fingerprint: &observed.fingerprint,
        alertname: alert.alertname(),
        severity: alert.severity(),
        summary: &observed.summary,
    };
    let intent = match transition.intent {
        None => None,
        Some(Intent::Notify(action)) => Some(NewIntent {
            occurrence: Some(id),
            source: source_id,
            destination,
            kind: "notify",
            action,
            content: render::firing(&subject),
        }),
        Some(Intent::Recovery) => Some(NewIntent {
            occurrence: Some(id),
            source: source_id,
            destination: recovery_destination.as_deref().unwrap_or(destination),
            kind: "recovery",
            action: Action::QuietPost,
            content: render::resolved(&subject),
        }),
    };
    let intent_id = match intent {
        Some(intent) => insert_intent(transaction, config.mode, &intent, now)?,
        None => None,
    };
    if !matches!(
        transition.reason,
        Reason::UnchangedOrLowerPriority | Reason::AlreadyResolved
    ) {
        transaction
            .prepare_cached(
                "INSERT INTO decisions(event_id, occurrence_id, action, reason, policy_version,
                    intent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?
            .execute(params![
                event_id,
                id,
                transition.action.as_str(),
                transition.reason.as_str(),
                config.policy_version,
                intent_id
            ])?;
    }
    if observed.prior == Prior::New && alert.status == Status::Firing && !proposal.floor {
        enqueue_assessment(transaction, config, id, alert, budget, now)?;
    }
    Ok(Decision {
        occurrence_id: id,
        action: transition.action,
        reason: transition.reason.as_str(),
        intent_created: intent_id.is_some(),
    })
}

/// The occurrence an observation belongs to, and its state before the observation.
struct Observed {
    id: i64,
    prior: Prior,
    fingerprint: String,
    labels: String,
    summary: String,
}

/// Finds or creates the occurrence and applies stale-event protection.
///
/// An older `startsAt` than one already seen is recorded as superseded; a newer
/// one supersedes older firing occurrences of the same fingerprint.
fn upsert_occurrence(
    transaction: &Transaction<'_>,
    context: &AlertContext<'_>,
    alert: &Alert,
) -> Result<Observed, StoreError> {
    let (source_id, now) = (context.source_id, context.now);
    let fingerprint = alert.fingerprint.to_ascii_lowercase();
    let starts_at = alert.start_key();
    let latest: Option<String> = transaction
        .prepare_cached(
            "SELECT max(starts_at) FROM occurrences WHERE source = ?1 AND fingerprint = ?2",
        )?
        .query_row(params![source_id, fingerprint], |row| row.get(0))?;
    let stale = latest.as_ref().is_some_and(|latest| *latest > starts_at);
    let newest = latest.as_ref().is_some_and(|latest| *latest < starts_at);
    let labels = serde_json::to_string(&alert.labels)?;
    let summary: String = alert
        .annotations
        .get("summary")
        .map(|summary| summary.chars().take(SUMMARY_CHARS).collect())
        .unwrap_or_default();
    let inserted = transaction
        .prepare_cached(
            "INSERT INTO occurrences(source, fingerprint, starts_at, status, highest_action,
                alertname, severity, labels, summary, first_seen_at, last_seen_at, observations,
                closed_at)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?8, ?9, ?9, 0, ?10)
             ON CONFLICT DO NOTHING",
        )?
        .execute(params![
            source_id,
            fingerprint,
            starts_at,
            if stale { "superseded" } else { "firing" },
            alert.alertname(),
            alert.severity(),
            labels,
            summary,
            now,
            stale.then_some(now)
        ])?
        > 0;
    let (id, status, highest): (i64, String, u8) = transaction
        .prepare_cached(
            "SELECT id, status, highest_action FROM occurrences
             WHERE source = ?1 AND fingerprint = ?2 AND starts_at = ?3",
        )?
        .query_row(params![source_id, fingerprint, starts_at], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if inserted && newest {
        supersede_older(transaction, source_id, &fingerprint, &starts_at, now)?;
    }
    let prior = if stale {
        Prior::Superseded
    } else if inserted {
        Prior::New
    } else {
        match status.as_str() {
            "resolved" => Prior::Resolved,
            "superseded" => Prior::Superseded,
            _ => Prior::Firing(Action::from_rank(highest).unwrap_or(Action::Ping)),
        }
    };
    Ok(Observed {
        id,
        prior,
        fingerprint,
        labels,
        summary,
    })
}

/// Queues a shadow assessment, or records a skip when the queue is full.
fn enqueue_assessment(
    transaction: &Transaction<'_>,
    config: &Config,
    occurrence: i64,
    alert: &Alert,
    budget: &mut AssessmentBudget,
    now: i64,
) -> Result<(), StoreError> {
    let Some(assessment) = &config.assessment else {
        return Ok(());
    };
    let remaining = match budget.0 {
        Some(remaining) => remaining,
        None => {
            let queued: i64 = transaction.query_row(
                "SELECT count(*) FROM assessments WHERE state IN ('pending', 'in_flight')",
                [],
                |row| row.get(0),
            )?;
            i64::from(assessment.max_pending) - queued
        }
    };
    budget.0 = Some(remaining - 1);
    let context = assessment::context(alert, assessment)?;
    let (state, outcome, completed_at) = if remaining > 0 {
        ("pending", None, None)
    } else {
        ("done", Some("skipped_queue_full"), Some(now))
    };
    transaction
        .prepare_cached(
            "INSERT INTO assessments(occurrence_id, state, outcome, context, context_sha256,
                context_fields, policy_version, requested_model, due_at, created_at,
                completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?10)",
        )?
        .execute(params![
            occurrence,
            state,
            outcome,
            context.json,
            context.sha256,
            context.fields,
            format!("{QUESTIONS_VERSION};{}", config.policy_version),
            assessment.model,
            now,
            completed_at
        ])?;
    Ok(())
}

/// A newer start for the same fingerprint replaces older firing occurrences.
fn supersede_older(
    transaction: &Transaction<'_>,
    source: &str,
    fingerprint: &str,
    starts_at: &str,
    now: i64,
) -> Result<(), StoreError> {
    transaction
        .prepare_cached(
            "UPDATE outbox SET state = 'superseded', completed_at = ?4,
                last_error = 'superseded_by_newer_occurrence'
             WHERE state = 'pending' AND possibly_delivered = 0 AND occurrence_id IN
                (SELECT id FROM occurrences WHERE source = ?1 AND fingerprint = ?2
                 AND starts_at < ?3 AND status = 'firing')",
        )?
        .execute(params![source, fingerprint, starts_at, now])?;
    transaction
        .prepare_cached(
            "UPDATE occurrences SET status = 'superseded', closed_at = ?4
             WHERE source = ?1 AND fingerprint = ?2 AND starts_at < ?3 AND status = 'firing'",
        )?
        .execute(params![source, fingerprint, starts_at, now])?;
    Ok(())
}

/// Cancels live intents that never reached a destination.
fn supersede_unattempted(
    transaction: &Transaction<'_>,
    occurrence: i64,
    action: Option<&str>,
    reason: &str,
    now: i64,
) -> Result<(), StoreError> {
    transaction
        .prepare_cached(
            "UPDATE outbox SET state = 'superseded', completed_at = ?2, last_error = ?3
             WHERE occurrence_id = ?1 AND state = 'pending' AND possibly_delivered = 0
             AND kind = 'notify' AND (?4 IS NULL OR action = ?4)",
        )?
        .execute(params![occurrence, now, reason, action])?;
    Ok(())
}

/// A durable notification intent to record.
struct NewIntent<'a> {
    occurrence: Option<i64>,
    source: &'a str,
    destination: &'a str,
    kind: &'static str,
    action: Action,
    content: String,
}

/// Records an intent; shadow intents are terminal and can never be claimed.
fn insert_intent(
    transaction: &Transaction<'_>,
    mode: Mode,
    intent: &NewIntent<'_>,
    now: i64,
) -> Result<Option<i64>, StoreError> {
    let (state, completed_at) = match mode {
        Mode::Shadow => ("shadow", Some(now)),
        Mode::Live => ("pending", None),
    };
    let inserted = transaction
        .prepare_cached(
            "INSERT INTO outbox(occurrence_id, source, destination, kind, action, mode, state,
                content, created_at, due_at, completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?10) ON CONFLICT DO NOTHING",
        )?
        .execute(params![
            intent.occurrence,
            intent.source,
            intent.destination,
            intent.kind,
            intent.action.as_str(),
            mode.as_str(),
            state,
            intent.content,
            now,
            completed_at
        ])?;
    Ok((inserted > 0).then(|| transaction.last_insert_rowid()))
}

fn claim_transaction(
    transaction: &Transaction<'_>,
    destination: &str,
    now: i64,
    limits: ClaimLimits,
) -> Result<Claim, StoreError> {
    let (paused_until, last_mention): (i64, i64) = transaction
        .query_row(
            "SELECT paused_until, last_mention_at FROM destination_state WHERE destination = ?1",
            [destination],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((0, 0));
    if paused_until > now {
        return Ok(Claim::Wait(Some(paused_until)));
    }
    struct Candidate {
        id: i64,
        kind: String,
        action: String,
        content: String,
        failures: u32,
        possibly_delivered: bool,
        occurrence: Option<i64>,
    }
    // An occurrence's intents are delivered in creation order: a row waits while an
    // earlier row of the same occurrence is pending or in flight.
    let candidates = transaction
        .prepare_cached(
            "SELECT b.id, b.kind, b.action, b.content, b.attempts - b.throttled,
                b.possibly_delivered, b.occurrence_id
             FROM outbox b
             WHERE b.destination = ?1 AND b.state IN ('pending', 'in_flight')
             AND b.state = 'pending' AND b.due_at <= ?2
             AND NOT EXISTS (SELECT 1 FROM outbox e WHERE e.occurrence_id = b.occurrence_id
                AND e.id < b.id AND e.state IN ('pending', 'in_flight'))
             ORDER BY b.due_at, b.id LIMIT ?3",
        )?
        .query_map(params![destination, now, limits.max_rows], |row| {
            Ok(Candidate {
                id: row.get(0)?,
                kind: row.get(1)?,
                action: row.get(2)?,
                content: row.get(3)?,
                failures: row.get(4)?,
                possibly_delivered: row.get(5)?,
                occurrence: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut batch = Batch {
        ids: Vec::with_capacity(candidates.len()),
        lines: Vec::with_capacity(candidates.len()),
        ping: false,
        mention_allowed: false,
        possibly_duplicate: false,
        max_failures: 0,
    };
    let mut used = 0;
    for candidate in candidates {
        if candidate.kind == "recovery" {
            let delivered: bool = transaction
                .prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM outbox WHERE occurrence_id = ?1 AND mode = 'live'
                     AND kind = 'notify' AND (state IN ('sent', 'ambiguous') OR possibly_delivered = 1))",
                )?
                .query_row([candidate.occurrence], |row| row.get(0))?;
            if !delivered {
                transaction.execute(
                    "UPDATE outbox SET state = 'superseded', completed_at = ?2,
                        last_error = 'no_prior_delivery' WHERE id = ?1",
                    params![candidate.id, now],
                )?;
                continue;
            }
        }
        let length = candidate.content.chars().count() + 1;
        if !batch.ids.is_empty() && used + length > limits.budget_chars {
            break;
        }
        used += length;
        batch.ping |= candidate.action == "ping";
        batch.possibly_duplicate |= candidate.possibly_delivered;
        batch.max_failures = batch.max_failures.max(candidate.failures);
        batch.ids.push(candidate.id);
        batch.lines.push(candidate.content);
    }
    if batch.ids.is_empty() {
        // Rows waiting behind an earlier send of their occurrence are not claimable,
        // so they must not set the next wake-up either.
        let next: Option<i64> = transaction
            .prepare_cached(
                "SELECT min(b.due_at) FROM outbox b
                 WHERE b.destination = ?1 AND b.state IN ('pending', 'in_flight')
                 AND b.state = 'pending'
                 AND NOT EXISTS (SELECT 1 FROM outbox e WHERE e.occurrence_id = b.occurrence_id
                    AND e.id < b.id AND e.state IN ('pending', 'in_flight'))",
            )?
            .query_row([destination], |row| row.get(0))?;
        return Ok(Claim::Wait(next));
    }
    transaction.execute(
        "UPDATE outbox SET state = 'in_flight', attempts = attempts + 1
         WHERE id IN (SELECT value FROM json_each(?1))",
        [serde_json::to_string(&batch.ids)?],
    )?;
    batch.mention_allowed = batch.ping && now - last_mention >= limits.mention_cooldown_ms;
    Ok(Claim::Batch(batch))
}

fn complete_transaction(
    transaction: &Transaction<'_>,
    destination: &str,
    ids: &[i64],
    completion: &Completion,
    max_attempts: u32,
    now: i64,
) -> Result<(), StoreError> {
    let ids = serde_json::to_string(ids)?;
    let set_destination = |column: &str, value: i64| {
        transaction.execute(
            &format!(
                "INSERT INTO destination_state(destination, {column}) VALUES (?1, ?2)
                 ON CONFLICT(destination) DO UPDATE SET {column} = max({column}, excluded.{column})"
            ),
            params![destination, value],
        )
    };
    match completion {
        Completion::Sent {
            remote_id,
            status,
            mentioned,
        } => {
            transaction.execute(
                "UPDATE outbox SET state = 'sent', remote_id = ?2, last_status = ?3,
                    last_error = NULL, completed_at = ?4
                 WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'in_flight'",
                params![ids, remote_id, status, now],
            )?;
            if *mentioned {
                set_destination("last_mention_at", now)?;
            }
        }
        Completion::RateLimited { retry_at, status } => {
            // Throttling is bounded separately so a persistent 429 cannot retry forever.
            transaction.execute(
                "UPDATE outbox SET throttled = throttled + 1, last_status = ?2,
                    last_error = 'rate_limited', due_at = ?3,
                    state = CASE WHEN attempts >= ?4 * 10
                        THEN CASE WHEN possibly_delivered = 1 THEN 'ambiguous' ELSE 'failed' END
                        ELSE 'pending' END,
                    completed_at = CASE WHEN attempts >= ?4 * 10 THEN ?5 END
                 WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'in_flight'",
                params![ids, status, retry_at, max_attempts, now],
            )?;
            set_destination("paused_until", *retry_at)?;
        }
        Completion::Retry {
            retry_at,
            status,
            error,
            ambiguous,
        } => {
            transaction.execute(
                "UPDATE outbox SET possibly_delivered = possibly_delivered | ?5,
                    last_status = ?2, last_error = ?3, due_at = ?4,
                    state = CASE WHEN attempts - throttled >= ?6
                        THEN CASE WHEN possibly_delivered | ?5 THEN 'ambiguous' ELSE 'failed' END
                        ELSE 'pending' END,
                    completed_at = CASE WHEN attempts - throttled >= ?6 THEN ?7 END
                 WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'in_flight'",
                params![ids, status, error, retry_at, ambiguous, max_attempts, now],
            )?;
        }
        Completion::Permanent { status, error } => {
            transaction.execute(
                "UPDATE outbox SET last_status = ?2, last_error = ?3, completed_at = ?4,
                    state = CASE WHEN possibly_delivered = 1 THEN 'ambiguous' ELSE 'failed' END
                 WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'in_flight'",
                params![ids, status, error, now],
            )?;
        }
    }
    Ok(())
}

fn coverage_transaction(
    transaction: &Transaction<'_>,
    config: &Config,
    now: i64,
    started_at: i64,
) -> Result<Vec<CoverageChange>, StoreError> {
    let mut changes = Vec::new();
    for (source_id, source) in &config.sources {
        let Some(heartbeat) = &source.heartbeat else {
            continue;
        };
        let stale_after = i64::try_from(heartbeat.stale_after.as_millis()).unwrap_or(i64::MAX);
        let (last, previous): (Option<i64>, String) = transaction
            .query_row(
                "SELECT last_heartbeat_at, coverage FROM source_status WHERE source = ?1",
                [source_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .unwrap_or((None, "unknown".to_owned()));
        let state = match last {
            None if previous == "never_seen" || now - started_at >= stale_after => "never_seen",
            None => "unknown",
            Some(at) if now - at <= stale_after => "healthy",
            Some(_) => "stale",
        };
        if state == previous {
            continue;
        }
        transaction.execute(
            "INSERT INTO source_status(source, coverage, coverage_changed_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(source) DO UPDATE SET coverage = excluded.coverage,
                coverage_changed_at = excluded.coverage_changed_at",
            params![source_id, state, now],
        )?;
        let message = match (previous.as_str(), state) {
            ("stale" | "never_seen", "healthy") => Some(render::coverage_recovered(source_id)),
            (_, "stale") => Some(render::coverage_stale(
                source_id,
                Duration::from_millis(u64::try_from(now - last.unwrap_or(now)).unwrap_or(0)),
            )),
            (_, "never_seen") => Some(render::coverage_never_seen(
                source_id,
                heartbeat.stale_after,
            )),
            _ => None,
        };
        let notified = match message {
            Some(content) => {
                let intent = NewIntent {
                    occurrence: None,
                    source: source_id,
                    destination: &heartbeat.destination,
                    kind: "coverage",
                    action: heartbeat.action,
                    content,
                };
                insert_intent(transaction, config.mode, &intent, now)?;
                true
            }
            None => false,
        };
        changes.push(CoverageChange {
            source: source_id.clone(),
            state,
            notified,
        });
    }
    Ok(changes)
}

fn stats(connection: &Connection, page_size: i64, max_pages: i64) -> Result<Stats, StoreError> {
    let (pages, free): (i64, i64) = connection.query_row(
        "SELECT (SELECT page_count FROM pragma_page_count()),
                (SELECT freelist_count FROM pragma_freelist_count())",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let pending = connection
        .prepare_cached(
            "SELECT destination, count(*), min(created_at) FROM outbox
             WHERE state IN ('pending', 'in_flight') GROUP BY destination",
        )?
        .query_map([], |row| {
            Ok(PendingDeliveries {
                destination: row.get(0)?,
                count: row.get(1)?,
                oldest_created_at: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let assessment_queue = connection.query_row(
        "SELECT count(*) FROM assessments WHERE state IN ('pending', 'in_flight')",
        [],
        |row| row.get(0),
    )?;
    let sources = connection
        .prepare_cached(
            "SELECT source, last_event_at, last_heartbeat_at, coverage FROM source_status",
        )?
        .query_map([], |row| {
            Ok(SourceFreshness {
                source: row.get(0)?,
                last_event_at: row.get(1)?,
                last_heartbeat_at: row.get(2)?,
                coverage: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Stats {
        used_bytes: (pages - free) * page_size,
        limit_bytes: max_pages * page_size,
        pending,
        assessment_queue,
        sources,
    })
}

#[cfg(test)]
mod tests;

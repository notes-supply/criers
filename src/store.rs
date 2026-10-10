use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::model::{Action, Alert, Status, Webhook};

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("database worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error("database lock poisoned")]
    Poisoned,
    #[error("event encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("database admission capacity is occupied")]
    Busy,
}

#[derive(Clone)]
pub(crate) struct Store {
    connection: Arc<Mutex<Connection>>,
    admission: Arc<Semaphore>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Receipt {
    pub event_id: i64,
    pub mode: &'static str,
    pub coverage_gap: bool,
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
    pub mode: &'static str,
    pub events: i64,
    pub occurrences: i64,
    pub firing: i64,
    pub shadow_intents: i64,
    pub last_event_at: Option<String>,
    pub truncated_alerts: Option<i64>,
    pub source_coverage: &'static str,
}

impl Store {
    pub async fn open(path: PathBuf) -> Result<Self, StoreError> {
        let connection = tokio::task::spawn_blocking(move || {
            let connection = Connection::open(path)?;
            connection.busy_timeout(Duration::from_secs(5))?;
            connection.execute_batch(include_str!("schema.sql"))?;
            Ok::<_, rusqlite::Error>(connection)
        })
        .await??;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            admission: Arc::new(Semaphore::new(8)),
        })
    }

    pub async fn with_connection<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let database = Arc::clone(&self.connection);
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| StoreError::Busy)?;
        tokio::task::spawn_blocking(move || {
            // A canceled HTTP request must not release a still-running database job's slot.
            let _permit = permit;
            let mut connection = database.lock().map_err(|_| StoreError::Poisoned)?;
            operation(&mut connection)
        })
        .await?
    }

    pub async fn ingest(&self, source: String, webhook: Webhook) -> Result<Receipt, StoreError> {
        self.with_connection(move |connection| {
            let received_at = Utc::now().to_rfc3339();
            let payload = serde_json::to_string(&webhook)?;
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO events(source, received_at, payload) VALUES (?1, ?2, ?3)",
                params![source, received_at, payload],
            )?;
            let event_id = transaction.last_insert_rowid();
            let mut decisions = Vec::with_capacity(webhook.alerts.len());
            for alert in &webhook.alerts {
                let decision = process_alert(&transaction, &source, alert, &received_at)?;
                transaction.execute(
                    "INSERT INTO decisions(event_id, occurrence_id, action, reason, policy_version, intent_created)
                     VALUES (?1, ?2, ?3, ?4, 'deterministic-v1', ?5)",
                    params![event_id, decision.occurrence_id, decision.action.as_str(), decision.reason, decision.intent_created],
                )?;
                decisions.push(decision);
            }
            transaction.execute(
                "INSERT INTO source_status(source, last_event_at, truncated_alerts) VALUES (?1, ?2, ?3)
                 ON CONFLICT(source) DO UPDATE SET last_event_at = excluded.last_event_at,
                 truncated_alerts = excluded.truncated_alerts",
                params![source, received_at, webhook.truncated_alerts],
            )?;
            transaction.commit()?;
            Ok(Receipt { event_id, mode: "shadow", coverage_gap: webhook.truncated_alerts > 0, decisions })
        }).await
    }

    pub async fn snapshot(&self, source: String) -> Result<Snapshot, StoreError> {
        self.with_connection(move |connection| {
            let freshness = connection
                .query_row(
                    "SELECT last_event_at, truncated_alerts FROM source_status WHERE source = ?1",
                    [source.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            let count = |query| connection.query_row(query, [source.as_str()], |row| row.get(0));
            Ok(Snapshot {
                mode: "shadow",
                events: count("SELECT count(*) FROM events WHERE source = ?1")?,
                occurrences: count("SELECT count(*) FROM occurrences WHERE source = ?1")?,
                firing: count("SELECT count(*) FROM occurrences o WHERE source = ?1 AND status = 'firing'
                    AND NOT EXISTS (SELECT 1 FROM occurrences newer WHERE newer.source = o.source
                    AND newer.fingerprint = o.fingerprint AND newer.starts_at > o.starts_at)")?,
                shadow_intents: count("SELECT count(*) FROM outbox b JOIN occurrences o ON o.id = b.occurrence_id WHERE o.source = ?1")?,
                last_event_at: freshness.as_ref().map(|(timestamp, _)| timestamp.clone()),
                truncated_alerts: freshness.map(|(_, count)| count),
                source_coverage: "unknown_without_independent_heartbeat",
            })
        })
        .await
    }
}

fn process_alert(
    transaction: &Transaction<'_>,
    source: &str,
    alert: &Alert,
    received_at: &str,
) -> Result<Decision, StoreError> {
    let starts_at = alert.start_key();
    let fingerprint = alert.fingerprint.to_ascii_lowercase();
    let latest: Option<String> = transaction.query_row(
        "SELECT max(starts_at) FROM occurrences WHERE source = ?1 AND fingerprint = ?2",
        params![source, fingerprint],
        |row| row.get(0),
    )?;
    let stale = latest.as_ref().is_some_and(|latest| latest > &starts_at);
    transaction.execute(
        "INSERT INTO occurrences(source, fingerprint, starts_at, status, highest_action)
         VALUES (?1, ?2, ?3, 'firing', 0) ON CONFLICT DO NOTHING",
        params![source, fingerprint, starts_at],
    )?;
    let (id, status, highest): (i64, String, u8) = transaction.query_row(
        "SELECT id, status, highest_action FROM occurrences WHERE source = ?1 AND fingerprint = ?2 AND starts_at = ?3",
        params![source, fingerprint, starts_at],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let (action, reason, kind) = if alert.status == Status::Resolved {
        transaction.execute(
            "UPDATE occurrences SET status = 'resolved' WHERE id = ?1",
            [id],
        )?;
        if stale {
            (Action::Record, "superseded_occurrence", None)
        } else if status == "resolved" {
            (Action::Record, "already_resolved", None)
        } else if highest > 0 {
            (Action::QuietPost, "recovery", Some("recovery"))
        } else {
            (Action::Record, "resolution_without_notification", None)
        }
    } else if stale {
        (Action::Record, "superseded_occurrence", None)
    } else if status == "resolved" {
        (Action::Record, "late_firing_after_resolution", None)
    } else {
        let (action, reason) = alert.policy();
        if action.rank() > highest {
            transaction.execute(
                "UPDATE occurrences SET highest_action = ?1 WHERE id = ?2",
                params![action.rank(), id],
            )?;
            (action, reason, Some(action.as_str()))
        } else {
            (Action::Record, "unchanged_or_lower_priority", None)
        }
    };
    let intent_created = if let Some(kind) = kind {
        transaction.execute(
            "INSERT INTO outbox(occurrence_id, kind, action, mode, created_at) VALUES (?1, ?2, ?3, 'shadow', ?4)
             ON CONFLICT DO NOTHING",
            params![id, kind, action.as_str(), received_at],
        )? > 0
    } else {
        false
    };
    Ok(Decision {
        occurrence_id: id,
        action,
        reason,
        intent_created,
    })
}

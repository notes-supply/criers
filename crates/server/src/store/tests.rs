//! Storage failure, retention, migration, ownership and coverage behavior on real SQLite.

use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::json;

use super::{Claim, ClaimLimits, Store, StoreError};
use crate::tests::{Harness, TOKEN, fixture, get, post_to, with};

const HOUR: i64 = 3_600_000;

#[tokio::test]
async fn read_only_database_returns_retryable_failure_and_fails_readiness() {
    let harness = Harness::shadow("").await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute_batch("PRAGMA query_only = ON")?;
            Ok(())
        })
        .await
        .unwrap();
    let (status, _) = post_to(harness.app(), "example", &fixture(), TOKEN).await;
    let ready = get(harness.ops(), "/health/ready").await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (status, ready, snapshot.events, snapshot.intents),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
            0,
            0
        )
    );
}

#[tokio::test]
async fn probe_detects_read_only_storage_without_ingestion_traffic() {
    let harness = Harness::shadow("").await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute_batch("PRAGMA query_only = ON")?;
            Ok(())
        })
        .await
        .unwrap();
    let probe = harness.store.probe().await;
    let ready = get(harness.ops(), "/health/ready").await;
    assert_eq!(
        (probe.is_err(), ready),
        (true, StatusCode::SERVICE_UNAVAILABLE)
    );
}

async fn cap_at_current_size(store: &Store) {
    store
        .with_connection(|connection| {
            let pages: u32 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
            connection.pragma_update(None, "max_page_count", pages)?;
            Ok(())
        })
        .await
        .unwrap();
}

fn bulky() -> serde_json::Value {
    let mut full = fixture();
    // Payloads are not retained, so occupy space through stored labels.
    let labels = full["alerts"][0]["labels"].as_object_mut().unwrap();
    for index in 0..32 {
        labels.insert(format!("detail{index}"), json!("x".repeat(8192)));
    }
    full
}

#[tokio::test]
async fn full_database_rolls_back_event_decision_and_intent_together() {
    let harness = Harness::shadow("").await;
    cap_at_current_size(&harness.store).await;
    let (status, _) = post_to(harness.app(), "example", &bulky(), TOKEN).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (
            status,
            snapshot.events,
            snapshot.occurrences,
            snapshot.intents
        ),
        (StatusCode::SERVICE_UNAVAILABLE, 0, 0, 0)
    );
}

#[tokio::test]
async fn retention_frees_a_full_database_so_ingestion_and_readiness_recover() {
    let harness = Harness::shadow("").await;
    for index in 0..40 {
        let resolved = with(
            bulky(),
            "/alerts/0/fingerprint",
            json!(format!("{index:016x}")),
        );
        harness.ingest(resolved.clone()).await;
        harness
            .ingest(with(resolved, "/alerts/0/status", json!("resolved")))
            .await;
    }
    cap_at_current_size(&harness.store).await;
    let filler = with(bulky(), "/alerts/0/fingerprint", json!("ffffffffffffffff"));
    let (full, _) = post_to(harness.app(), "example", &filler, TOKEN).await;
    let unready = get(harness.ops(), "/health/ready").await;
    let deleted = harness
        .store
        .retention_step(crate::now_ms() + HOUR, 1000)
        .await
        .unwrap();
    let (recovered, _) = post_to(harness.app(), "example", &filler, TOKEN).await;
    let ready = get(harness.ops(), "/health/ready").await;
    assert_eq!(
        (full, unready, deleted.occurrences, recovered, ready),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::SERVICE_UNAVAILABLE,
            40,
            StatusCode::ACCEPTED,
            StatusCode::NO_CONTENT
        )
    );
}

#[tokio::test]
async fn retention_keeps_unresolved_occurrences_and_open_deliveries() {
    let harness = Harness::live("http://127.0.0.1:9/api/webhooks/123456789012345678/x", "").await;
    harness.ingest(fixture()).await;
    let resolved = with(
        fixture(),
        "/alerts/0/fingerprint",
        json!("fedcba9876543210"),
    );
    harness.ingest(resolved.clone()).await;
    harness
        .ingest(with(resolved, "/alerts/0/status", json!("resolved")))
        .await;
    let deleted = harness
        .store
        .retention_step(crate::now_ms() + HOUR, 1000)
        .await
        .unwrap();
    let remaining: i64 = harness
        .query("SELECT count(*) FROM occurrences WHERE status = 'firing'")
        .await;
    let open: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE state = 'pending'")
        .await;
    assert_eq!((deleted.occurrences, remaining, open), (1, 1, 1));
}

#[tokio::test]
async fn retention_deletes_in_bounded_batches() {
    let harness = Harness::shadow("").await;
    for index in 0..5 {
        harness
            .ingest(with(
                fixture(),
                "/alerts/0/fingerprint",
                json!(format!("{index:016x}")),
            ))
            .await;
    }
    let first = harness
        .store
        .retention_step(crate::now_ms() + HOUR, 2)
        .await
        .unwrap();
    let events: i64 = harness.query("SELECT count(*) FROM events").await;
    assert_eq!((first.decisions, first.events, events), (2, 2, 3));
}

#[tokio::test]
async fn second_process_cannot_open_the_same_database() {
    let harness = Harness::shadow("").await;
    let second = Store::open(harness.path.clone(), harness.config.storage.max_bytes).await;
    assert!(
        matches!(second, Err(StoreError::Locked(_))),
        "{:?}",
        second.err()
    );
}

#[tokio::test]
async fn scaffold_database_migrates_and_keeps_shadow_intents_shadow() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alerts.sqlite");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("scaffold_v0.sql"))
            .unwrap();
        let payload = fixture().to_string();
        connection
            .execute_batch(&format!(
                "INSERT INTO events(id, source, received_at, payload)
                 VALUES (1, 'example', '2026-10-10T10:00:01.5+00:00', '{payload}');
                 INSERT INTO occurrences(id, source, fingerprint, starts_at, status, highest_action)
                 VALUES (1, 'example', '0123456789abcdef', '2026-10-10T10:00:00.000000000Z', 'firing', 2),
                        (2, 'example', '0123456789abcdef', '2026-10-09T10:00:00.000000000Z', 'firing', 2);
                 INSERT INTO decisions(event_id, occurrence_id, action, reason, policy_version, intent_created)
                 VALUES (1, 1, 'ping', 'trusted_critical_floor', 'deterministic-v1', 1);
                 INSERT INTO outbox(occurrence_id, kind, action, mode, created_at)
                 VALUES (1, 'ping', 'ping', 'shadow', '2026-10-10T10:00:01.5+00:00');
                 INSERT INTO source_status(source, last_event_at, truncated_alerts)
                 VALUES ('example', '2026-10-10T10:00:01.5+00:00', 0);"
            ))
            .unwrap();
    }
    let harness = Harness::live("http://127.0.0.1:9/api/webhooks/123456789012345678/x", "").await;
    drop(harness.state);
    drop(harness.store);
    std::fs::copy(&path, &harness.path).unwrap();
    let store = Store::open(harness.path.clone(), harness.config.storage.max_bytes)
        .await
        .unwrap();
    let webhook: criers_core::Webhook = serde_json::from_value(fixture()).unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    let receipt = store
        .ingest(
            Arc::clone(&harness.config),
            "example".into(),
            webhook,
            permit,
        )
        .await
        .unwrap();
    let (state, alertname, received): (String, String, i64) = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT b.state, o.alertname, e.received_at FROM outbox b
                 JOIN occurrences o ON o.id = b.occurrence_id JOIN events e ON e.id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?)
        })
        .await
        .unwrap();
    let older: String = store
        .with_connection(|connection| {
            Ok(
                connection.query_row("SELECT status FROM occurrences WHERE id = 2", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    let claim = store
        .claim_deliveries(
            "ops".into(),
            crate::now_ms(),
            ClaimLimits {
                max_rows: 10,
                budget_chars: 1900,
                mention_cooldown_ms: 0,
            },
        )
        .await
        .unwrap();
    assert!(
        !receipt.decisions[0].intent_created
            && state == "shadow"
            && alertname == "ExampleStorageFailure"
            && received == 1_791_626_401_500
            && older == "superseded"
            && matches!(claim, Claim::Wait(None)),
        "state={state} alertname={alertname} received={received} older={older} claim={claim:?}"
    );
}

#[tokio::test]
async fn coverage_moves_from_never_seen_to_recovered_to_stale() {
    let harness = Harness::live(
        "http://127.0.0.1:9/api/webhooks/123456789012345678/x",
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    let config = Arc::clone(&harness.config);
    let started = crate::now_ms();
    let early = harness
        .store
        .coverage_tick(Arc::clone(&config), started + 1000, started)
        .await
        .unwrap();
    let never = harness
        .store
        .coverage_tick(Arc::clone(&config), started + 61_000, started)
        .await
        .unwrap();
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/alertname",
            json!("Watchdog"),
        ))
        .await;
    let now = crate::now_ms();
    let recovered = harness
        .store
        .coverage_tick(Arc::clone(&config), now, started)
        .await
        .unwrap();
    let stale = harness
        .store
        .coverage_tick(Arc::clone(&config), now + 120_000, started)
        .await
        .unwrap();
    let repeat = harness
        .store
        .coverage_tick(Arc::clone(&config), now + 130_000, started)
        .await
        .unwrap();
    let notifications: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE kind = 'coverage' AND state = 'pending'")
        .await;
    let states: Vec<&str> = [&early, &never, &recovered, &stale, &repeat]
        .iter()
        .flat_map(|changes| changes.iter().map(|change| change.state))
        .collect();
    assert_eq!(
        (states, notifications),
        (vec!["never_seen", "healthy", "stale"], 3)
    );
}

const DEAD_DISCORD: &str = "http://127.0.0.1:9/api/webhooks/123456789012345678/x";
const LIMITS: ClaimLimits = ClaimLimits {
    max_rows: 10,
    budget_chars: 1900,
    mention_cooldown_ms: 0,
};

async fn claim_and_complete(harness: &Harness, completion: super::Completion) {
    let Claim::Batch(batch) = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap()
    else {
        panic!("expected a batch");
    };
    harness
        .store
        .complete_deliveries("ops".into(), batch.ids, completion, 3)
        .await
        .unwrap();
}

#[tokio::test]
async fn rows_blocked_behind_an_earlier_send_do_not_cause_an_immediate_reclaim() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/severity",
            json!("warning"),
        ))
        .await;
    let retry_at = crate::now_ms() + HOUR;
    claim_and_complete(
        &harness,
        super::Completion::Retry {
            retry_at,
            status: Some(502),
            error: "HTTP 502".into(),
            ambiguous: true,
        },
    )
    .await;
    harness.ingest(fixture()).await;
    let claim = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    assert!(
        matches!(claim, Claim::Wait(Some(at)) if at >= retry_at),
        "{claim:?}"
    );
}

#[tokio::test]
async fn no_op_writes_do_not_mask_a_full_database_in_readiness() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    cap_at_current_size(&harness.store).await;
    let (status, _) = post_to(harness.app(), "example", &bulky(), TOKEN).await;
    let claim = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await;
    let ready = get(harness.ops(), "/health/ready").await;
    assert_eq!(
        (status, claim.is_ok(), ready),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            StatusCode::SERVICE_UNAVAILABLE
        )
    );
}

#[tokio::test]
async fn retention_keeps_the_delivery_record_of_a_still_firing_alert() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    claim_and_complete(
        &harness,
        super::Completion::Sent {
            remote_id: Some("1".into()),
            status: 200,
            mentioned: true,
        },
    )
    .await;
    harness
        .store
        .retention_step(crate::now_ms() + HOUR, 1000)
        .await
        .unwrap();
    let receipt = harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    assert_eq!(
        (
            receipt.decisions[0].reason,
            receipt.decisions[0].intent_created
        ),
        ("recovery", true)
    );
}

#[tokio::test]
async fn open_deliveries_for_removed_destinations_are_ended() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness
        .ingest(with(fixture(), "/receiver", json!("quiet")))
        .await;
    let abandoned = harness
        .store
        .abandon_unconfigured(vec!["ops".into()])
        .await
        .unwrap();
    let state: String = harness.query("SELECT state FROM outbox").await;
    assert_eq!((abandoned, state.as_str()), (1, "failed"));
}

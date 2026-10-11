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
    claim_and_complete_at(harness, "ops", completion).await;
}

async fn claim_and_complete_at(
    harness: &Harness,
    destination: &str,
    completion: super::Completion,
) {
    let Claim::Batch(batch) = harness
        .store
        .claim_deliveries(destination.into(), crate::now_ms(), LIMITS)
        .await
        .unwrap()
    else {
        panic!("expected a batch");
    };
    harness
        .store
        .complete_deliveries(destination.into(), batch.ids, batch.mention, completion, 3)
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

fn sent() -> super::Completion {
    super::Completion::Sent {
        remote_id: Some("1".into()),
        status: 200,
    }
}

#[tokio::test]
async fn recovery_reaches_every_destination_that_was_notified() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness
        .ingest(with(warning, "/receiver", json!("quiet")))
        .await;
    claim_and_complete_at(&harness, "quiet", sent()).await;
    harness.ingest(fixture()).await;
    claim_and_complete_at(&harness, "ops", sent()).await;
    harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    let destinations: String = harness
        .query(
            "SELECT group_concat(destination) FROM (SELECT destination FROM outbox
             WHERE kind = 'recovery' ORDER BY destination)",
        )
        .await;
    assert_eq!(destinations, "ops,quiet");
}

#[tokio::test]
async fn retention_keeps_an_old_occurrence_whose_delivery_completed_recently() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    claim_and_complete(&harness, sent()).await;
    harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    claim_and_complete(&harness, sent()).await;
    let old = crate::now_ms() - 2 * HOUR;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "UPDATE occurrences SET closed_at = ?1, last_seen_at = ?1",
                [old],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let deleted = harness
        .store
        .retention_step(crate::now_ms() - HOUR, 1000)
        .await
        .unwrap();
    let deliveries: i64 = harness.query("SELECT count(*) FROM outbox").await;
    assert_eq!((deleted.occurrences, deliveries), (0, 2));
}

#[tokio::test]
async fn retention_keeps_an_old_occurrence_with_a_pending_assessment() {
    let harness = Harness::shadow(
        "[assessment]\naccount_id = \"0123456789abcdef0123456789abcdef\"\n\
         api_token_file = \"clef.token\"\nbase_url = \"http://127.0.0.1:9/client/v4\"\n",
    )
    .await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness.ingest(warning.clone()).await;
    harness
        .ingest(with(warning, "/alerts/0/status", json!("resolved")))
        .await;
    let old = crate::now_ms() - 2 * HOUR;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "UPDATE occurrences SET closed_at = ?1, last_seen_at = ?1",
                [old],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .store
        .retention_step(crate::now_ms() - HOUR, 1000)
        .await
        .unwrap();
    let assessments: i64 = harness.query("SELECT count(*) FROM assessments").await;
    assert_eq!(assessments, 1);
}

fn pager_extra() -> String {
    format!(
        "[destinations.pager]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n\
         mention_users = [\"{}\"]\nmention_cooldown = \"10m\"\n\
         [sources.paged]\ncredential_files = [\"third.token\"]\ndefault_destination = \"pager\"\n",
        crate::tests::USER_ID
    )
}

const PAGER_LIMITS: ClaimLimits = ClaimLimits {
    max_rows: 10,
    budget_chars: 1900,
    mention_cooldown_ms: 600_000,
};

async fn claim_pager(store: &Store) -> super::Batch {
    match store
        .claim_deliveries("pager".into(), crate::now_ms(), PAGER_LIMITS)
        .await
        .unwrap()
    {
        Claim::Batch(batch) => batch,
        Claim::Wait(until) => panic!("expected a batch, got Wait({until:?})"),
    }
}

fn critical(fingerprint: &str) -> serde_json::Value {
    with(fixture(), "/alerts/0/fingerprint", json!(fingerprint))
}

#[tokio::test]
async fn ambiguous_ping_consumes_the_mention_cooldown() {
    let harness = Harness::live(DEAD_DISCORD, &pager_extra()).await;
    harness
        .ingest_as("paged", critical("0000000000000001"))
        .await;
    let first = claim_pager(&harness.store).await;
    harness
        .store
        .complete_deliveries(
            "pager".into(),
            first.ids.clone(),
            first.mention,
            super::Completion::Retry {
                retry_at: crate::now_ms(),
                status: Some(504),
                error: "HTTP 504".into(),
                ambiguous: true,
            },
            3,
        )
        .await
        .unwrap();
    harness
        .ingest_as("paged", critical("0000000000000002"))
        .await;
    let second = claim_pager(&harness.store).await;
    assert_eq!(
        (
            first.mention_allowed(),
            second.ping,
            second.mention_allowed()
        ),
        (true, true, false)
    );
}

#[tokio::test]
async fn definitely_undelivered_ping_keeps_the_mention_available() {
    let harness = Harness::live(DEAD_DISCORD, &pager_extra()).await;
    harness
        .ingest_as("paged", critical("0000000000000001"))
        .await;
    let first = claim_pager(&harness.store).await;
    harness
        .store
        .complete_deliveries(
            "pager".into(),
            first.ids.clone(),
            first.mention,
            super::Completion::Retry {
                retry_at: crate::now_ms(),
                status: None,
                error: "connection refused".into(),
                ambiguous: false,
            },
            3,
        )
        .await
        .unwrap();
    let retry = claim_pager(&harness.store).await;
    assert_eq!(
        (first.mention_allowed(), retry.mention_allowed()),
        (true, true)
    );
}

#[tokio::test]
async fn ping_interrupted_by_a_crash_consumes_the_mention_cooldown() {
    let harness = Harness::live(DEAD_DISCORD, &pager_extra()).await;
    harness
        .ingest_as("paged", critical("0000000000000001"))
        .await;
    let first = claim_pager(&harness.store).await;
    let crate::tests::Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let reopened = Store::open(path, config.storage.max_bytes).await.unwrap();
    let retry = claim_pager(&reopened).await;
    assert_eq!(
        (first.mention_allowed(), retry.mention_allowed()),
        (true, false)
    );
}

#[tokio::test]
async fn coverage_recovery_waits_for_a_delayed_stale_notification() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    let config = Arc::clone(&harness.config);
    let now = crate::now_ms();
    let never = harness
        .store
        .coverage_tick(Arc::clone(&config), now, now - 61_000)
        .await
        .unwrap();
    let retry_at = crate::now_ms() + HOUR;
    claim_and_complete(
        &harness,
        super::Completion::Retry {
            retry_at,
            status: Some(503),
            error: "HTTP 503".into(),
            ambiguous: false,
        },
    )
    .await;
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/alertname",
            json!("Watchdog"),
        ))
        .await;
    let recovered = harness
        .store
        .coverage_tick(Arc::clone(&config), crate::now_ms(), now - 61_000)
        .await
        .unwrap();
    let claim = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    assert!(
        never.len() == 1
            && recovered.len() == 1
            && matches!(claim, Claim::Wait(Some(at)) if at >= retry_at),
        "never={never:?} recovered={recovered:?} claim={claim:?}"
    );
}

#[tokio::test]
async fn first_v1_database_upgrades_in_place_and_recovers_every_destination() {
    // Recreate a database written by the first schema-1 build: narrow intent index
    // and none of the later indexes, holding sent, pending and coverage state.
    let directory = tempfile::tempdir().unwrap();
    let old = directory.path().join("old-v1.sqlite");
    {
        let connection = rusqlite::Connection::open(&old).unwrap();
        connection
            .execute_batch(include_str!("schema_v1_initial.sql"))
            .unwrap();
        connection
            .execute_batch(
                "PRAGMA user_version = 1;
                 INSERT INTO events(id, source, received_at, receiver, group_key, alerts, truncated_alerts)
                 VALUES (1, 'example', 1000, 'router', 'g', 1, 0);
                 INSERT INTO occurrences(id, source, fingerprint, starts_at, status, highest_action,
                    alertname, severity, labels, summary, first_seen_at, last_seen_at, observations)
                 VALUES (1, 'example', '0123456789abcdef', '2026-10-10T10:00:00.000000000Z', 'firing', 2,
                    'ExampleStorageFailure', 'critical', '{}', '', 1000, 1000, 2),
                    (2, 'other', 'fedcba9876543210', '2026-10-10T10:00:00.000000000Z', 'firing', 1,
                    'Other', 'warning', '{}', '', 1000, 1000, 1);
                 INSERT INTO decisions(event_id, occurrence_id, action, reason, policy_version)
                 VALUES (1, 1, 'ping', 'trusted_critical_floor', 'policy-old');
                 INSERT INTO outbox(id, occurrence_id, source, destination, kind, action, mode, state,
                    content, created_at, due_at, attempts, remote_id, completed_at)
                 VALUES (1, 1, 'example', 'quiet', 'notify', 'quiet_post', 'live', 'sent', 'q', 1000, 1000, 1, '7', 1001),
                    (2, 1, 'example', 'ops', 'notify', 'ping', 'live', 'sent', 'p', 1002, 1002, 1, '8', 1003),
                    (3, 2, 'other', 'ops', 'notify', 'quiet_post', 'live', 'pending', 'w', 1004, 1004, 0, NULL, NULL);
                 INSERT INTO source_status(source, last_event_at, truncated_alerts, last_heartbeat_at, coverage)
                 VALUES ('example', 1000, 0, 900, 'healthy');",
            )
            .unwrap();
    }
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let crate::tests::Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    std::fs::copy(&old, &path).unwrap();
    let store = Store::open(path, config.storage.max_bytes).await.unwrap();
    let webhook: criers_core::Webhook =
        serde_json::from_value(with(fixture(), "/alerts/0/status", json!("resolved"))).unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    let receipt = store
        .ingest(Arc::clone(&config), "example".into(), webhook, permit)
        .await
        .unwrap();
    let (version, recoveries, kept, coverage): (i64, String, String, String) = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT user_version FROM pragma_user_version()),
                    (SELECT group_concat(destination) FROM (SELECT destination FROM outbox
                        WHERE kind = 'recovery' ORDER BY destination)),
                    (SELECT group_concat(id || ':' || source || ':' || state) FROM (SELECT * FROM outbox
                        WHERE id <= 3 ORDER BY id)),
                    (SELECT coverage FROM source_status WHERE source = 'example')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?)
        })
        .await
        .unwrap();
    let claim = store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    assert!(
        version == 2
            && receipt.decisions[0].reason == "recovery"
            && recoveries == "ops,quiet"
            && kept == "1:example:sent,2:example:sent,3:other:pending"
            && coverage == "healthy"
            && matches!(&claim, Claim::Batch(batch) if batch.ids.contains(&3)),
        "version={version} reason={} recoveries={recoveries} kept={kept} coverage={coverage} \
         claim={claim:?}",
        receipt.decisions[0].reason
    );
}

#[tokio::test]
async fn recovery_for_a_removed_destination_is_terminal_and_does_not_pin_retention() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness
        .ingest(with(warning.clone(), "/receiver", json!("quiet")))
        .await;
    claim_and_complete_at(&harness, "quiet", sent()).await;
    // Restart with the quiet destination and its receiver mapping removed.
    let config_path = harness.directory.path().join("criers.toml");
    let text = std::fs::read_to_string(&config_path)
        .unwrap()
        .replace("receivers = { quiet = \"quiet\" }\n", "")
        .replace(
            "[destinations.quiet]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n",
            "",
        );
    std::fs::write(&config_path, text).unwrap();
    let reduced = Arc::new(crate::config::Config::load(&config_path).unwrap());
    assert!(!reduced.destinations.contains_key("quiet"));
    let crate::tests::Harness {
        directory: _directory,
        path,
        store,
        state,
        ..
    } = harness;
    drop(state);
    drop(store);
    let store = Store::open(path, reduced.storage.max_bytes).await.unwrap();
    let abandoned = store
        .abandon_unconfigured(reduced.destinations.keys().cloned().collect())
        .await
        .unwrap();
    let webhook: criers_core::Webhook =
        serde_json::from_value(with(warning, "/alerts/0/status", json!("resolved"))).unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    store
        .ingest(Arc::clone(&reduced), "example".into(), webhook, permit)
        .await
        .unwrap();
    let recovery: String = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT destination || ':' || state || ':' || coalesce(last_error, '')
                 FROM outbox WHERE kind = 'recovery'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    let claim = store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    let early = store
        .retention_step(crate::now_ms() - HOUR, 1000)
        .await
        .unwrap();
    let later = store
        .retention_step(crate::now_ms() + HOUR, 1000)
        .await
        .unwrap();
    assert!(
        abandoned == 0
            && recovery == "quiet:failed:destination_not_configured"
            && matches!(claim, Claim::Wait(None))
            && early.occurrences == 0
            && later.occurrences == 1,
        "abandoned={abandoned} recovery={recovery} claim={claim:?} early={early:?} later={later:?}"
    );
}

#[tokio::test]
async fn databases_sharing_a_stem_have_separate_single_writer_locks() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite = directory.path().join("alerts.sqlite");
    let db = directory.path().join("alerts.db");
    let _first = Store::open(sqlite.clone(), 64 << 20).await.unwrap();
    let second = Store::open(db, 64 << 20).await;
    let same = Store::open(sqlite, 64 << 20).await;
    assert!(
        second.is_ok() && matches!(same, Err(StoreError::Locked(_))),
        "second={:?} same={:?}",
        second.err(),
        same.err()
    );
}

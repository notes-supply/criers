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
            |_| {},
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
    assert_eq!(
        (abandoned, state.as_str()),
        (
            vec![super::Ended {
                destination: "quiet".to_owned(),
                state: "failed"
            }],
            "failed"
        )
    );
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
    let retained = harness
        .store
        .retention_step(crate::now_ms() - HOUR, 1000)
        .await
        .unwrap();
    let assessments: i64 = harness.query("SELECT count(*) FROM assessments").await;
    assert_eq!((assessments, retained.assessments_closed), (1, 1));
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
        .ingest(
            Arc::clone(&config),
            "example".into(),
            webhook,
            permit,
            |_| {},
        )
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
        version == super::SCHEMA_VERSION
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
        .ingest(
            Arc::clone(&reduced),
            "example".into(),
            webhook,
            permit,
            |_| {},
        )
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
        abandoned.is_empty()
            && recovery == "quiet:failed:destination_not_configured"
            && matches!(claim, Claim::Wait(None))
            && early.occurrences == 0
            && later.occurrences == 1,
        "abandoned={abandoned:?} recovery={recovery} claim={claim:?} early={early:?} later={later:?}"
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

#[tokio::test]
async fn an_unavailable_destination_does_not_block_another_destinations_escalation() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness
        .ingest(with(warning, "/receiver", json!("quiet")))
        .await;
    // The quiet channel's send hangs in flight.
    let quiet = harness
        .store
        .claim_deliveries("quiet".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    harness.ingest(fixture()).await;
    let ops = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    assert!(
        matches!(quiet, Claim::Batch(_)) && matches!(&ops, Claim::Batch(batch) if batch.ping),
        "quiet={quiet:?} ops={ops:?}"
    );
}

#[tokio::test]
async fn recovery_for_a_healthy_destination_is_not_held_by_another_destinations_retry() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness
        .ingest(with(warning, "/receiver", json!("quiet")))
        .await;
    claim_and_complete_at(
        &harness,
        "quiet",
        super::Completion::Retry {
            retry_at: crate::now_ms() + HOUR,
            status: Some(504),
            error: "HTTP 504".into(),
            ambiguous: true,
        },
    )
    .await;
    harness.ingest(fixture()).await;
    claim_and_complete_at(&harness, "ops", sent()).await;
    harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    let ops = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    assert!(
        matches!(&ops, Claim::Batch(batch) if batch.lines[0].contains("**RESOLVED**")),
        "{ops:?}"
    );
}

fn heartbeat(alertname: &str) -> serde_json::Value {
    with(fixture(), "/alerts/0/labels/alertname", json!(alertname))
}

async fn reopen_with(
    harness: crate::tests::Harness,
    edit: impl FnOnce(String) -> String,
) -> (tempfile::TempDir, Arc<crate::config::Config>, Store) {
    let config_path = harness.directory.path().join("criers.toml");
    let text = edit(std::fs::read_to_string(&config_path).unwrap());
    std::fs::write(&config_path, text).unwrap();
    let config = Arc::new(crate::config::Config::load(&config_path).unwrap());
    let crate::tests::Harness {
        directory,
        path,
        store,
        state,
        ..
    } = harness;
    drop(state);
    drop(store);
    let store = Store::open(path, config.storage.max_bytes).await.unwrap();
    (directory, config, store)
}

async fn ingest_into(store: &Store, config: &Arc<crate::config::Config>, value: serde_json::Value) {
    let webhook: criers_core::Webhook = serde_json::from_value(value).unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    store
        .ingest(
            Arc::clone(config),
            "example".into(),
            webhook,
            permit,
            |_| {},
        )
        .await
        .unwrap();
}

fn states(changes: &[super::CoverageChange]) -> Vec<&'static str> {
    changes.iter().map(|change| change.state).collect()
}

#[tokio::test]
async fn changing_the_heartbeat_alertname_returns_coverage_to_startup_grace() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let now = crate::now_ms();
    let before = harness
        .store
        .coverage_tick(Arc::clone(&harness.config), now, now - 1000)
        .await
        .unwrap();
    let (_directory, config, store) = reopen_with(harness, |text| {
        text.replace(
            "stale_after = \"1m\"",
            "alertname = \"DeadMansSwitch\"\nstale_after = \"1m\"",
        )
    })
    .await;
    let started = crate::now_ms();
    let reset = store
        .coverage_tick(Arc::clone(&config), started, started)
        .await
        .unwrap();
    let missing = store
        .coverage_tick(Arc::clone(&config), started + 61_000, started)
        .await
        .unwrap();
    ingest_into(&store, &config, heartbeat("DeadMansSwitch")).await;
    let restored = store
        .coverage_tick(Arc::clone(&config), crate::now_ms(), started)
        .await
        .unwrap();
    assert_eq!(
        (
            states(&before),
            states(&reset),
            states(&missing),
            states(&restored)
        ),
        (
            vec!["healthy"],
            vec!["unknown"],
            vec!["never_seen"],
            vec!["healthy"]
        )
    );
}

#[tokio::test]
async fn unchanged_heartbeat_alertname_keeps_freshness_across_restart() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let now = crate::now_ms();
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), now, now - 1000)
        .await
        .unwrap();
    let (_directory, config, store) = reopen_with(harness, |text| text).await;
    let started = crate::now_ms();
    let after = store
        .coverage_tick(Arc::clone(&config), started, started)
        .await
        .unwrap();
    let coverage: String = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT coverage FROM source_status WHERE source = 'example'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!((states(&after), coverage.as_str()), (vec![], "healthy"));
}

#[tokio::test]
async fn schema_2_heartbeat_without_identity_upgrades_and_returns_to_startup_grace() {
    let directory = tempfile::tempdir().unwrap();
    let old = directory.path().join("v2.sqlite");
    let recent = crate::now_ms() - 1000;
    {
        let connection = rusqlite::Connection::open(&old).unwrap();
        connection
            .execute_batch(include_str!("schema_v2.sql"))
            .unwrap();
        connection
            .execute_batch(&format!(
                "PRAGMA user_version = 2;
                 INSERT INTO source_status(source, last_event_at, truncated_alerts,
                    last_heartbeat_at, coverage, coverage_changed_at)
                 VALUES ('example', {recent}, 0, {recent}, 'healthy', {recent});"
            ))
            .unwrap();
    }
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    let crate::tests::Harness {
        directory: _harness_directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    std::fs::copy(&old, &path).unwrap();
    let store = Store::open(path, config.storage.max_bytes).await.unwrap();
    let started = crate::now_ms();
    let changes = store
        .coverage_tick(Arc::clone(&config), started, started)
        .await
        .unwrap();
    let (version, event_kept): (i64, Option<i64>) = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT user_version FROM pragma_user_version()),
                    (SELECT last_event_at FROM source_status WHERE source = 'example')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        (version, states(&changes), event_kept),
        (super::SCHEMA_VERSION, vec!["unknown"], Some(recent))
    );
}

fn rate_limited(retry_at: i64, global: bool) -> super::Completion {
    super::Completion::RateLimited {
        retry_at,
        status: 429,
        global,
    }
}

async fn claim_at(store: &Store, destination: &str) -> Claim {
    store
        .claim_deliveries(destination.into(), crate::now_ms(), LIMITS)
        .await
        .unwrap()
}

#[tokio::test]
async fn global_rate_limit_pauses_every_destination_and_survives_restart() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    let warning = with(warning, "/alerts/0/fingerprint", json!("fedcba9876543210"));
    harness
        .ingest(with(warning, "/receiver", json!("quiet")))
        .await;
    let later = crate::now_ms() + HOUR;
    claim_and_complete_at(&harness, "ops", rate_limited(later, true)).await;
    let quiet = claim_at(&harness.store, "quiet").await;
    let crate::tests::Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let store = Store::open(path, config.storage.max_bytes).await.unwrap();
    let after_restart = claim_at(&store, "quiet").await;
    let throttled: i64 = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT throttled FROM outbox WHERE destination = 'ops'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(
        matches!(quiet, Claim::Wait(Some(at)) if at >= later)
            && matches!(after_restart, Claim::Wait(Some(at)) if at >= later)
            && throttled == 1,
        "quiet={quiet:?} after_restart={after_restart:?} throttled={throttled}"
    );
}

#[tokio::test]
async fn global_pause_keeps_the_longest_deadline() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    let later = crate::now_ms() + HOUR;
    claim_and_complete_at(&harness, "ops", rate_limited(later, true)).await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE outbox SET due_at = 0, state = 'pending' WHERE destination = 'ops'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Force a second claim past the pause to record an earlier global deadline.
    let earlier = crate::now_ms() + 1000;
    let ids: Vec<i64> = vec![1];
    harness
        .store
        .with_connection(|connection| {
            connection.execute("UPDATE outbox SET state = 'in_flight' WHERE id = 1", [])?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .store
        .complete_deliveries("ops".into(), ids, None, rate_limited(earlier, true), 3)
        .await
        .unwrap();
    let paused: String = harness
        .query("SELECT value FROM meta WHERE key = 'discord_paused_until'")
        .await;
    assert_eq!(paused, later.to_string());
}

#[tokio::test]
async fn local_rate_limit_pauses_only_its_destination() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    let warning = with(warning, "/alerts/0/fingerprint", json!("fedcba9876543210"));
    harness
        .ingest(with(warning, "/receiver", json!("quiet")))
        .await;
    let later = crate::now_ms() + HOUR;
    claim_and_complete_at(&harness, "ops", rate_limited(later, false)).await;
    let ops = claim_at(&harness.store, "ops").await;
    let quiet = claim_at(&harness.store, "quiet").await;
    assert!(
        matches!(ops, Claim::Wait(Some(at)) if at >= later) && matches!(quiet, Claim::Batch(_)),
        "ops={ops:?} quiet={quiet:?}"
    );
}

#[tokio::test]
async fn a_symlinked_database_path_shares_the_single_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let real = directory.path().join("criers.sqlite");
    let link = directory.path().join("alias.sqlite");
    let _first = Store::open(real.clone(), 64 << 20).await.unwrap();
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let second = Store::open(link, 64 << 20).await;
    assert!(
        matches!(second, Err(StoreError::Locked(_))),
        "{:?}",
        second.err()
    );
}

#[tokio::test]
async fn a_symlinked_directory_shares_the_single_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let alias = directory.path().join("alias");
    std::os::unix::fs::symlink(&data, &alias).unwrap();
    let _first = Store::open(data.join("criers.sqlite"), 64 << 20)
        .await
        .unwrap();
    let second = Store::open(alias.join("criers.sqlite"), 64 << 20).await;
    assert!(
        matches!(second, Err(StoreError::Locked(_))),
        "{:?}",
        second.err()
    );
}

#[tokio::test]
async fn a_hard_linked_database_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let real = directory.path().join("criers.sqlite");
    drop(Store::open(real.clone(), 64 << 20).await.unwrap());
    let link = directory.path().join("alias.sqlite");
    std::fs::hard_link(&real, &link).unwrap();
    let _first = Store::open(real, 64 << 20).await;
    let second = Store::open(link, 64 << 20).await;
    assert!(
        matches!(second, Err(StoreError::HardLinked(_))),
        "{:?}",
        second.err()
    );
}

#[tokio::test]
async fn retention_drains_decisions_before_deleting_their_occurrence() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    // Every late firing after resolution is recorded as its own decision.
    for _ in 0..5 {
        harness.ingest(fixture()).await;
    }
    let old = crate::now_ms() - 2 * HOUR;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute_batch(&format!(
                "UPDATE occurrences SET closed_at = {old}, last_seen_at = {old};
                 UPDATE events SET received_at = {old};
                 UPDATE outbox SET created_at = {old}, completed_at = {old};"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let total: i64 = harness.query("SELECT count(*) FROM decisions").await;
    let first = harness
        .store
        .retention_step(crate::now_ms() - HOUR, 2)
        .await
        .unwrap();
    let after_first: i64 = harness.query("SELECT count(*) FROM decisions").await;
    let mut steps = vec![(first.decisions, first.occurrences)];
    for _ in 0..10 {
        let step = harness
            .store
            .retention_step(crate::now_ms() - HOUR, 2)
            .await
            .unwrap();
        if step.decisions == 0 && step.occurrences == 0 && step.events == 0 {
            break;
        }
        steps.push((step.decisions, step.occurrences));
    }
    let occurrences: i64 = harness.query("SELECT count(*) FROM occurrences").await;
    let decided: usize = steps.iter().map(|(decisions, _)| decisions).sum();
    assert!(
        total > 2
            && first.occurrences == 0
            && after_first == total - 2
            && steps.iter().all(|(decisions, _)| *decisions <= 2)
            && decided as i64 == total
            && occurrences == 0,
        "total={total} first={first:?} after_first={after_first} steps={steps:?} occurrences={occurrences}"
    );
}

#[tokio::test]
async fn disabling_heartbeats_clears_persisted_coverage_at_startup() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let now = crate::now_ms();
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), now, now - 1000)
        .await
        .unwrap();
    let (_directory, config, store) = reopen_with(harness, |text| {
        text.replace("[sources.example.heartbeat]\nstale_after = \"1m\"\n", "")
    })
    .await;
    assert!(
        config
            .sources
            .values()
            .all(|source| source.heartbeat.is_none())
    );
    let cancel = tokio_util::sync::CancellationToken::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::tasks::coverage(
            store.clone(),
            Arc::clone(&config),
            Arc::new(tokio::sync::Notify::new()),
            crate::now_ms(),
            cancel,
        ),
    )
    .await
    .unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    let snapshot = store.snapshot("example".into(), permit).await.unwrap();
    let notices: i64 = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT count(*) FROM outbox WHERE kind = 'coverage'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        (
            snapshot.coverage.as_str(),
            snapshot.last_heartbeat_at,
            notices
        ),
        ("unknown", None, 0)
    );
}

#[tokio::test]
async fn escalation_at_one_destination_keeps_another_destinations_pending_post() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    harness
        .ingest(with(warning.clone(), "/receiver", json!("quiet")))
        .await;
    harness.ingest(fixture()).await;
    let after_ops_ping: String = harness
        .query(
            "SELECT group_concat(destination || ':' || action || ':' || state) FROM
                (SELECT * FROM outbox WHERE kind = 'notify' ORDER BY id)",
        )
        .await;
    harness
        .ingest(with(fixture(), "/receiver", json!("quiet")))
        .await;
    let after_quiet_ping: String = harness
        .query(
            "SELECT group_concat(destination || ':' || action || ':' || state) FROM
                (SELECT * FROM outbox WHERE kind = 'notify' ORDER BY id)",
        )
        .await;
    assert_eq!(
        (after_ops_ping.as_str(), after_quiet_ping.as_str()),
        (
            "quiet:quiet_post:pending,ops:ping:pending",
            "quiet:quiet_post:superseded,ops:ping:pending,quiet:ping:pending"
        )
    );
}

#[tokio::test]
async fn disabling_heartbeats_clears_stale_coverage_without_a_new_notice() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let now = crate::now_ms();
    let config = Arc::clone(&harness.config);
    harness
        .store
        .coverage_tick(Arc::clone(&config), now, now - 1000)
        .await
        .unwrap();
    let stale = harness
        .store
        .coverage_tick(Arc::clone(&config), now + 120_000, now - 1000)
        .await
        .unwrap();
    let notices_before: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE kind = 'coverage'")
        .await;
    let (_directory, config, store) = reopen_with(harness, |text| {
        text.replace("[sources.example.heartbeat]\nstale_after = \"1m\"\n", "")
    })
    .await;
    crate::tasks::coverage(
        store.clone(),
        Arc::clone(&config),
        Arc::new(tokio::sync::Notify::new()),
        crate::now_ms(),
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    let stats = store.probe().await.unwrap();
    let observed = stats
        .sources
        .iter()
        .find(|source| source.source == "example")
        .map(|source| (source.coverage.clone(), source.last_heartbeat_at));
    let notices_after: i64 = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT count(*) FROM outbox WHERE kind = 'coverage'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        (states(&stale), observed, notices_after - notices_before),
        (vec!["stale"], Some(("unknown".to_owned(), None)), 0)
    );
}

#[tokio::test]
async fn equal_priority_fanout_recovers_each_destination_it_reached() {
    let harness = Harness::live(DEAD_DISCORD, "").await;
    harness.ingest(fixture()).await;
    harness
        .ingest(with(fixture(), "/receiver", json!("quiet")))
        .await;
    claim_and_complete_at(&harness, "ops", sent()).await;
    claim_and_complete_at(&harness, "quiet", sent()).await;
    harness.ingest(fixture()).await;
    let repeats: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE kind = 'notify'")
        .await;
    harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    let recoveries: String = harness
        .query(
            "SELECT group_concat(destination) FROM (SELECT destination FROM outbox
             WHERE kind = 'recovery' ORDER BY destination)",
        )
        .await;
    assert_eq!((repeats, recoveries.as_str()), (2, "ops,quiet"));
}

async fn read_only(store: &Store, enabled: bool) {
    store
        .with_connection(move |connection| {
            connection.pragma_update(None, "query_only", enabled)?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_heartbeat_reconciliation_retries_until_it_succeeds() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let now = crate::now_ms();
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), now, now - 1000)
        .await
        .unwrap();
    let (_directory, config, store) = reopen_with(harness, |text| {
        text.replace("[sources.example.heartbeat]\nstale_after = \"1m\"\n", "")
    })
    .await;
    // The startup reconciliation's write fails transiently.
    read_only(&store, true).await;
    let task = tokio::spawn(crate::tasks::coverage(
        store.clone(),
        Arc::clone(&config),
        Arc::new(tokio::sync::Notify::new()),
        crate::now_ms(),
        tokio_util::sync::CancellationToken::new(),
    ));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let retired_while_failing = task.is_finished();
    read_only(&store, false).await;
    let finished = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .is_ok();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    let snapshot = store.snapshot("example".into(), permit).await.unwrap();
    assert_eq!(
        (
            retired_while_failing,
            finished,
            snapshot.coverage.as_str(),
            snapshot.last_heartbeat_at
        ),
        (false, true, "unknown", None)
    );
}

#[tokio::test]
async fn reconciled_heartbeat_removes_the_exported_heartbeat_age() {
    let harness = Harness::live(
        DEAD_DISCORD,
        "[sources.example.heartbeat]\nstale_after = \"1m\"\n",
    )
    .await;
    harness.ingest(heartbeat("Watchdog")).await;
    let (_directory, config, store) = reopen_with(harness, |text| {
        text.replace("[sources.example.heartbeat]\nstale_after = \"1m\"\n", "")
    })
    .await;
    let metrics = Arc::new(crate::metrics::Metrics::new());
    // One real probe-and-publish pass of the storage observer.
    let observe_once = || {
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        crate::tasks::observe(
            store.clone(),
            Arc::clone(&config),
            Arc::clone(&metrics),
            60_000,
            cancel,
        )
    };
    let series = r#"criers_source_heartbeat_age_seconds{source="example"}"#;
    let exported = |metrics: &crate::metrics::Metrics| {
        metrics
            .render()
            .unwrap()
            .lines()
            .any(|line| line.starts_with(series))
    };
    // Observation can run before startup reconciliation and export the old age.
    observe_once().await;
    let before = exported(&metrics);
    crate::tasks::coverage(
        store.clone(),
        Arc::clone(&config),
        Arc::new(tokio::sync::Notify::new()),
        crate::now_ms(),
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    observe_once().await;
    let after = exported(&metrics);
    let coverage_unknown = metrics
        .render()
        .unwrap()
        .lines()
        .any(|line| line == r#"criers_source_coverage{source="example",state="unknown"} 1"#);
    assert_eq!((before, after, coverage_unknown), (true, false, true));
}

#[tokio::test]
async fn schema_3_upgrade_keeps_history_deliveries_and_assessments_and_accepts_summaries() {
    let directory = tempfile::tempdir().unwrap();
    let old = directory.path().join("v3.sqlite");
    let now = crate::now_ms();
    {
        let connection = rusqlite::Connection::open(&old).unwrap();
        connection
            .execute_batch(include_str!("schema_v3.sql"))
            .unwrap();
        // Released ranks: 0 record, 1 quiet_post, 2 ping.
        connection
            .execute_batch(&format!(
                "PRAGMA user_version = 3;
                 INSERT INTO events(id, source, received_at, receiver, group_key, alerts,
                    truncated_alerts) VALUES (1, 'example', {now}, 'router', 'g', 3, 0);
                 INSERT INTO occurrences(id, source, fingerprint, starts_at, status, highest_action,
                    alertname, severity, labels, summary, first_seen_at, last_seen_at,
                    observations, closed_at)
                 VALUES (1, 'example', '1000000000000001', '2026-10-12T10:00:00.000000000Z',
                        'firing', 2, 'Paged', 'critical', '{{}}', '', {now}, {now}, 1, NULL),
                    (2, 'example', '1000000000000002', '2026-10-12T10:00:00.000000000Z',
                        'resolved', 1, 'Posted', 'warning', '{{}}', '', {now}, {now}, 2, {now}),
                    (3, 'example', '1000000000000003', '2026-10-12T10:00:00.000000000Z',
                        'firing', 0, 'Recorded', 'info', '{{}}', '', {now}, {now}, 1, NULL);
                 INSERT INTO decisions(event_id, occurrence_id, action, reason, policy_version,
                    intent_id)
                 VALUES (1, 1, 'ping', 'trusted_critical_floor', 'policy-old', 1),
                    (1, 2, 'quiet_post', 'configured_severity', 'policy-old', 2),
                    (1, 3, 'record', 'configured_severity', 'policy-old', NULL);
                 INSERT INTO outbox(id, occurrence_id, source, destination, kind, action, mode,
                    state, content, created_at, due_at, attempts, possibly_delivered, completed_at)
                 VALUES (1, 1, 'example', 'ops', 'notify', 'ping', 'live', 'pending', 'page',
                        {now}, {now}, 1, 1, NULL),
                    (2, 2, 'example', 'ops', 'notify', 'quiet_post', 'live', 'sent', 'post',
                        {now}, {now}, 1, 0, {now}),
                    (3, NULL, 'example', 'ops', 'coverage', 'quiet_post', 'shadow', 'shadow',
                        'coverage', {now}, {now}, 0, 0, {now});
                 INSERT INTO assessments(occurrence_id, state, context, context_sha256,
                    context_fields, policy_version, requested_model, due_at, created_at)
                 VALUES (1, 'pending', '{{}}', 'sha', '[]', 'v', 'clef-flash', {now}, {now});
                 INSERT INTO source_status(source, last_event_at, truncated_alerts, coverage,
                    heartbeat_alertname) VALUES ('example', {now}, 0, 'unknown', NULL);"
            ))
            .unwrap();
    }
    let harness = Harness::live(DEAD_DISCORD, &summary_at_noon()).await;
    let crate::tests::Harness {
        directory: _harness_directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    std::fs::copy(&old, &path).unwrap();
    let store = Store::open(path, config.storage.max_bytes).await.unwrap();
    let kept: (i64, String, String, String, i64) = store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT user_version FROM pragma_user_version()),
                    (SELECT group_concat(highest_action, ',') FROM
                        (SELECT highest_action FROM occurrences ORDER BY id)),
                    (SELECT group_concat(id || ':' || state || ':' || possibly_delivered, ',') FROM
                        (SELECT id, state, possibly_delivered FROM outbox ORDER BY id)),
                    (SELECT group_concat(action, ',') FROM
                        (SELECT action FROM decisions ORDER BY id)),
                    (SELECT count(*) FROM assessments WHERE state = 'pending')",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    let claim = store
        .claim_deliveries("ops".into(), crate::now_ms(), LIMITS)
        .await
        .unwrap();
    // The rebuilt outbox accepts summary rows; the released one rejected the kind.
    store
        .summary_tick(Arc::clone(&config), crate::now_ms())
        .await
        .unwrap();
    let queued = store
        .summary_tick(Arc::clone(&config), crate::now_ms() + 86_400_000)
        .await
        .unwrap();
    let Claim::Batch(batch) = claim else {
        panic!("the pending page was not claimable after the upgrade: {claim:?}");
    };
    assert_eq!(
        (
            kept,
            batch.ids,
            batch.possibly_duplicate,
            matches!(queued, Some(super::SummaryTick::Queued(_)))
        ),
        (
            (
                super::SCHEMA_VERSION,
                "3,2,0".to_owned(),
                "1:pending:1,2:sent:0,3:shadow:0".to_owned(),
                "ping,quiet_post,record".to_owned(),
                1
            ),
            vec![1],
            true,
            true
        )
    );
}

fn summary_at_noon() -> String {
    "[summary]\ndestination = \"ops\"\nat = \"12:00\"\n".to_owned()
}

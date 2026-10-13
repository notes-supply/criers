//! Scheduled summaries and digested warnings on real SQLite, delivered to a
//! labelled mock Discord fixture.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};

use super::{
    Harness,
    delivery::{content, spawn, wait_for_state, worker},
    fixture,
    fixtures::{MockProvider, Scripted},
    with,
};
use crate::{
    store::{Claim, ClaimLimits, Store, SummaryTick},
    summary::{DAY_MS, MAX_CHARS},
};

const HOUR: i64 = 3_600_000;

/// A summary whose current window started about twelve hours ago, so test
/// events at the real current time fall well inside it.
fn summary_section() -> String {
    let at = chrono::Utc::now() - chrono::Duration::hours(12);
    format!(
        "[summary]\ndestination = \"ops\"\nat = \"{}\"\n",
        at.format("%H:%M")
    )
}

fn digest_policy() -> String {
    format!(
        "{}[policy.severity]\ncritical = \"ping\"\nwarning = \"digest\"\ninfo = \"record\"\n",
        summary_section()
    )
}

fn alert(alertname: &str, severity: &str, fingerprint: &str, status: &str) -> Value {
    let value = with(
        fixture(),
        "/alerts/0/labels",
        json!({
            "alertname": alertname,
            "severity": severity,
            "namespace": "media",
            "pod": format!("{}-0", alertname.to_lowercase()),
        }),
    );
    let value = with(value, "/alerts/0/fingerprint", json!(fingerprint));
    with(value, "/alerts/0/status", json!(status))
}

async fn tick(harness: &Harness, now: i64) -> SummaryTick {
    harness
        .store
        .summary_tick(Arc::clone(&harness.config), now)
        .await
        .unwrap()
        .unwrap()
}

/// Starts reporting now and returns the first window end.
async fn start(harness: &Harness) -> i64 {
    match tick(harness, crate::now_ms()).await {
        SummaryTick::Started(end) => end,
        other => panic!("expected the first tick to start reporting, got {other:?}"),
    }
}

/// Queues a summary due right now: the cursor moves back one day so the window
/// that just ended is reported at the real current time.
async fn queue_now(harness: &Harness) -> i64 {
    start(harness).await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE summaries SET window_end = window_end - ?1",
                [DAY_MS],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    match tick(harness, crate::now_ms()).await {
        SummaryTick::Queued(_) => {}
        other => panic!("expected a queued summary, got {other:?}"),
    }
    harness
        .query("SELECT id FROM outbox WHERE kind = 'summary'")
        .await
}

async fn summaries(harness: &Harness) -> Vec<(String, String)> {
    harness
        .store
        .with_connection(|connection| {
            Ok(connection
                .prepare("SELECT state, content FROM outbox WHERE kind = 'summary' ORDER BY id")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn first_tick_starts_reporting_without_posting() {
    let harness = Harness::shadow(&summary_section()).await;
    harness
        .ingest(alert(
            "PodCrashLooping",
            "warning",
            "1000000000000001",
            "firing",
        ))
        .await;
    let end = start(&harness).await;
    let cursor: i64 = harness.query("SELECT max(window_end) FROM summaries").await;
    assert_eq!((cursor, summaries(&harness).await.len()), (end, 0));
}

#[tokio::test]
async fn ended_window_reports_open_new_and_resolved_alerts_separately() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    harness
        .ingest(alert(
            "PodCrashLooping",
            "warning",
            "1000000000000001",
            "firing",
        ))
        .await;
    harness
        .ingest(alert(
            "PodCrashLooping",
            "warning",
            "1000000000000002",
            "firing",
        ))
        .await;
    harness
        .ingest(alert("DiskFull", "critical", "2000000000000001", "firing"))
        .await;
    harness
        .ingest(alert(
            "DiskFull",
            "critical",
            "2000000000000001",
            "resolved",
        ))
        .await;
    harness
        .ingest(alert("Informational", "info", "3000000000000001", "firing"))
        .await;

    let queued = tick(&harness, end + DAY_MS).await;
    let reports = summaries(&harness).await;
    let text = &reports[0].1;
    assert_eq!(
        (
            queued,
            reports.len(),
            text.contains("**Open: 2** (2 new)"),
            text.contains("- example: PodCrashLooping [warning] x2, new: media/podcrashlooping-0"),
            text.contains("**Resolved: 1**"),
            text.contains("- example: DiskFull [critical]: media/diskfull-0 - Synthetic critical"),
            text.contains("Informational"),
        ),
        (
            SummaryTick::Queued(end + DAY_MS),
            1,
            true,
            true,
            true,
            true,
            false
        ),
        "{text}"
    );
}

#[tokio::test]
async fn heartbeats_are_coverage_evidence_not_alerts() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"1m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        !text.contains("Watchdog") && text.contains("**Open: 0**"),
        "{text}"
    );
}

#[tokio::test]
async fn heartbeat_gaps_inside_the_window_are_reported_with_their_span() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"1m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    let now = crate::now_ms();
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let config = Arc::clone(&harness.config);
    // Healthy now, stale two hours later, healthy again an hour after that.
    harness
        .store
        .coverage_tick(Arc::clone(&config), now, now)
        .await
        .unwrap();
    harness
        .store
        .coverage_tick(Arc::clone(&config), now + 2 * HOUR, now)
        .await
        .unwrap();
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "UPDATE source_status SET last_heartbeat_at = ?1",
                [now + 3 * HOUR],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .store
        .coverage_tick(Arc::clone(&config), now + 3 * HOUR, now)
        .await
        .unwrap();
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        text.contains("- example: heartbeat stale ") && text.contains("(1h)"),
        "{text}"
    );
}

#[tokio::test]
async fn a_repeated_tick_never_queues_the_same_window_twice() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    tick(&harness, end + DAY_MS).await;
    let again = tick(&harness, end + DAY_MS + 1000).await;
    assert_eq!(
        (again, summaries(&harness).await.len()),
        (SummaryTick::Current(end + DAY_MS), 1)
    );
}

#[tokio::test]
async fn downtime_over_several_days_queues_one_summary_since_the_last_window() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    harness
        .ingest(alert(
            "PodCrashLooping",
            "warning",
            "1000000000000001",
            "firing",
        ))
        .await;
    let after_downtime = end + 3 * DAY_MS + HOUR;
    let queued = tick(&harness, after_downtime).await;
    let (window_start, window_end): (i64, i64) = harness
        .store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT window_start, window_end FROM summaries ORDER BY window_end DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    let reports = summaries(&harness).await;
    assert_eq!(
        (
            queued,
            window_start,
            window_end,
            reports.len(),
            reports[0].1.contains("(delayed")
        ),
        (
            SummaryTick::Queued(end + 3 * DAY_MS),
            end,
            end + 3 * DAY_MS,
            1,
            true
        ),
        "{}",
        reports[0].1
    );
}

#[tokio::test]
async fn a_catch_up_window_never_starts_before_retained_history() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    let much_later = end + 90 * DAY_MS;
    tick(&harness, much_later).await;
    let window_start: i64 = harness
        .query("SELECT window_start FROM summaries ORDER BY window_end DESC LIMIT 1")
        .await;
    let retention = i64::try_from(harness.config.storage.retention.as_millis()).unwrap();
    assert_eq!(window_start, much_later - retention);
}

#[tokio::test]
async fn an_occurrence_starting_exactly_at_the_window_end_belongs_to_the_next_window() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Before", "warning", "1000000000000001", "firing"))
        .await;
    harness
        .ingest(alert("AtEnd", "warning", "1000000000000002", "firing"))
        .await;
    let next = end + DAY_MS;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "UPDATE occurrences SET first_seen_at = ?1 - 1 WHERE alertname = 'Before'",
                [next],
            )?;
            connection.execute(
                "UPDATE occurrences SET first_seen_at = ?1 WHERE alertname = 'AtEnd'",
                [next],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    tick(&harness, next).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(text.contains("Before") && !text.contains("AtEnd"), "{text}");
}

#[tokio::test]
async fn an_alert_resolved_after_the_window_end_was_open_at_that_end() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    harness
        .ingest(alert("LateFix", "warning", "1000000000000001", "firing"))
        .await;
    harness
        .ingest(alert("LateFix", "warning", "1000000000000001", "resolved"))
        .await;
    // Generated late: the resolution happened after this window's end.
    let window_end = end + DAY_MS;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "UPDATE occurrences SET first_seen_at = ?1 - 10, closed_at = ?1 + 10",
                [window_end],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    tick(&harness, window_end + 20).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        text.contains("**Open: 1**") && text.contains("**Resolved: 0**"),
        "{text}"
    );
}

#[tokio::test]
async fn shadow_summaries_stay_shadow_after_switching_to_live() {
    let shadow = Harness::shadow(&summary_section()).await;
    let end = start(&shadow).await;
    tick(&shadow, end + DAY_MS).await;
    let Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = shadow;
    drop(state);
    drop(store);
    let live = Store::open(path, config.storage.max_bytes).await.unwrap();
    let claim = live
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
    assert!(matches!(claim, Claim::Wait(None)), "{claim:?}");
}

#[tokio::test]
async fn live_summary_is_posted_alone_and_without_mentions() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), &digest_policy()).await;
    queue_now(&harness).await;
    // A page queued after the summary must not be folded into it.
    harness
        .ingest(with(
            alert("Escalated", "critical", "5000000000000001", "firing"),
            "/receiver",
            json!("router"),
        ))
        .await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    cancel.cancel();
    let summary = &requests[0];
    assert_eq!(
        (
            content(summary).starts_with("**Summary**"),
            content(summary).contains("Escalated"),
            summary.body["allowed_mentions"]["users"].clone(),
            content(&requests[1]).contains("**FIRING** Escalated"),
        ),
        (true, false, json!([]), true),
        "{requests:?}"
    );
}

#[tokio::test]
async fn rate_limited_summary_is_retried_after_the_limit() {
    let discord = MockProvider::discord(vec![
        Scripted::json(
            429,
            json!({"message": "limited", "retry_after": 0.3, "global": false}),
        )
        .with_header("retry-after", "0.3"),
    ])
    .await;
    let harness = Harness::live(&discord.discord_url(), &summary_section()).await;
    let id = queue_now(&harness).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, id, "sent").await;
    cancel.cancel();
    let requests = discord.requests();
    let gap = requests[1].at.duration_since(requests[0].at);
    assert!(
        state == "sent" && requests.len() == 2 && gap >= Duration::from_millis(300),
        "state={state} requests={} gap={gap:?}",
        requests.len()
    );
}

#[tokio::test]
async fn ambiguous_summary_send_is_retried_and_marked_as_a_possible_duplicate() {
    let discord = MockProvider::discord(vec![Scripted::json(504, json!({}))]).await;
    let harness = Harness::live(&discord.discord_url(), &summary_section()).await;
    let id = queue_now(&harness).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, id, "sent").await;
    cancel.cancel();
    let possibly: i64 = harness
        .query("SELECT possibly_delivered FROM outbox WHERE kind = 'summary'")
        .await;
    assert_eq!(
        (state.as_str(), possibly, discord.requests().len()),
        ("sent", 1, 2)
    );
}

#[tokio::test]
async fn queued_summary_survives_restart_and_is_not_generated_again() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), &summary_section()).await;
    queue_now(&harness).await;
    let cursor: i64 = harness.query("SELECT max(window_end) FROM summaries").await;
    let Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let reopened = Store::open(path, config.storage.max_bytes).await.unwrap();
    let again = reopened
        .summary_tick(Arc::clone(&config), crate::now_ms())
        .await
        .unwrap();
    let destination = &config.destinations["ops"];
    let cancel = spawn(crate::delivery::Worker {
        destination: "ops".into(),
        discord: crate::delivery::Discord::new(destination, Duration::from_secs(2)).unwrap(),
        mention_cooldown: destination.mention_cooldown,
        store: reopened.clone(),
        metrics: Arc::new(crate::metrics::Metrics::new()),
        wake: Arc::new(tokio::sync::Notify::new()),
        settings: config.delivery,
    });
    let requests = discord.wait_for(1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    assert_eq!(
        (
            again,
            requests.len(),
            discord.requests().len(),
            content(&requests[0]).starts_with("**Summary**")
        ),
        (Some(SummaryTick::Current(cursor)), 1, 1, true)
    );
}

#[tokio::test]
async fn full_storage_delays_the_summary_without_losing_its_window() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    // Cap the file, then fill the outbox's own pages: a summary is an outbox
    // row, so it needs space there rather than anywhere in the file.
    harness
        .store
        .with_connection(|connection| {
            let pages: u32 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
            connection.pragma_update(None, "max_page_count", pages)?;
            for size in [3000, 400, 40] {
                while connection
                    .execute(
                        "INSERT INTO outbox(source, destination, kind, action, mode, state,
                            content, created_at, due_at, completed_at)
                         VALUES ('filler', 'ops', 'coverage', 'quiet_post', 'shadow', 'shadow',
                            ?1, 0, 0, 0)",
                        [&"x".repeat(size)],
                    )
                    .is_ok()
                {}
            }
            Ok(())
        })
        .await
        .unwrap();
    let failed = harness
        .store
        .summary_tick(Arc::clone(&harness.config), end + DAY_MS)
        .await
        .is_err();
    let cursor_after_failure: i64 = harness.query("SELECT max(window_end) FROM summaries").await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute("DELETE FROM outbox WHERE source = 'filler'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let recovered = tick(&harness, end + DAY_MS + 60_000).await;
    assert_eq!(
        (failed, cursor_after_failure, recovered),
        (true, end, SummaryTick::Queued(end + DAY_MS))
    );
}

#[tokio::test]
async fn retention_keeps_the_cursor_and_a_window_still_being_reported() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Resolved", "warning", "1000000000000001", "firing"))
        .await;
    harness
        .ingest(alert("Resolved", "warning", "1000000000000001", "resolved"))
        .await;
    // Retention of everything older than the current window keeps this window's
    // closed occurrence and the cursor itself.
    harness.store.retention_step(end, 500).await.unwrap();
    tick(&harness, end + DAY_MS).await;
    let reported = summaries(&harness).await.remove(0).1;
    let later = harness
        .store
        .retention_step(end + 2 * DAY_MS, 500)
        .await
        .unwrap();
    let cursor: i64 = harness.query("SELECT max(window_end) FROM summaries").await;
    assert_eq!(
        (
            reported.contains("**Resolved: 1**"),
            later.summaries,
            cursor
        ),
        (true, 1, end + DAY_MS),
        "{reported}"
    );
}

#[tokio::test]
async fn digested_warning_is_recorded_once_without_an_immediate_post() {
    let harness = Harness::live(
        "https://discord.invalid/api/webhooks/123456789012345678/x",
        &digest_policy(),
    )
    .await;
    for _ in 0..3 {
        harness
            .ingest(alert(
                "PodCrashLooping",
                "warning",
                "1000000000000001",
                "firing",
            ))
            .await;
    }
    let decisions: i64 = harness.query("SELECT count(*) FROM decisions").await;
    let intents: i64 = harness.query("SELECT count(*) FROM outbox").await;
    let action: String = harness.query("SELECT action FROM decisions").await;
    assert_eq!((decisions, intents, action.as_str()), (1, 0, "digest"));
}

#[tokio::test]
async fn digested_warning_that_turns_critical_pings_immediately() {
    let harness = Harness::live(
        "https://discord.invalid/api/webhooks/123456789012345678/x",
        &digest_policy(),
    )
    .await;
    harness
        .ingest(alert(
            "DiskFilling",
            "warning",
            "1000000000000001",
            "firing",
        ))
        .await;
    harness
        .ingest(alert(
            "DiskFilling",
            "critical",
            "1000000000000001",
            "firing",
        ))
        .await;
    let (action, state): (String, String) = harness
        .store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT action, state FROM outbox WHERE kind = 'notify'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!((action.as_str(), state.as_str()), ("ping", "pending"));
}

#[tokio::test]
async fn digested_warning_resolution_is_reported_by_the_summary_not_a_recovery() {
    let harness = Harness::live(
        "https://discord.invalid/api/webhooks/123456789012345678/x",
        &digest_policy(),
    )
    .await;
    let end = start(&harness).await;
    harness
        .ingest(alert("JobFailed", "warning", "1000000000000001", "firing"))
        .await;
    harness
        .ingest(alert(
            "JobFailed",
            "warning",
            "1000000000000001",
            "resolved",
        ))
        .await;
    let recoveries: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE kind = 'recovery'")
        .await;
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        recoveries == 0 && text.contains("**Resolved: 1**") && text.contains("JobFailed"),
        "recoveries={recoveries}\n{text}"
    );
}

#[tokio::test]
async fn critical_routing_does_not_wait_for_a_pending_or_failing_summary() {
    let harness = Harness::live(
        "https://discord.invalid/api/webhooks/123456789012345678/x",
        &digest_policy(),
    )
    .await;
    let end = start(&harness).await;
    tick(&harness, end + DAY_MS).await;
    // The summary is permanently rejected; the critical page is claimed regardless.
    harness
        .store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE outbox SET state = 'failed', completed_at = 1 WHERE kind = 'summary'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .ingest(alert("NodeDown", "critical", "6000000000000001", "firing"))
        .await;
    let claim = harness
        .store
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
    let Claim::Batch(batch) = claim else {
        panic!("critical page was not claimable: {claim:?}");
    };
    assert!(
        batch.ping && batch.lines[0].contains("NodeDown"),
        "{batch:?}"
    );
}

#[tokio::test]
async fn the_largest_mention_prefix_still_leaves_room_for_a_summary() {
    let users: Vec<String> = (0..10).map(|index| format!("{:020}", index + 1)).collect();
    let roles: Vec<String> = (0..10).map(|index| format!("{:020}", index + 11)).collect();
    let harness = Harness::live(
        "https://discord.invalid/api/webhooks/123456789012345678/x",
        &format!(
            "[destinations.wide]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n\
             mention_users = {users:?}\nmention_roles = {roles:?}\n"
        ),
    )
    .await;
    let discord =
        crate::delivery::Discord::new(&harness.config.destinations["wide"], Duration::from_secs(1))
            .unwrap();
    assert!(
        discord.line_budget() >= MAX_CHARS,
        "{}",
        discord.line_budget()
    );
}

#[tokio::test]
async fn an_ongoing_gap_outlives_retention_of_the_transition_that_began_it() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"1m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    let now = crate::now_ms();
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let config = Arc::clone(&harness.config);
    harness
        .store
        .coverage_tick(Arc::clone(&config), now, now)
        .await
        .unwrap();
    harness
        .store
        .coverage_tick(Arc::clone(&config), now + 2 * HOUR, now)
        .await
        .unwrap();
    tick(&harness, end + DAY_MS).await;
    let removed = harness
        .store
        .retention_step(end + DAY_MS + 1, 500)
        .await
        .unwrap();
    tick(&harness, end + 2 * DAY_MS).await;
    // Retention also removed the first, already completed shadow summary.
    let later = summaries(&harness).await.pop().unwrap().1;
    assert!(
        removed.coverage == 2
            && later.contains("heartbeat stale since")
            && later.contains("ongoing"),
        "removed={removed:?}\n{later}"
    );
}

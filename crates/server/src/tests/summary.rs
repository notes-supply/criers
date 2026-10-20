//! Scheduled summaries and digested warnings on real SQLite, delivered to a
//! labelled mock Discord fixture.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};

use super::{
    Harness,
    delivery::{shown, spawn, wait_for_state, worker},
    fixture,
    fixtures::{MockProvider, Scripted},
    with,
};
use crate::{
    render::Part,
    store::{Claim, ClaimLimits, Store, SummaryTick},
    summary::DAY_MS,
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

/// Each summary's state and what a reader sees in it.
async fn summaries(harness: &Harness) -> Vec<(String, String)> {
    let rows: Vec<(String, String)> = harness
        .store
        .with_connection(|connection| {
            Ok(connection
                .prepare("SELECT state, content FROM outbox WHERE kind = 'summary' ORDER BY id")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?)
        })
        .await
        .unwrap();
    rows.into_iter()
        .map(|(state, content)| (state, Part::parse(content).text()))
        .collect()
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
            text.contains("**2 open** (2 warning) · 2 new · 1 recovered"),
            text.contains("example · **PodCrashLooping** ×2 · new · media/podcrashlooping-0"),
            text.contains("Recovered · 1"),
            text.contains(
                "example · **DiskFull** · critical · media/diskfull-0 — Synthetic critical"
            ),
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
        !text.contains("Watchdog")
            && text.contains("**No open alerts** · **coverage gaps**")
            && !text.contains("All clear"),
        "{text}"
    );
}

#[tokio::test]
async fn a_heartbeat_ending_a_gap_records_its_span() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"1m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    // The previous heartbeat arrived 1h 59m 30s ago; this one ends the gap. The gap
    // starts one minute after it, so it spans 1h 58m 30s plus however long this
    // ingest takes, which rounds up to 1h 59m for any delay from 0 ms up to 30 s.
    let before = crate::now_ms() - 2 * HOUR + 30_000;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE source_status SET last_heartbeat_at = ?1", [before])?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    heartbeats_until(&harness, end + DAY_MS - 30_000).await;
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        text.contains("example: no heartbeat ")
            && text.contains("(1h 59m)")
            && !text.contains("ongoing"),
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
            reports[0].1.contains("\nDelayed · generated <t:")
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
        text.contains("**1 open** (1 warning)") && !text.contains("Recovered"),
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
            shown(summary).starts_with("Daily summary"),
            shown(summary).contains("Escalated"),
            summary.body["allowed_mentions"]["users"].clone(),
            shown(&requests[1]).contains("**Critical** · firing"),
            shown(&requests[1]).contains("example · Escalated · 5000000000000001"),
        ),
        (true, false, json!([]), true, true),
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
            shown(&requests[0]).starts_with("Daily summary")
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
        (reported.contains("Recovered · 1"), later.summaries, cursor),
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
        recoveries == 0 && text.contains("Recovered · 1") && text.contains("JobFailed"),
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
        batch.ping && batch.parts[0].text().contains("NodeDown"),
        "{batch:?}"
    );
}

#[tokio::test]
async fn the_widest_mention_and_longest_headline_keep_content_within_discords_limit() {
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
    // A pinging card with the longest headline, then plain lines of an earlier
    // release filling the rest of the claim budget.
    let headline = "\u{1F534}".repeat(crate::render::HEADLINE_LIMIT / 2);
    let batch = crate::store::Batch {
        ids: vec![1, 2],
        parts: vec![
            Part::Card(crate::render::Card {
                headline: headline.clone(),
                embed: crate::render::Embed::default(),
            }),
            Part::Line("x".repeat(discord.line_budget() - 1)),
        ],
        headline: Some(headline),
        ping: true,
        mention: Some(crate::store::MentionClaim { at: 0, previous: 0 }),
        possibly_duplicate: false,
        max_failures: 0,
    };
    let rendered = discord.message(&batch);
    assert!(
        rendered.mention && crate::render::units(&rendered.content) <= crate::render::CONTENT_LIMIT,
        "{} units",
        crate::render::units(&rendered.content)
    );
}

#[tokio::test]
async fn an_ongoing_gap_is_reported_from_heartbeat_freshness_after_retention() {
    let (harness, end, last) = restarted_after_heartbeat_loss().await;
    tick(&harness, end + DAY_MS).await;
    let removed = harness
        .store
        .retention_step(end + DAY_MS + 1, 500)
        .await
        .unwrap();
    tick(&harness, end + 2 * DAY_MS).await;
    let later = summaries(&harness).await.pop().unwrap().1;
    let onset = crate::render::discord_time(last + 10 * 60_000, 'f');
    assert!(
        removed.summaries == 1
            && later.contains(&format!("heartbeat stale since {onset}, ongoing")),
        "removed={removed:?}\n{later}"
    );
}

#[tokio::test]
async fn a_revoked_webhook_fails_the_summary_without_marking_it_delivered() {
    let discord =
        MockProvider::start(Vec::new(), Scripted::json(404, json!({"code": 10015}))).await;
    let harness = Harness::live(&discord.discord_url(), &summary_section()).await;
    let id = queue_now(&harness).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, id, "failed").await;
    cancel.cancel();
    let stats = harness.store.probe().await.unwrap();
    let metrics = harness.state.metrics.render().unwrap();
    assert!(
        state == "failed"
            && stats.summary_delivered.is_none()
            && metrics.contains(
                r#"criers_deliveries_finished_total{destination="ops",state="failed"} 1"#
            ),
        "state={state} delivered={:?}\n{metrics}",
        stats.summary_delivered
    );
}

#[tokio::test]
async fn a_sent_summary_is_reported_as_delivered() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), &summary_section()).await;
    let id = queue_now(&harness).await;
    let window_end: i64 = harness.query("SELECT max(window_end) FROM summaries").await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, id, "sent").await;
    cancel.cancel();
    let stats = harness.store.probe().await.unwrap();
    let metrics = harness.state.metrics.render().unwrap();
    assert!(
        state == "sent"
            && stats.summary_delivered == Some(window_end)
            && metrics
                .contains(r#"criers_deliveries_finished_total{destination="ops",state="sent"} 1"#),
        "state={state} delivered={:?}\n{metrics}",
        stats.summary_delivered
    );
}

/// Stands in for heartbeats continuing every minute until `last`.
async fn heartbeats_until(harness: &Harness, last: i64) {
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE source_status SET last_heartbeat_at = ?1", [last])?;
            Ok(())
        })
        .await
        .unwrap();
}

/// A harness whose `example` heartbeat was last received three hours ago, just
/// before the daemon stopped, then reopened as if it restarted.
async fn restarted_after_heartbeat_loss() -> (Harness, i64, i64) {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let config = Arc::clone(&harness.config);
    let now = crate::now_ms();
    harness
        .store
        .coverage_tick(Arc::clone(&config), now, now)
        .await
        .unwrap();
    let last = now - 3 * HOUR;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE source_status SET last_heartbeat_at = ?1", [last])?;
            Ok(())
        })
        .await
        .unwrap();
    let Harness {
        directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let store = Store::open(path.clone(), config.storage.max_bytes)
        .await
        .unwrap();
    let mut harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    harness.directory = directory;
    harness.path = path;
    harness.config = config;
    harness.state.store = store.clone();
    harness.store = store;
    (harness, end, last)
}

#[tokio::test]
async fn restart_after_the_boundary_before_a_fresh_heartbeat_reports_the_outage() {
    let (harness, end, last) = restarted_after_heartbeat_loss().await;
    let after_boundary = end + DAY_MS + 60_000;
    // First evaluation after the restart happens after the missed window end.
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), after_boundary, after_boundary)
        .await
        .unwrap();
    tick(&harness, after_boundary).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    let onset = crate::render::discord_time(last + 10 * 60_000, 'f');
    assert!(
        text.contains(&format!("since {onset}, ongoing at window end")),
        "expected an outage from {onset}:\n{text}"
    );
}

#[tokio::test]
async fn first_heartbeat_after_downtime_does_not_erase_unobserved_summary_window() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    let started = end - DAY_MS;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE summaries SET window_end = ?1", [started])?;
            Ok(())
        })
        .await
        .unwrap();
    // The daemon stopped during startup grace, before its first heartbeat.
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), started, started)
        .await
        .unwrap();
    // Its first heartbeat arrives after the unobserved reporting window ended.
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, crate::now_ms()).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    // The gap began when the startup grace ran out and was open at the window end.
    let onset = crate::render::discord_time(started + 10 * 60_000, 'f');
    assert!(
        !text.contains("No heartbeat gaps")
            && text.contains("example")
            && text.contains(&format!(
                "example: no heartbeat recorded since {onset}, ongoing at window end"
            )),
        "the entire window lacked heartbeat evidence:\n{text}"
    );
}

#[tokio::test]
async fn first_heartbeat_after_schema_3_upgrade_keeps_the_unobserved_window() {
    let mut harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let now = crate::now_ms();
    let end = start(&harness).await;
    let started = end - DAY_MS;
    let upgraded_path = harness.directory.path().join("schema-3.sqlite");
    {
        let connection = rusqlite::Connection::open(&upgraded_path).unwrap();
        connection
            .execute_batch(include_str!("../store/schema_v3.sql"))
            .unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
        connection
            .execute(
                "INSERT INTO source_status(source, coverage, coverage_changed_at,
                    heartbeat_alertname) VALUES ('example', 'unknown', ?1, 'Watchdog')",
                [started],
            )
            .unwrap();
    }
    let migrated = Store::open(upgraded_path.clone(), harness.config.storage.max_bytes)
        .await
        .unwrap();
    harness.store = migrated.clone();
    harness.state.store = migrated;
    harness.path = upgraded_path;
    // Reporting begins after the native schema upgrade, before the downtime.
    assert_eq!(
        tick(&harness, now - DAY_MS).await,
        SummaryTick::Started(started)
    );
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), started, started)
        .await
        .unwrap();
    // The migrated source has never had a heartbeat. Its first one arrives
    // after a whole reporting window has elapsed without collection.
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, now).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    // Schema 3 kept when watching began for this still-unknown source, so the gap
    // starts at the end of its startup grace.
    assert!(
        !text.contains("No heartbeat gaps")
            && text.contains("example")
            && text.contains(&format!(
                "example: no heartbeat recorded since {}, ongoing at window end",
                minute(started + 10 * 60_000)
            )),
        "the upgraded source's unobserved window must not become healthy:\n{text}"
    );
}

#[tokio::test]
async fn late_never_seen_evaluation_does_not_erase_the_unobserved_summary_window() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    let started = end - DAY_MS;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE summaries SET window_end = ?1", [started])?;
            Ok(())
        })
        .await
        .unwrap();
    let config = Arc::clone(&harness.config);
    // Monitoring began at the window start; the daemon stopped during grace.
    harness
        .store
        .coverage_tick(Arc::clone(&config), started, started)
        .await
        .unwrap();
    // After the missed boundary a restarted daemon's own grace runs out first.
    let late = end + 20 * 60_000;
    let changes = harness
        .store
        .coverage_tick(Arc::clone(&config), late, late - 11 * 60_000)
        .await
        .unwrap();
    tick(&harness, late).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    let onset = crate::render::discord_time(started + 10 * 60_000, 'f');
    assert!(
        changes.iter().any(|change| change.state == "never_seen")
            && text.contains(&format!(
                "example: no heartbeat recorded since {onset}, ongoing at window end"
            )),
        "{changes:?}\n{text}"
    );
}

#[tokio::test]
async fn restart_after_the_boundary_after_a_fresh_heartbeat_reports_the_closed_outage() {
    let (harness, end, last) = restarted_after_heartbeat_loss().await;
    // The first thing after the restart is a fresh heartbeat, before any evaluation.
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let now = crate::now_ms();
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), now, now)
        .await
        .unwrap();
    heartbeats_until(&harness, end + DAY_MS - 30_000).await;
    tick(&harness, end + DAY_MS + 60_000).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    let onset = crate::render::discord_time(last + 10 * 60_000, 'f');
    assert!(
        text.contains(&format!("no heartbeat {onset} – ")) && !text.contains("ongoing"),
        "expected a closed outage from {onset}:\n{text}"
    );
}

#[tokio::test]
async fn counting_stops_at_the_cap_without_hiding_other_sources_or_the_end_state() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"1m\"\n\
         [sources.other.heartbeat]\nstale_after = \"1m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    harness
        .ingest_as(
            "other",
            alert("Watchdog", "none", "4000000000000002", "firing"),
        )
        .await;
    let base = end + HOUR;
    let cap = crate::store::MAX_GAPS;
    // One more closed gap for `example` than is counted, all before `other`'s gap.
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
                 INSERT INTO heartbeat_gaps(source, started_at, ended_at)
                 SELECT 'example', ?1 + i * 2, ?1 + i * 2 + 1 FROM n",
                [base, cap],
            )?;
            connection.execute(
                "INSERT INTO heartbeat_gaps(source, started_at, ended_at)
                 VALUES ('other', ?1, ?1 + 1800000)",
                [base + 4 * HOUR],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Both sources heartbeat again before the window end, so neither gap is open.
    let fresh = end + DAY_MS - 30_000;
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("UPDATE source_status SET last_heartbeat_at = ?1", [fresh])?;
            Ok(())
        })
        .await
        .unwrap();
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        text.contains(&format!("example: at least {cap} heartbeat gaps"))
            && text.contains("other: no heartbeat ")
            && text.contains("(30m)")
            && !text.contains("ongoing"),
        "{text}"
    );
}

#[tokio::test]
async fn thousands_of_alert_names_report_true_totals_from_a_bounded_read() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    let first_seen = end + HOUR;
    // 5,000 distinct names over two sources, plus one name with 30 occurrences.
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 5029)
                 INSERT INTO occurrences(source, fingerprint, starts_at, status, highest_action,
                    alertname, severity, labels, summary, first_seen_at, last_seen_at,
                    observations, closed_at)
                 SELECT CASE WHEN i % 2 = 0 THEN 'example' ELSE 'other' END,
                    printf('%016x', i), '2026-10-12T00:00:00.000000000Z', 'firing',
                    CASE WHEN i >= 5000 THEN 3 ELSE 1 END,
                    CASE WHEN i >= 5000 THEN 'Storm' ELSE 'Name' || i END,
                    'warning', json_object('namespace', 'ns', 'pod', 'pod-' || i), '',
                    ?1, ?1, 1, NULL
                 FROM n",
                [first_seen],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    let listed = text
        .lines()
        .filter(|line| line.starts_with("example · ") || line.starts_with("other · "))
        .count();
    assert!(
        text.contains("**5030 open** (5030 warning) · 5030 new")
            && text
                .lines()
                .find(|line| line.starts_with("example · ") || line.starts_with("other · "))
                .is_some_and(|line| line.contains("Storm") && line.contains("×15"))
            && listed <= crate::summary::MAX_GROUP_LINES
            && text.contains(&format!("+{} more (", 5002 - listed)),
        "listed={listed}\n{text}"
    );
}

#[tokio::test]
async fn a_first_heartbeat_within_startup_grace_records_no_gap() {
    let harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let began = crate::now_ms() - 5 * 60_000;
    harness
        .store
        .coverage_tick(Arc::clone(&harness.config), began, began)
        .await
        .unwrap();
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let (gaps, unobserved): (i64, Option<i64>) = harness
        .store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT count(*) FROM heartbeat_gaps),
                    (SELECT unobserved_since FROM source_status WHERE source = 'example')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!((gaps, unobserved), (0, None));
}

/// Reporting from one window ago over a native schema-3 database whose
/// `example` source already watched Watchdog, in `coverage` since `changed`
/// after the window start (none if `None`), with no heartbeat yet. Returns the
/// harness and the window start.
async fn upgraded_from_schema_3(coverage: &str, changed: Option<i64>) -> (Harness, i64) {
    let mut harness = Harness::shadow(&format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    ))
    .await;
    let end = start(&harness).await;
    let started = end - DAY_MS;
    let upgraded_path = harness.directory.path().join("schema-3.sqlite");
    {
        let connection = rusqlite::Connection::open(&upgraded_path).unwrap();
        connection
            .execute_batch(include_str!("../store/schema_v3.sql"))
            .unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
        connection
            .execute(
                "INSERT INTO source_status(source, coverage, coverage_changed_at,
                    heartbeat_alertname) VALUES ('example', ?1, ?2, 'Watchdog')",
                rusqlite::params![coverage, changed.map(|offset| started + offset)],
            )
            .unwrap();
    }
    let migrated = Store::open(upgraded_path.clone(), harness.config.storage.max_bytes)
        .await
        .unwrap();
    harness.store = migrated.clone();
    harness.state.store = migrated;
    harness.path = upgraded_path;
    assert_eq!(
        tick(&harness, crate::now_ms() - DAY_MS).await,
        SummaryTick::Started(started)
    );
    (harness, started)
}

fn minute(millis: i64) -> String {
    crate::render::discord_time(millis, 'f')
}

#[tokio::test]
async fn first_heartbeat_ingested_before_any_tick_after_schema_3_upgrade_keeps_the_gap() {
    let (harness, started) = upgraded_from_schema_3("unknown", Some(0)).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, crate::now_ms()).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    assert!(
        text.contains(&format!(
            "example: no heartbeat recorded since {}, ongoing at window end",
            minute(started + 10 * 60_000)
        )),
        "{text}"
    );
}

#[tokio::test]
async fn upgraded_never_seen_source_is_unobserved_before_and_after_its_first_heartbeat() {
    // 0.1.0 had found the heartbeat overdue two hours into the window, but schema 3
    // did not keep when watching began, so no start may be invented.
    let (harness, started) = upgraded_from_schema_3("never_seen", Some(2 * HOUR)).await;
    let expected = "example: no heartbeat ever recorded, ongoing at window end";
    let before = harness
        .store
        .summary_tick(Arc::clone(&harness.config), crate::now_ms())
        .await
        .unwrap();
    let before_text = summaries(&harness).await.pop().unwrap().1;
    // The same window, reported again after the first heartbeat arrived.
    harness
        .store
        .with_connection(move |connection| {
            connection.execute("DELETE FROM summaries WHERE window_end > ?1", [started])?;
            connection.execute("DELETE FROM outbox", [])?;
            Ok(())
        })
        .await
        .unwrap();
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, crate::now_ms()).await;
    let after_text = summaries(&harness).await.pop().unwrap().1;
    assert!(
        matches!(before, Some(SummaryTick::Queued(_)))
            && before_text.contains(expected)
            && after_text.contains(expected),
        "before:\n{before_text}\nafter:\n{after_text}"
    );
}

#[tokio::test]
async fn upgraded_source_without_any_recorded_coverage_time_is_never_reported_healthy() {
    let (harness, _) = upgraded_from_schema_3("unknown", None).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    tick(&harness, crate::now_ms()).await;
    let text = summaries(&harness).await.pop().unwrap().1;
    assert!(
        text.contains("example: no heartbeat ever recorded, ongoing at window end"),
        "{text}"
    );
}

/// Restarts the store under the same configuration, as a daemon restart would.
async fn restart(harness: Harness, extra: &str) -> Harness {
    let Harness {
        directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let store = Store::open(path.clone(), config.storage.max_bytes)
        .await
        .unwrap();
    let mut harness = Harness::shadow(extra).await;
    harness.directory = directory;
    harness.path = path;
    harness.config = config;
    harness.state.store = store.clone();
    harness.store = store;
    harness
}

#[tokio::test]
async fn a_newly_configured_heartbeat_reports_alike_in_either_startup_order() {
    let extra = format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    );
    let mut coverage = Vec::new();
    for coverage_first in [false, true] {
        let harness = Harness::shadow(&extra).await;
        // Reporting began a day ago, before this heartbeat was configured.
        start(&harness).await;
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
        let now = crate::now_ms();
        if coverage_first {
            harness
                .store
                .coverage_tick(Arc::clone(&harness.config), now, now)
                .await
                .unwrap();
        }
        assert!(matches!(tick(&harness, now).await, SummaryTick::Queued(_)));
        let text = summaries(&harness).await.pop().unwrap().1;
        coverage.push(text.contains("No heartbeat gaps") && !text.contains("Coverage"));
    }
    assert_eq!(
        (coverage[0], coverage[1]),
        (true, true),
        "monitoring began after the window, in both orders"
    );
}

#[tokio::test]
async fn groups_differing_only_around_control_characters_are_counted_apart() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    for index in 0..13 {
        harness
            .ingest(alert(
                &format!("Plain{index:02}"),
                "warning",
                &format!("1{index:015x}"),
                "firing",
            ))
            .await;
    }
    // Joined with the separators the old count used, these two groups collided.
    harness
        .ingest(alert("a\u{1f}b", "c", "2000000000000001", "firing"))
        .await;
    harness
        .ingest(alert("a", "b\u{1f}c", "2000000000000002", "firing"))
        .await;
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    assert!(
        text.contains("**15 open** (2 critical, 13 warning) · 15 new")
            && text.contains("**a** (bc)")
            && text.contains("**ab** (c)")
            && text.contains("+5 more (5 alerts)"),
        "{text}"
    );
}

#[tokio::test]
async fn omissions_reported_with_only_a_heartbeat_survive_a_clean_heartbeat_and_restart() {
    let extra = format!(
        "{}[sources.example.heartbeat]\nstale_after = \"10m\"\n",
        summary_section()
    );
    let harness = Harness::shadow(&extra).await;
    let end = start(&harness).await;
    let truncated = with(
        alert("Watchdog", "none", "4000000000000001", "firing"),
        "/truncatedAlerts",
        json!(5),
    );
    harness.ingest(truncated).await;
    harness
        .ingest(alert("Watchdog", "none", "4000000000000001", "firing"))
        .await;
    let harness = restart(harness, &extra).await;
    heartbeats_until(&harness, end + DAY_MS - 30_000).await;
    tick(&harness, end + DAY_MS).await;
    let text = summaries(&harness).await.remove(0).1;
    let (occurrences, decisions): (i64, i64) = harness
        .store
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT count(*) FROM occurrences), (SELECT count(*) FROM decisions)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert!(
        text.contains("example: 5 alerts omitted by Alertmanager")
            && !text.contains("Watchdog")
            && (occurrences, decisions) == (0, 0),
        "occurrences={occurrences} decisions={decisions}\n{text}"
    );
}

#[tokio::test]
async fn the_generated_window_is_reported_only_after_a_summary_is_generated() {
    let harness = Harness::shadow(&summary_section()).await;
    let end = start(&harness).await;
    let initial = harness.store.probe().await.unwrap();
    let restarted = restart(harness, &summary_section()).await;
    let current = tick(&restarted, crate::now_ms()).await;
    let after_restart = restarted.store.probe().await.unwrap();
    tick(&restarted, end + DAY_MS).await;
    let generated = restarted.store.probe().await.unwrap();
    let again = restart(restarted, &summary_section()).await;
    let restored = again.store.probe().await.unwrap();
    assert_eq!(
        (
            initial.summary_generated,
            current,
            after_restart.summary_generated,
            generated.summary_generated,
            restored.summary_generated
        ),
        (
            None,
            SummaryTick::Current(end),
            None,
            Some(end + DAY_MS),
            Some(end + DAY_MS)
        )
    );
}

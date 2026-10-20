//! Outbox delivery against a labelled mock Discord fixture.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Harness, USER_ID, fixture,
    fixtures::{MockProvider, Scripted},
    with,
};
use crate::{
    delivery::{Discord, Worker},
    store::{Claim, ClaimLimits, Completion, Store},
};

pub(super) fn worker(harness: &Harness, destination: &str, timeout: Duration) -> Worker {
    let config = &harness.config.destinations[destination];
    Worker {
        destination: destination.to_owned(),
        discord: Discord::new(config, timeout).unwrap(),
        mention_cooldown: config.mention_cooldown,
        store: harness.store.clone(),
        metrics: Arc::clone(&harness.state.metrics),
        wake: Arc::clone(&harness.state.wake),
        settings: harness.config.delivery,
    }
}

pub(super) fn spawn(worker: Worker) -> CancellationToken {
    let cancel = CancellationToken::new();
    tokio::spawn(worker.run(cancel.clone()));
    cancel
}

pub(super) async fn wait_for_state(store: &Store, id: i64, expected: &'static str) -> String {
    let mut state = String::new();
    for _ in 0..200 {
        state =
            store
                .with_connection(move |connection| {
                    Ok(connection.query_row(
                        "SELECT state FROM outbox WHERE id = ?1",
                        [id],
                        |row| row.get(0),
                    )?)
                })
                .await
                .unwrap();
        if state == expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    state
}

/// The message content; empty when a message is embeds only.
pub(super) fn content(request: &super::fixtures::Captured) -> &str {
    request.body["content"].as_str().unwrap_or_default()
}

/// Everything a reader sees in a sent message: content, then each embed's text.
pub(super) fn shown(request: &super::fixtures::Captured) -> String {
    let mut lines = vec![content(request).to_owned()];
    for embed in request.body["embeds"].as_array().into_iter().flatten() {
        let embed: crate::render::Embed = serde_json::from_value(embed.clone()).unwrap();
        lines.push(
            crate::render::Part::Card(crate::render::Card {
                headline: String::new(),
                embed,
            })
            .text(),
        );
    }
    lines.retain(|line| !line.is_empty());
    lines.join("\n")
}

#[tokio::test]
async fn critical_alert_is_delivered_with_wait_true_and_explicit_mentions() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(1).await;
    let state = wait_for_state(&harness.store, 1, "sent").await;
    cancel.cancel();
    let remote: String = harness
        .query("SELECT remote_id FROM outbox WHERE id = 1")
        .await;
    assert_eq!(
        (
            requests[0].query.as_deref(),
            &requests[0].body["allowed_mentions"],
            content(&requests[0])
                == format!(
                    "<@{USER_ID}> Critical: Synthetic critical storage alert, not a real incident"
                )
                && shown(&requests[0]).contains("**Critical** · firing · example-service"),
            state.as_str(),
            remote.as_str()
        ),
        (
            Some("wait=true"),
            &json!({"parse": [], "users": [USER_ID], "roles": []}),
            true,
            "sent",
            "200000000000000001"
        )
    );
}

#[tokio::test]
async fn warning_is_posted_without_any_mention() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/severity",
            json!("warning"),
        ))
        .await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(1).await;
    cancel.cancel();
    assert_eq!(
        (
            &requests[0].body["allowed_mentions"],
            content(&requests[0]).contains("<@")
        ),
        (&json!({"parse": [], "users": [], "roles": []}), false)
    );
}

#[tokio::test]
async fn rate_limit_response_delays_the_retry_by_retry_after() {
    let limited = Scripted::json(
        429,
        json!({"message": "limited", "retry_after": 0.4, "global": false}),
    )
    .with_header("retry-after", "1");
    let discord = MockProvider::discord(vec![limited]).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    let state = wait_for_state(&harness.store, 1, "sent").await;
    cancel.cancel();
    let throttled: i64 = harness
        .query("SELECT throttled FROM outbox WHERE id = 1")
        .await;
    let waited = requests[1].at - requests[0].at;
    assert!(
        state == "sent" && throttled == 1 && waited >= Duration::from_secs(1),
        "state={state} throttled={throttled} waited={waited:?}"
    );
}

#[tokio::test]
async fn missing_webhook_fails_permanently_without_retrying() {
    let discord = MockProvider::discord(vec![Scripted::json(
        404,
        json!({"message": "Unknown Webhook", "code": 10015}),
    )])
    .await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, 1, "failed").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    cancel.cancel();
    assert_eq!((state.as_str(), discord.requests().len()), ("failed", 1));
}

#[tokio::test]
async fn gateway_timeout_is_retried_and_recorded_as_a_possible_duplicate() {
    let discord = MockProvider::discord(vec![Scripted::json(504, json!({}))]).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, 1, "sent").await;
    cancel.cancel();
    let possibly: i64 = harness
        .query("SELECT possibly_delivered FROM outbox WHERE id = 1")
        .await;
    assert_eq!(
        (state.as_str(), possibly, discord.requests().len()),
        ("sent", 1, 2)
    );
}

#[tokio::test]
async fn exhausted_ambiguous_retries_end_as_ambiguous_not_failed() {
    let discord = MockProvider::start(Vec::new(), Scripted::json(502, json!({}))).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let state = wait_for_state(&harness.store, 1, "ambiguous").await;
    cancel.cancel();
    assert_eq!((state.as_str(), discord.requests().len()), ("ambiguous", 3));
}

#[tokio::test]
async fn response_timeout_after_sending_is_ambiguous() {
    let slow =
        Scripted::json(200, json!({"id": "200000000000000009"})).delayed(Duration::from_secs(2));
    let discord = MockProvider::discord(vec![slow]).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_millis(300)));
    let state = wait_for_state(&harness.store, 1, "sent").await;
    cancel.cancel();
    let possibly: i64 = harness
        .query("SELECT possibly_delivered FROM outbox WHERE id = 1")
        .await;
    assert_eq!((state.as_str(), possibly), ("sent", 1));
}

#[tokio::test]
async fn resolution_before_the_first_attempt_cancels_the_send_and_recovery() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let receipt = harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    let state: String = harness.query("SELECT state FROM outbox WHERE id = 1").await;
    assert_eq!(
        (
            receipt.decisions[0].reason,
            state.as_str(),
            discord.requests().len()
        ),
        ("resolution_without_notification", "superseded", 0)
    );
}

#[tokio::test]
async fn recovery_cannot_overtake_an_unresolved_ambiguous_send() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let limits = ClaimLimits {
        max_rows: 10,
        budget_chars: 1900,
        mention_cooldown_ms: 0,
    };
    let Claim::Batch(batch) = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), limits)
        .await
        .unwrap()
    else {
        panic!("expected a batch");
    };
    harness
        .store
        .complete_deliveries(
            "ops".into(),
            batch.ids,
            batch.mention,
            Completion::Retry {
                retry_at: crate::now_ms(),
                status: Some(504),
                error: "HTTP 504".into(),
                ambiguous: true,
            },
            3,
        )
        .await
        .unwrap();
    let receipt = harness
        .ingest(with(fixture(), "/alerts/0/status", json!("resolved")))
        .await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    cancel.cancel();
    assert_eq!(
        (
            receipt.decisions[0].reason,
            shown(&requests[0]).contains("**Critical** · firing"),
            shown(&requests[1]).contains("**Recovered** · critical")
        ),
        ("recovery", true, true)
    );
}

#[tokio::test]
async fn interrupted_send_resumes_after_restart_as_a_possible_duplicate() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let limits = ClaimLimits {
        max_rows: 10,
        budget_chars: 1900,
        mention_cooldown_ms: 0,
    };
    let claim = harness
        .store
        .claim_deliveries("ops".into(), crate::now_ms(), limits)
        .await
        .unwrap();
    assert!(matches!(claim, Claim::Batch(_)));
    let Harness {
        directory: _directory,
        path,
        config,
        store,
        state,
    } = harness;
    drop(state);
    drop(store);
    let reopened = Store::open(path.clone(), config.storage.max_bytes)
        .await
        .unwrap();
    let possibly: i64 = reopened
        .with_connection(|connection| {
            Ok(connection.query_row(
                "SELECT possibly_delivered FROM outbox WHERE state = 'pending'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    let destination = &config.destinations["ops"];
    let worker = Worker {
        destination: "ops".into(),
        discord: Discord::new(destination, Duration::from_secs(2)).unwrap(),
        mention_cooldown: destination.mention_cooldown,
        store: reopened.clone(),
        metrics: Arc::new(crate::metrics::Metrics::new()),
        wake: Arc::new(tokio::sync::Notify::new()),
        settings: config.delivery,
    };
    let cancel = spawn(worker);
    let state = wait_for_state(&reopened, 1, "sent").await;
    cancel.cancel();
    assert_eq!(
        (possibly, state.as_str(), discord.requests().len()),
        (1, "sent", 1)
    );
}

#[tokio::test]
async fn storm_is_batched_and_mentions_are_coalesced_without_dropping_alerts() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(
        &discord.discord_url(),
        &format!(
            "[destinations.pager]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n\
             mention_users = [\"{USER_ID}\"]\nmention_cooldown = \"10m\"\n\
             [sources.paged]\ncredential_files = [\"third.token\"]\ndefault_destination = \"pager\"\n"
        ),
    )
    .await;
    let mut storm = fixture();
    let template = storm["alerts"][0].clone();
    storm["alerts"] = Value::Array(
        (0..30)
            .map(|index| {
                with(
                    template.clone(),
                    "/fingerprint",
                    json!(format!("{index:016x}")),
                )
            })
            .collect(),
    );
    harness.ingest_as("paged", storm).await;
    let cancel = spawn(worker(&harness, "pager", Duration::from_secs(2)));
    let requests = discord.wait_for(3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let sent: i64 = harness
        .query("SELECT count(*) FROM outbox WHERE state = 'sent'")
        .await;
    let mentioned = requests
        .iter()
        .filter(|request| request.body["allowed_mentions"]["users"] == json!([USER_ID]))
        .count();
    let longest = requests
        .iter()
        .map(|request| {
            let embeds: Vec<crate::render::Embed> =
                serde_json::from_value(request.body["embeds"].clone()).unwrap();
            let within = embeds.len() <= crate::render::MAX_EMBEDS
                && embeds.iter().all(crate::render::Embed::within_limits)
                && embeds
                    .iter()
                    .map(crate::render::Embed::units)
                    .sum::<usize>()
                    <= crate::render::EMBEDS_TOTAL_LIMIT;
            if within {
                crate::render::units(content(request))
            } else {
                usize::MAX
            }
        })
        .max()
        .unwrap();
    assert!(
        sent == 30 && requests.len() == 3 && mentioned == 1 && longest <= 2000,
        "sent={sent} requests={} mentioned={mentioned} longest={longest}",
        requests.len()
    );
}

fn quiet_warning() -> Value {
    let warning = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    let warning = with(warning, "/alerts/0/fingerprint", json!("fedcba9876543210"));
    with(warning, "/receiver", json!("quiet"))
}

async fn rate_limit_gap(scope: &str, global: bool) -> Duration {
    let limited = Scripted::json(
        429,
        json!({"message": "limited", "retry_after": 1.5, "global": global}),
    )
    .with_header("x-ratelimit-scope", scope);
    let discord = MockProvider::discord(vec![limited]).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    harness.ingest(quiet_warning()).await;
    let ops = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    discord.wait_for(1).await;
    let quiet = spawn(worker(&harness, "quiet", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    ops.cancel();
    quiet.cancel();
    requests[1].at - requests[0].at
}

#[tokio::test(flavor = "multi_thread")]
async fn global_rate_limit_pauses_every_destination_worker() {
    let gap = rate_limit_gap("global", true).await;
    assert!(gap >= Duration::from_millis(1400), "{gap:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_rate_limit_pauses_only_its_own_destination() {
    let gap = rate_limit_gap("shared", false).await;
    assert!(gap < Duration::from_secs(1), "{gap:?}");
}

/// Rows an earlier release left pending hold plain Markdown lines.
async fn queue_plain(harness: &Harness, kind: &str, content: &str) {
    let (kind, content) = (kind.to_owned(), content.to_owned());
    harness
        .store
        .with_connection(move |connection| {
            connection.execute(
                "INSERT INTO outbox(occurrence_id, source, destination, kind, action, mode, state,
                    content, created_at, due_at)
                 VALUES (NULL, 'example', 'ops', ?1, 'quiet_post', 'live', 'pending', ?2, ?3, ?3)",
                rusqlite::params![kind, content, crate::now_ms()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn messages_queued_by_an_earlier_release_are_still_sent_unchanged() {
    let discord = MockProvider::discord(Vec::new()).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    let line = "**COVERAGE RECOVERED** source example: heartbeats resumed";
    let summary = format!(
        "**Summary** 2026-10-12 06:00 to 2026-10-13 06:00 UTC\n**Open: 0**\n{}",
        "- x [warning]\n".repeat(100)
    );
    queue_plain(&harness, "coverage", line).await;
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/severity",
            json!("warning"),
        ))
        .await;
    queue_plain(&harness, "summary", &summary).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    cancel.cancel();
    let embeds =
        |request: &super::fixtures::Captured| request.body["embeds"].as_array().map_or(0, Vec::len);
    assert_eq!(
        (
            content(&requests[0]),
            embeds(&requests[0]),
            shown(&requests[0]).contains("**Warning** · firing"),
            content(&requests[1]),
            embeds(&requests[1]),
        ),
        (line, 1, true, summary.as_str(), 0),
        "{requests:?}"
    );
}

#[tokio::test]
async fn a_retried_card_is_sent_exactly_as_first_rendered() {
    let discord = MockProvider::discord(vec![Scripted::json(504, json!({}))]).await;
    let harness = Harness::live(&discord.discord_url(), "").await;
    harness.ingest(fixture()).await;
    let cancel = spawn(worker(&harness, "ops", Duration::from_secs(2)));
    let requests = discord.wait_for(2).await;
    cancel.cancel();
    assert!(
        requests[0].body["embeds"] == requests[1].body["embeds"]
            && requests[1].body["embeds"][0]["title"].is_string(),
        "{requests:?}"
    );
}

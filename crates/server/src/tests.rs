use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

use crate::{
    config::Config,
    credentials::Credentials,
    http::{self, AppState},
    metrics::Metrics,
    store::Store,
};

mod assessment;
mod delivery;
pub(crate) mod fixtures;
mod preview;
mod summary;

pub(crate) const TOKEN: &str = "synthetic-test-token-not-a-live-secret-0123456789";
pub(crate) const OTHER_TOKEN: &str = "synthetic-other-token-not-a-live-secret-987654321";
pub(crate) const THIRD_TOKEN: &str = "synthetic-third-token-not-a-live-secret-55555555";
pub(crate) const USER_ID: &str = "123456789012345678";

pub(crate) fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../examples/critical.json")).unwrap()
}

pub(crate) fn with(mut value: Value, pointer: &str, replacement: Value) -> Value {
    *value.pointer_mut(pointer).unwrap() = replacement;
    value
}

pub(crate) struct Harness {
    pub directory: TempDir,
    pub path: PathBuf,
    pub config: Arc<Config>,
    pub store: Store,
    pub state: AppState,
}

impl Harness {
    /// A shadow-mode harness with sources `example` and `other`.
    pub(crate) async fn shadow(extra: &str) -> Self {
        Self::build("shadow", None, extra).await
    }

    /// A live harness whose `ops` destination points at a mock Discord URL.
    pub(crate) async fn live(discord_url: &str, extra: &str) -> Self {
        Self::build("live", Some(discord_url), extra).await
    }

    async fn build(mode: &str, discord_url: Option<&str>, extra: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("example.token"), TOKEN).unwrap();
        std::fs::write(root.join("other.token"), OTHER_TOKEN).unwrap();
        std::fs::write(root.join("clef.token"), "synthetic-clef-token-not-live").unwrap();
        std::fs::write(root.join("third.token"), THIRD_TOKEN).unwrap();
        std::fs::write(
            root.join("ops.url"),
            discord_url.unwrap_or("https://discord.invalid/api/webhooks/1/x"),
        )
        .unwrap();
        let text = format!(
            r#"
mode = "{mode}"
[storage]
path = "alerts.sqlite"
[delivery]
backoff_base = "100ms"
backoff_max = "200ms"
max_attempts = 3
[sources.example]
credential_files = ["example.token"]
default_destination = "ops"
receivers = {{ quiet = "quiet" }}
[sources.other]
credential_files = ["other.token"]
default_destination = "ops"
[destinations.ops]
kind = "discord"
webhook_url_file = "ops.url"
mention_users = ["{USER_ID}"]
mention_cooldown = "0s"
[destinations.quiet]
kind = "discord"
webhook_url_file = "ops.url"
{extra}
"#
        );
        let config_path = root.join("criers.toml");
        std::fs::write(&config_path, text).unwrap();
        let config = Arc::new(Config::load(&config_path).unwrap());
        let path = config.storage.path.clone();
        let store = Store::open(path.clone(), config.storage.max_bytes)
            .await
            .unwrap();
        let credentials = Credentials::load(
            config
                .sources
                .iter()
                .map(|(id, source)| (id.clone(), source.credential_files.clone())),
        )
        .unwrap();
        let state = AppState {
            config: Arc::clone(&config),
            credentials: Arc::new(credentials),
            store: store.clone(),
            admission: Arc::new(http::Admission::new(
                8,
                std::time::Duration::from_millis(100),
            )),
            metrics: Arc::new(Metrics::new()),
            wake: Arc::new(Notify::new()),
            draining: Arc::new(AtomicBool::new(false)),
            ready_window_ms: 60_000,
        };
        Self {
            directory,
            path,
            config,
            store,
            state,
        }
    }

    pub(crate) fn app(&self) -> Router {
        http::ingest_router(self.state.clone())
    }

    pub(crate) fn ops(&self) -> Router {
        http::ops_router(self.state.clone())
    }

    pub(crate) async fn ingest(&self, value: Value) -> crate::store::Receipt {
        self.ingest_as("example", value).await
    }

    pub(crate) async fn ingest_as(&self, source: &str, value: Value) -> crate::store::Receipt {
        let webhook: criers_core::Webhook = serde_json::from_value(value).unwrap();
        webhook.validate().unwrap();
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        self.store
            .ingest(
                Arc::clone(&self.config),
                source.into(),
                webhook,
                permit,
                |_| {},
            )
            .await
            .unwrap()
    }

    pub(crate) async fn snapshot(&self) -> crate::store::Snapshot {
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        self.store.snapshot("example".into(), permit).await.unwrap()
    }

    pub(crate) async fn query<T: rusqlite::types::FromSql + Send + 'static>(
        &self,
        sql: &'static str,
    ) -> T {
        self.store
            .with_connection(move |connection| Ok(connection.query_row(sql, [], |row| row.get(0))?))
            .await
            .unwrap()
    }
}

pub(crate) async fn post_to(
    app: Router,
    source: &str,
    payload: &Value,
    token: &str,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sources/{source}/alertmanager"))
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

pub(crate) async fn get(app: Router, uri: &str) -> StatusCode {
    app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

fn resolved() -> Value {
    with(fixture(), "/alerts/0/status", json!("resolved"))
}

fn later() -> Value {
    let value = with(
        fixture(),
        "/alerts/0/startsAt",
        json!("2026-10-11T10:00:00Z"),
    );
    with(value, "/alerts/0/endsAt", json!("2026-10-11T10:10:00Z"))
}

#[tokio::test]
async fn accepted_critical_event_commits_a_shadow_ping_without_sending() {
    let harness = Harness::shadow("").await;
    let (status, body) = post_to(harness.app(), "example", &fixture(), TOKEN).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (
            status,
            body["decisions"][0]["action"].as_str(),
            body["mode"].as_str(),
            snapshot.intents,
            snapshot.live_open
        ),
        (StatusCode::ACCEPTED, Some("ping"), Some("shadow"), 1, 0)
    );
}

#[tokio::test]
async fn duplicate_delivery_after_database_reopen_does_not_create_a_second_intent() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    drop(harness.state);
    let max_bytes = harness.config.storage.max_bytes;
    drop(harness.store);
    let reopened = Store::open(harness.path.clone(), max_bytes).await.unwrap();
    let webhook: criers_core::Webhook = serde_json::from_value(fixture()).unwrap();
    let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    let receipt = reopened
        .ingest(
            Arc::clone(&harness.config),
            "example".into(),
            webhook,
            permit,
            |_| {},
        )
        .await
        .unwrap();
    let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    let snapshot = reopened.snapshot("example".into(), permit).await.unwrap();
    assert_eq!(
        (
            receipt.decisions[0].intent_created,
            snapshot.events,
            snapshot.occurrences,
            snapshot.intents
        ),
        (false, 2, 1, 1)
    );
}

#[tokio::test]
async fn a_later_recurrence_creates_a_new_occurrence_and_intent() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    harness.ingest(later()).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (snapshot.occurrences, snapshot.intents, snapshot.firing),
        (2, 2, 1)
    );
}

#[tokio::test]
async fn stale_resolution_cannot_close_a_newer_occurrence() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    harness.ingest(later()).await;
    let receipt = harness.ingest(resolved()).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (
            snapshot.firing,
            receipt.decisions[0].reason,
            snapshot.intents
        ),
        (1, "superseded_occurrence", 2)
    );
}

#[tokio::test]
async fn late_firing_cannot_reopen_a_resolved_occurrence() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    harness.ingest(resolved()).await;
    let receipt = harness.ingest(fixture()).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (snapshot.firing, receipt.decisions[0].reason),
        (0, "late_firing_after_resolution")
    );
}

#[tokio::test]
async fn shadow_resolution_creates_no_recovery_because_nothing_was_delivered() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    let receipt = harness.ingest(resolved()).await;
    assert_eq!(
        (
            receipt.decisions[0].reason,
            receipt.decisions[0].intent_created
        ),
        ("resolution_without_notification", false)
    );
}

#[tokio::test]
async fn mixed_batch_uses_individual_status_not_group_status() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    let mut mixed = with(resolved(), "/status", json!("resolved"));
    let firing = with(
        fixture()["alerts"][0].clone(),
        "/fingerprint",
        json!("fedcba9876543210"),
    );
    mixed["alerts"].as_array_mut().unwrap().push(firing);
    harness.ingest(mixed).await;
    let snapshot = harness.snapshot().await;
    assert_eq!((snapshot.firing, snapshot.intents), (1, 2));
}

#[tokio::test]
async fn truncated_batch_records_coverage_gap_without_inventing_resolution() {
    let harness = Harness::shadow("").await;
    harness.ingest(fixture()).await;
    let partial = with(
        fixture(),
        "/alerts/0/fingerprint",
        json!("fedcba9876543210"),
    );
    let receipt = harness
        .ingest(with(partial, "/truncatedAlerts", json!(5)))
        .await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (
            receipt.coverage_gap,
            snapshot.firing,
            snapshot.truncated_alerts
        ),
        (true, 2, 5)
    );
}

#[tokio::test]
async fn unrecognized_severity_uses_conservative_fallback_without_a_model() {
    let harness = Harness::shadow("").await;
    let receipt = harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/severity",
            json!("unrecognized"),
        ))
        .await;
    assert_eq!(
        (receipt.decisions[0].action, receipt.decisions[0].reason),
        (
            criers_core::Action::Ping,
            "unknown_severity_conservative_fallback"
        )
    );
}

#[tokio::test]
async fn untrusted_source_severity_never_reaches_the_critical_floor() {
    let harness = Harness::shadow(
        "[sources.untrusted]\ncredential_files = [\"third.token\"]\ndefault_destination = \"ops\"\ntrust_severity = false\n",
    )
    .await;
    let receipt = harness.ingest_as("untrusted", fixture()).await;
    assert_eq!(
        (receipt.decisions[0].action, receipt.decisions[0].reason),
        (criers_core::Action::Ping, "untrusted_severity_fallback")
    );
}

#[tokio::test]
async fn severity_escalation_creates_one_higher_priority_intent() {
    let harness = Harness::shadow("").await;
    harness
        .ingest(with(
            fixture(),
            "/alerts/0/labels/severity",
            json!("warning"),
        ))
        .await;
    harness.ingest(fixture()).await;
    harness.ingest(fixture()).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(snapshot.intents, 2);
}

#[tokio::test]
async fn repeated_observations_are_compacted_into_occurrence_counters() {
    let harness = Harness::shadow("").await;
    for _ in 0..5 {
        harness.ingest(fixture()).await;
    }
    let decisions: i64 = harness.query("SELECT count(*) FROM decisions").await;
    let observations: i64 = harness.query("SELECT observations FROM occurrences").await;
    assert_eq!((decisions, observations), (1, 5));
}

#[tokio::test]
async fn receiver_mapping_chooses_the_destination_and_labels_cannot() {
    let harness = Harness::live("http://127.0.0.1:9/api/webhooks/123456789012345678/x", "").await;
    let mut payload = with(fixture(), "/receiver", json!("quiet"));
    payload["alerts"][0]["labels"]["destination"] = json!("ops");
    harness.ingest(payload).await;
    let destination: String = harness.query("SELECT destination FROM outbox").await;
    assert_eq!(destination, "quiet");
}

#[tokio::test]
async fn heartbeat_updates_freshness_without_storing_an_event() {
    let harness = Harness::shadow(
        "[sources.example.heartbeat]\nalertname = \"Watchdog\"\nstale_after = \"5m\"\n",
    )
    .await;
    let heartbeat = with(fixture(), "/alerts/0/labels/alertname", json!("Watchdog"));
    let receipt = harness.ingest(heartbeat).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (
            receipt.heartbeat,
            snapshot.events,
            snapshot.occurrences,
            snapshot.last_heartbeat_at.is_some()
        ),
        (true, 0, 0, true)
    );
}

#[tokio::test]
async fn another_sources_valid_token_cannot_impersonate_this_source() {
    let harness = Harness::shadow("").await;
    let (status, _) = post_to(harness.app(), "example", &fixture(), OTHER_TOKEN).await;
    let events: i64 = harness.query("SELECT count(*) FROM events").await;
    assert_eq!((status, events), (StatusCode::UNAUTHORIZED, 0));
}

#[tokio::test]
async fn source_identity_comes_from_the_authenticated_path() {
    let harness = Harness::shadow("").await;
    let (status, _) = post_to(harness.app(), "other", &fixture(), OTHER_TOKEN).await;
    let source: String = harness.query("SELECT source FROM occurrences").await;
    assert_eq!((status, source.as_str()), (StatusCode::ACCEPTED, "other"));
}

#[tokio::test]
async fn unknown_source_is_rejected_like_a_wrong_token() {
    let harness = Harness::shadow("").await;
    let (status, _) = post_to(harness.app(), "nobody", &fixture(), TOKEN).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authentication_failure_never_commits_an_event() {
    let harness = Harness::shadow("").await;
    let (status, _) = post_to(harness.app(), "example", &fixture(), "incorrect").await;
    let snapshot = harness.snapshot().await;
    assert_eq!((status, snapshot.events), (StatusCode::UNAUTHORIZED, 0));
}

#[tokio::test]
async fn unsupported_webhook_version_never_commits_an_event() {
    let harness = Harness::shadow("").await;
    let (status, _) = post_to(
        harness.app(),
        "example",
        &with(fixture(), "/version", json!("5")),
        TOKEN,
    )
    .await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (status, snapshot.events),
        (StatusCode::UNPROCESSABLE_ENTITY, 0)
    );
}

#[tokio::test]
async fn oversized_body_is_a_permanent_413_without_persisting() {
    let harness = Harness::shadow("").await;
    let oversized = with(
        fixture(),
        "/alerts/0/annotations/summary",
        json!("x".repeat(1_048_576)),
    );
    let (status, _) = post_to(harness.app(), "example", &oversized, TOKEN).await;
    let snapshot = harness.snapshot().await;
    assert_eq!(
        (status, snapshot.events),
        (StatusCode::PAYLOAD_TOO_LARGE, 0)
    );
}

#[tokio::test]
async fn occupied_admission_returns_retryable_503() {
    let harness = Harness::shadow("").await;
    let mut state = harness.state.clone();
    state.admission = Arc::new(http::Admission::new(
        0,
        std::time::Duration::from_millis(50),
    ));
    let (status, _) = post_to(http::ingest_router(state), "example", &fixture(), TOKEN).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn timed_out_ingestion_returns_503_with_retry_after() {
    let harness =
        Harness::shadow("[server]\nrequest_timeout = \"1s\"\nadmission_wait = \"100ms\"").await;
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let store = harness.store.clone();
    let blocker = tokio::spawn(async move {
        store
            .with_connection(move |_connection| {
                locked_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    locked_rx.await.unwrap();

    let response = harness
        .app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sources/example/alertmanager")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::from(fixture().to_string()))
                .unwrap(),
        )
        .await;

    release_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    let response = response.unwrap();
    assert_eq!(
        (
            response.status(),
            response
                .headers()
                .get("Retry-After")
                .and_then(|value| value.to_str().ok())
        ),
        (StatusCode::SERVICE_UNAVAILABLE, Some("5"))
    );
}

#[tokio::test]
async fn readiness_ignores_occupied_ingestion_admission() {
    let harness = Harness::shadow("").await;
    let mut state = harness.state.clone();
    state.admission = Arc::new(http::Admission::new(
        0,
        std::time::Duration::from_millis(50),
    ));
    let status = get(http::ops_router(state), "/health/ready").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn draining_rejects_new_webhooks_with_retryable_503() {
    let harness = Harness::shadow("").await;
    harness
        .state
        .draining
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (status, _) = post_to(harness.app(), "example", &fixture(), TOKEN).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn metrics_use_bounded_labels_without_fingerprints() {
    let harness = Harness::shadow("").await;
    post_to(harness.app(), "example", &fixture(), TOKEN).await;
    post_to(harness.app(), "attacker-chosen", &fixture(), TOKEN).await;
    let response = harness
        .ops()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let text = String::from_utf8(
        to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains(r#"criers_ingest_requests_total{source="example",outcome="accepted"} 1"#)
            && text.contains(r#"source="unknown",outcome="unauthorized""#)
            && !text.contains("0123456789abcdef")
            && !text.contains("attacker-chosen"),
        "{text}"
    );
}

#[tokio::test]
async fn status_reads_share_ingestion_admission() {
    let harness = Harness::shadow("").await;
    let mut state = harness.state.clone();
    state.admission = Arc::new(http::Admission::new(
        0,
        std::time::Duration::from_millis(50),
    ));
    let response = http::ingest_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/sources/example/status")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn large_batches_are_decoded_off_the_reactor_and_accepted() {
    let harness = Harness::shadow("").await;
    let mut batch = fixture();
    let template = batch["alerts"][0].clone();
    batch["alerts"] = Value::Array(
        (0..400)
            .map(|index| {
                with(
                    template.clone(),
                    "/fingerprint",
                    json!(format!("{index:016x}")),
                )
            })
            .collect(),
    );
    assert!(batch.to_string().len() > 65_536);
    let (status, body) = post_to(harness.app(), "example", &batch, TOKEN).await;
    assert_eq!(
        (status, body["decisions"].as_array().map(Vec::len)),
        (StatusCode::ACCEPTED, Some(400))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_burst_within_the_waiting_line_is_admitted_instead_of_rejected() {
    let harness = Harness::shadow("").await;
    let mut state = harness.state.clone();
    state.admission = Arc::new(http::Admission::new(2, std::time::Duration::from_secs(10)));
    let app = http::ingest_router(state);
    let posts: Vec<_> = (0..10)
        .map(|index| {
            let app = app.clone();
            let payload = with(
                fixture(),
                "/alerts/0/fingerprint",
                json!(format!("{index:016x}")),
            );
            tokio::spawn(async move { post_to(app, "example", &payload, TOKEN).await.0 })
        })
        .collect();
    let mut statuses = Vec::new();
    for post in posts {
        statuses.push(post.await.unwrap());
    }
    assert!(
        statuses
            .iter()
            .all(|&status| status == StatusCode::ACCEPTED),
        "{statuses:?}"
    );
}

/// Probes admission without touching SQLite: a malformed body is rejected with
/// 422 after a slot is granted, or with 503 when no slot is free.
async fn admission_probe(app: Router) -> StatusCode {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sources/example/alertmanager")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

fn status_request() -> Request<Body> {
    Request::builder()
        .uri("/v1/sources/example/status")
        .header("Authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn cancelled_status_read_keeps_its_admission_slot_until_the_read_settles() {
    let harness = Harness::shadow("").await;
    let mut state = harness.state.clone();
    state.admission = Arc::new(http::Admission::new(1, std::time::Duration::ZERO));
    let app = http::ingest_router(state);

    // Hold the only SQLite connection so the status read queues behind it.
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let store = harness.store.clone();
    let blocker = async move {
        store
            .with_connection(move |_connection| {
                locked_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    };
    let blocker = tokio::spawn(blocker);
    locked_rx.await.unwrap();

    let request = tokio::spawn(app.clone().oneshot(status_request()));
    let mut held_while_queued = false;
    for _ in 0..1000 {
        if admission_probe(app.clone()).await == StatusCode::SERVICE_UNAVAILABLE {
            held_while_queued = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    request.abort();
    let cancelled = request.await.unwrap_err().is_cancelled();
    let after_cancel = admission_probe(app.clone()).await;

    release_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut after_settle = admission_probe(app.clone()).await;
    while after_settle != StatusCode::UNPROCESSABLE_ENTITY && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        after_settle = admission_probe(app.clone()).await;
    }
    let ordinary = app.oneshot(status_request()).await.unwrap().status();
    assert_eq!(
        (
            held_while_queued,
            cancelled,
            after_cancel,
            after_settle,
            ordinary
        ),
        (
            true,
            true,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::UNPROCESSABLE_ENTITY,
            StatusCode::OK
        )
    );
}

#[tokio::test]
async fn first_record_only_observation_is_a_durable_decision_and_repeats_are_compacted() {
    let harness = Harness::shadow("").await;
    let info = with(fixture(), "/alerts/0/labels/severity", json!("info"));
    let first = harness.ingest(info.clone()).await;
    harness.ingest(info).await;
    let decisions: i64 = harness.query("SELECT count(*) FROM decisions").await;
    let reason: String = harness.query("SELECT reason FROM decisions").await;
    assert_eq!(
        (first.decisions[0].reason, decisions, reason.as_str()),
        ("configured_severity", 1, "configured_severity")
    );
}

fn series_line(harness: &Harness, series: &str) -> Option<String> {
    let text = harness.state.metrics.render().unwrap();
    text.lines()
        .find(|line| line.starts_with(series))
        .map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_after_an_http_timeout_still_records_observations_and_wakes_workers() {
    let harness =
        Harness::shadow("[server]\nrequest_timeout = \"1s\"\nadmission_wait = \"100ms\"\n").await;
    let app = harness.app();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let store = harness.store.clone();
    let blocker = tokio::spawn(async move {
        store
            .with_connection(move |_connection| {
                locked_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    locked_rx.await.unwrap();
    let wake = Arc::clone(&harness.state.wake);
    let woken = wake.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    // The request times out while its transaction waits behind the held connection.
    let (status, _) = post_to(app, "example", &fixture(), TOKEN).await;
    release_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    let woke = tokio::time::timeout(std::time::Duration::from_secs(5), woken)
        .await
        .is_ok();
    let events: i64 = harness.query("SELECT count(*) FROM events").await;
    assert_eq!(
        (
            status,
            events,
            woke,
            series_line(
                &harness,
                r#"criers_decisions_total{action="ping",reason="trusted_critical_floor"}"#
            ),
            series_line(
                &harness,
                r#"criers_alerts_observed_total{source="example",status="firing"}"#
            ),
            series_line(
                &harness,
                r#"criers_ingest_requests_total{source="example",outcome="accepted"}"#
            )
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            1,
            true,
            Some(
                r#"criers_decisions_total{action="ping",reason="trusted_critical_floor"} 1"#
                    .to_owned()
            ),
            Some(r#"criers_alerts_observed_total{source="example",status="firing"} 1"#.to_owned()),
            None
        )
    );
}

#[tokio::test]
async fn a_rolled_back_ingest_emits_no_success_observations() {
    let harness = Harness::shadow("").await;
    harness
        .store
        .with_connection(|connection| {
            connection.execute_batch("PRAGMA query_only = ON")?;
            Ok(())
        })
        .await
        .unwrap();
    let wake = Arc::clone(&harness.state.wake);
    let woken = wake.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    let (status, _) = post_to(harness.app(), "example", &fixture(), TOKEN).await;
    let woke = tokio::time::timeout(std::time::Duration::from_millis(300), woken)
        .await
        .is_ok();
    assert_eq!(
        (
            status,
            woke,
            series_line(&harness, "criers_decisions_total"),
            series_line(&harness, "criers_alerts_observed_total")
        ),
        (StatusCode::SERVICE_UNAVAILABLE, false, None, None)
    );
}

#[tokio::test]
async fn equal_priority_fanout_notifies_each_mapped_destination_once() {
    let harness = Harness::shadow("").await;
    let via_quiet = with(fixture(), "/receiver", json!("quiet"));
    harness.ingest(fixture()).await;
    harness.ingest(via_quiet.clone()).await;
    harness.ingest(fixture()).await;
    harness.ingest(via_quiet).await;
    let intents: String = harness
        .query(
            "SELECT group_concat(destination || ':' || action) FROM
                (SELECT destination, action FROM outbox WHERE kind = 'notify' ORDER BY destination)",
        )
        .await;
    assert_eq!(intents, "ops:ping,quiet:ping");
}

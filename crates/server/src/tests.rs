use std::{path::PathBuf, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use criers_core::{Action, Webhook};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Semaphore;
use tower::ServiceExt;

use crate::{
    http::{self, AppState},
    store::Store,
};

pub(super) const TOKEN: &str = "synthetic-test-token-not-a-live-secret-0123456789";

pub(super) fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../examples/critical.json")).unwrap()
}

pub(super) async fn setup() -> (TempDir, PathBuf, Store, Router) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alerts.sqlite");
    let store = Store::open(path.clone()).await.unwrap();
    let app = http::router(AppState {
        store: store.clone(),
        source: "example".into(),
        token: Arc::from(TOKEN),
        admission: Arc::new(Semaphore::new(8)),
    });
    (directory, path, store, app)
}

async fn ingest(store: &Store, value: Value) -> crate::store::Receipt {
    let webhook: Webhook = serde_json::from_value(value).unwrap();
    webhook.validate().unwrap();
    store.ingest("example".into(), webhook).await.unwrap()
}

pub(super) async fn post(app: Router, payload: Value, token: &str) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/alertmanager")
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::from(payload.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn accepted_critical_event_commits_a_shadow_ping_without_sending() {
    let (_directory, _path, store, app) = setup().await;
    let response = post(app, fixture(), TOKEN).await;
    let status = response.status();
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            status,
            body["decisions"][0]["action"].as_str(),
            snapshot.events,
            snapshot.shadow_intents,
            snapshot.mode
        ),
        (StatusCode::ACCEPTED, Some("ping"), 1, 1, "shadow")
    );
}

#[tokio::test]
async fn duplicate_delivery_after_database_reopen_does_not_create_a_second_ping() {
    let (_directory, path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let reopened = Store::open(path).await.unwrap();
    let receipt = ingest(&reopened, fixture()).await;
    let snapshot = reopened.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            receipt.decisions[0].intent_created,
            snapshot.events,
            snapshot.occurrences,
            snapshot.shadow_intents
        ),
        (false, 2, 1, 1)
    );
}

#[tokio::test]
async fn a_later_recurrence_creates_a_new_occurrence_and_ping() {
    let (_directory, _path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let mut later = fixture();
    later["alerts"][0]["startsAt"] = json!("2026-10-11T10:00:00Z");
    later["alerts"][0]["endsAt"] = json!("2026-10-11T10:10:00Z");
    ingest(&store, later).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!((snapshot.occurrences, snapshot.shadow_intents), (2, 2));
}

#[tokio::test]
async fn stale_resolution_cannot_close_a_newer_occurrence() {
    let (_directory, _path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let mut later = fixture();
    later["alerts"][0]["startsAt"] = json!("2026-10-11T10:00:00Z");
    later["alerts"][0]["endsAt"] = json!("2026-10-11T10:10:00Z");
    ingest(&store, later).await;
    let mut resolved = fixture();
    resolved["alerts"][0]["status"] = json!("resolved");
    let receipt = ingest(&store, resolved).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            snapshot.firing,
            receipt.decisions[0].reason,
            snapshot.shadow_intents
        ),
        (1, "superseded_occurrence", 2)
    );
}

#[tokio::test]
async fn late_firing_cannot_reopen_a_resolved_occurrence() {
    let (_directory, _path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let mut resolved = fixture();
    resolved["alerts"][0]["status"] = json!("resolved");
    ingest(&store, resolved).await;
    let receipt = ingest(&store, fixture()).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            snapshot.firing,
            snapshot.shadow_intents,
            receipt.decisions[0].reason
        ),
        (0, 2, "late_firing_after_resolution")
    );
}

#[tokio::test]
async fn mixed_batch_uses_individual_status_not_group_status() {
    let (_directory, _path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let mut mixed = fixture();
    mixed["status"] = json!("resolved");
    mixed["alerts"][0]["status"] = json!("resolved");
    let mut firing = fixture()["alerts"][0].clone();
    firing["fingerprint"] = json!("fedcba9876543210");
    mixed["alerts"].as_array_mut().unwrap().push(firing);
    ingest(&store, mixed).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!((snapshot.firing, snapshot.shadow_intents), (1, 3));
}

#[tokio::test]
async fn truncated_batch_records_coverage_gap_without_inventing_resolution() {
    let (_directory, _path, store, _app) = setup().await;
    ingest(&store, fixture()).await;
    let mut partial = fixture();
    partial["alerts"][0]["fingerprint"] = json!("fedcba9876543210");
    partial["truncatedAlerts"] = json!(5);
    let receipt = ingest(&store, partial).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            receipt.coverage_gap,
            snapshot.firing,
            snapshot.truncated_alerts
        ),
        (true, 2, Some(5))
    );
}

#[tokio::test]
async fn unrecognized_severity_uses_conservative_fallback_without_a_model() {
    let (_directory, _path, store, _app) = setup().await;
    let mut unknown = fixture();
    unknown["alerts"][0]["labels"]["severity"] = json!("unrecognized");
    let receipt = ingest(&store, unknown).await;
    assert_eq!(
        (receipt.decisions[0].action, receipt.decisions[0].reason),
        (Action::Ping, "unknown_severity_conservative_fallback")
    );
}

#[tokio::test]
async fn severity_escalation_creates_one_higher_priority_intent() {
    let (_directory, _path, store, _app) = setup().await;
    let mut warning = fixture();
    warning["alerts"][0]["labels"]["severity"] = json!("warning");
    ingest(&store, warning).await;
    ingest(&store, fixture()).await;
    ingest(&store, fixture()).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(snapshot.shadow_intents, 2);
}

#[tokio::test]
async fn authentication_failure_never_commits_an_event() {
    let (_directory, _path, store, app) = setup().await;
    let response = post(app, fixture(), "incorrect").await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (response.status(), snapshot.events),
        (StatusCode::UNAUTHORIZED, 0)
    );
}

#[tokio::test]
async fn unsupported_webhook_version_never_commits_an_event() {
    let (_directory, _path, store, app) = setup().await;
    let mut unsupported = fixture();
    unsupported["version"] = json!("5");
    let response = post(app, unsupported, TOKEN).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (response.status(), snapshot.events),
        (StatusCode::UNPROCESSABLE_ENTITY, 0)
    );
}

#[tokio::test]
async fn oversized_body_is_rejected_without_persisting() {
    let (_directory, _path, store, app) = setup().await;
    let mut oversized = fixture();
    oversized["alerts"][0]["annotations"]["summary"] = json!("x".repeat(1_048_576));
    let response = post(app, oversized, TOKEN).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (response.status(), snapshot.events),
        (StatusCode::PAYLOAD_TOO_LARGE, 0)
    );
}

#[tokio::test]
async fn readiness_write_leaves_observation_history_unchanged() {
    let (_directory, _path, store, app) = setup().await;
    ingest(&store, fixture()).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            response.status(),
            snapshot.events,
            snapshot.occurrences,
            snapshot.shadow_intents
        ),
        (StatusCode::NO_CONTENT, 1, 1, 1)
    );
}

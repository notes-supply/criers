use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

use crate::tests::{TOKEN, fixture, post, setup};

#[tokio::test]
async fn readiness_fails_when_storage_is_read_only() {
    let (_directory, _path, store, app) = setup().await;
    store
        .with_connection(|connection| {
            connection.execute_batch("PRAGMA query_only = ON")?;
            Ok(())
        })
        .await
        .unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn read_only_database_returns_retryable_failure_instead_of_false_acceptance() {
    let (_directory, _path, store, app) = setup().await;
    store
        .with_connection(|connection| {
            connection.execute_batch("PRAGMA query_only = ON")?;
            Ok(())
        })
        .await
        .unwrap();
    let response = post(app, fixture(), TOKEN).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (response.status(), snapshot.events, snapshot.shadow_intents),
        (StatusCode::SERVICE_UNAVAILABLE, 0, 0)
    );
}

#[tokio::test]
async fn full_database_rolls_back_event_decision_and_intent_together() {
    let (_directory, _path, store, app) = setup().await;
    store
        .with_connection(|connection| {
            let pages: u32 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
            connection.pragma_update(None, "max_page_count", pages)?;
            Ok(())
        })
        .await
        .unwrap();
    let mut full = fixture();
    let annotations = full["alerts"][0]["annotations"].as_object_mut().unwrap();
    for index in 0..32 {
        annotations.insert(format!("detail{index}"), json!("x".repeat(8192)));
    }
    let response = post(app, full, TOKEN).await;
    let snapshot = store.snapshot("example".into()).await.unwrap();
    assert_eq!(
        (
            response.status(),
            snapshot.events,
            snapshot.occurrences,
            snapshot.shadow_intents
        ),
        (StatusCode::SERVICE_UNAVAILABLE, 0, 0, 0)
    );
}

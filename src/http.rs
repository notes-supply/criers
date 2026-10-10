use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

use crate::{
    model::Webhook,
    store::{Receipt, Snapshot, Store, StoreError},
};

const MAX_BODY_BYTES: usize = 1_048_576;

#[derive(Clone)]
pub(crate) struct AppState {
    pub store: Store,
    pub source: String,
    pub token: Arc<str>,
    pub admission: Arc<Semaphore>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppError {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("ingestion capacity is occupied; retry later")]
    Busy,
    #[error(transparent)]
    Storage(#[from] StoreError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Invalid(reason) => (StatusCode::UNPROCESSABLE_ENTITY, reason),
            Self::Busy => (StatusCode::SERVICE_UNAVAILABLE, "ingestion busy; retry"),
            Self::Storage(error) => {
                tracing::error!(%error, "database operation failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage unavailable; retry",
                )
            }
        };
        let mut response = (status, Json(json!({ "error": message }))).into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        response
    }
}

pub(crate) fn router(state: AppState) -> Router {
    let authenticated = Router::new()
        .route("/v1/alertmanager", post(ingest))
        .route("/v1/status", get(snapshot))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    Router::new()
        .merge(authenticated)
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn authorize(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match supplied {
        Some(token) if bool::from(token.as_bytes().ct_eq(state.token.as_bytes())) => {
            let Ok(_permit) = state.admission.try_acquire() else {
                return AppError::Busy.into_response();
            };
            next.run(request).await
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "valid bearer credential required" })),
        )
            .into_response(),
    }
}

async fn ingest(
    State(state): State<AppState>,
    Json(webhook): Json<Webhook>,
) -> Result<(StatusCode, Json<Receipt>), AppError> {
    webhook.validate().map_err(AppError::Invalid)?;
    let receipt = state.store.ingest(state.source, webhook).await?;
    Ok((StatusCode::ACCEPTED, Json(receipt)))
}

async fn snapshot(State(state): State<AppState>) -> Result<Json<Snapshot>, AppError> {
    Ok(Json(state.store.snapshot(state.source).await?))
}

async fn ready(State(state): State<AppState>) -> Result<StatusCode, AppError> {
    let _permit = state.admission.try_acquire().map_err(|_| AppError::Busy)?;
    state
        .store
        .with_connection(|connection| {
            // A rollback-only write proves writability without altering alert history.
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO events(source, received_at, payload) VALUES ('readiness', '', '')",
                [],
            )?;
            transaction.rollback()?;
            Ok(())
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

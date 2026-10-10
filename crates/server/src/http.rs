//! Authenticated per-source ingestion and a separate unauthenticated ops listener.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use criers_core::{Status, Webhook};
use http_body_util::LengthLimitError;
use serde_json::json;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

use crate::{
    config::Config,
    credentials::CredentialSet,
    metrics::{DecisionLabels, Metrics, SourceLabels, SourceOutcome, SourceStatus},
    store::{Receipt, Snapshot, Store, StoreError},
};

/// Shared state of both listeners.
#[derive(Clone)]
pub(crate) struct AppState {
    pub config: Arc<Config>,
    pub credentials: Arc<BTreeMap<String, CredentialSet>>,
    pub store: Store,
    pub admission: Arc<Admission>,
    pub metrics: Arc<Metrics>,
    pub wake: Arc<Notify>,
    pub draining: Arc<AtomicBool>,
    pub ready_window_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppError {
    #[error("{0}")]
    Invalid(String),
    #[error(
        "request body exceeds {0} bytes; lower Alertmanager max_alerts or raise server.max_body_bytes"
    )]
    TooLarge(usize),
    #[error("request body could not be read")]
    BodyRead,
    #[error("ingestion capacity is occupied; retry later")]
    Busy,
    #[error("shutting down; retry later")]
    Draining,
    #[error(transparent)]
    Storage(#[from] StoreError),
}

impl AppError {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid",
            Self::TooLarge(_) => "too_large",
            Self::BodyRead => "body_error",
            Self::Busy => "busy",
            Self::Draining => "draining",
            Self::Storage(_) => "storage_error",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Invalid(reason) => (StatusCode::UNPROCESSABLE_ENTITY, reason),
            Self::TooLarge(_) => (StatusCode::PAYLOAD_TOO_LARGE, self.to_string()),
            Self::BodyRead => (StatusCode::BAD_REQUEST, self.to_string()),
            Self::Busy | Self::Draining => (StatusCode::SERVICE_UNAVAILABLE, self.to_string()),
            Self::Storage(error) => {
                tracing::error!(%error, "storage operation failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage unavailable; retry".to_owned(),
                )
            }
        };
        let mut response = (status, Json(json!({ "error": message }))).into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
        }
        response
    }
}

/// Bounded ingestion admission: a few slots plus a short, capped waiting line.
///
/// A burst of group flushes queues briefly instead of failing at once; beyond
/// the waiting line or the wait limit, callers get a retryable 503.
pub(crate) struct Admission {
    slots: Arc<Semaphore>,
    waiting: Semaphore,
    wait: Duration,
}

impl Admission {
    pub(crate) fn new(slots: usize, wait: Duration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots)),
            waiting: Semaphore::new(slots.saturating_mul(4)),
            wait,
        }
    }

    async fn enter(&self) -> Result<OwnedSemaphorePermit, AppError> {
        if let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() {
            return Ok(permit);
        }
        let _queued = self.waiting.try_acquire().map_err(|_| AppError::Busy)?;
        match tokio::time::timeout(self.wait, Arc::clone(&self.slots).acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) | Err(_) => Err(AppError::Busy),
        }
    }
}

/// The authenticated ingestion listener.
pub(crate) fn ingest_router(state: AppState) -> Router {
    let timeout = state.config.server.request_timeout;
    Router::new()
        .route("/v1/sources/{source}/alertmanager", post(ingest))
        .route("/v1/sources/{source}/status", get(snapshot))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        // The ingest handler enforces server.max_body_bytes itself after admission.
        .layer(DefaultBodyLimit::disable())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            timeout,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Liveness, readiness and metrics; never authenticated, never admits alerts.
pub(crate) fn ops_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_secs(10),
        ))
        .with_state(state)
}

/// Source identity is the URL path plus a matching credential for that source.
async fn authorize(
    State(state): State<AppState>,
    Path(source): Path<String>,
    request: Request,
    next: Next,
) -> Response {
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let credentials = state.credentials.get(&source);
    // Unknown sources and wrong tokens are indistinguishable to the caller.
    let authenticated = match (credentials, presented) {
        (Some(set), Some(token)) => set.verify(token.as_bytes()),
        _ => false,
    };
    if authenticated {
        return next.run(request).await;
    }
    let label = if credentials.is_some() {
        source
    } else {
        "unknown".to_owned()
    };
    record(&state, label, "unauthorized");
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "valid bearer credential for this source required" })),
    )
        .into_response()
}

fn record(state: &AppState, source: String, outcome: &'static str) {
    state
        .metrics
        .ingest_requests
        .get_or_create(&SourceOutcome { source, outcome })
        .inc();
}

/// Admission happens before the body is read, so a full ingestion pipeline
/// rejects quickly with a retryable 503 instead of buffering more input.
async fn ingest(
    State(state): State<AppState>,
    Path(source): Path<String>,
    request: Request,
) -> Response {
    let started = Instant::now();
    match admit(&state, &source, request).await {
        Ok(admitted) => {
            state.wake.notify_waiters();
            observe(&state, &source, &admitted);
            state
                .metrics
                .ingest_duration
                .observe(started.elapsed().as_secs_f64());
            record(&state, source, "accepted");
            (StatusCode::ACCEPTED, Json(admitted.receipt)).into_response()
        }
        Err(error) => {
            record(&state, source, error.outcome());
            error.into_response()
        }
    }
}

const INLINE_DECODE_BYTES: usize = 65_536;

fn decode(body: &[u8]) -> Result<Webhook, AppError> {
    let webhook: Webhook = serde_json::from_slice(body).map_err(|error| {
        AppError::Invalid(format!("invalid Alertmanager webhook JSON: {error}"))
    })?;
    webhook
        .validate()
        .map_err(|reason| AppError::Invalid(reason.to_owned()))?;
    Ok(webhook)
}

struct Admitted {
    receipt: Receipt,
    firing: u64,
    resolved: u64,
}

async fn admit(state: &AppState, source: &str, request: Request) -> Result<Admitted, AppError> {
    if state.draining.load(Ordering::Relaxed) {
        return Err(AppError::Draining);
    }
    let permit = state.admission.enter().await?;
    let limit = state.config.server.max_body_bytes;
    let body = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|error| {
            if error.into_inner().is::<LengthLimitError>() {
                AppError::TooLarge(limit)
            } else {
                AppError::BodyRead
            }
        })?;
    // Large batches are decoded on a blocking thread so they cannot stall the reactor.
    let webhook = if body.len() > INLINE_DECODE_BYTES {
        tokio::task::spawn_blocking(move || decode(&body))
            .await
            .map_err(|error| AppError::Storage(error.into()))??
    } else {
        decode(&body)?
    };
    let heartbeat = state
        .config
        .sources
        .get(source)
        .and_then(|source| source.heartbeat.as_ref())
        .map(|heartbeat| heartbeat.alertname.as_str());
    let (firing, resolved) = webhook
        .alerts
        .iter()
        .filter(|alert| Some(alert.alertname()) != heartbeat)
        .fold((0, 0), |(firing, resolved), alert| match alert.status {
            Status::Firing => (firing + 1, resolved),
            Status::Resolved => (firing, resolved + 1),
        });
    let receipt = state
        .store
        .ingest(
            Arc::clone(&state.config),
            source.to_owned(),
            webhook,
            permit,
        )
        .await?;
    Ok(Admitted {
        receipt,
        firing,
        resolved,
    })
}

fn observe(state: &AppState, source: &str, admitted: &Admitted) {
    let metrics = &state.metrics;
    if admitted.receipt.heartbeat {
        metrics
            .heartbeats
            .get_or_create(&SourceLabels {
                source: source.to_owned(),
            })
            .inc();
    }
    for decision in &admitted.receipt.decisions {
        metrics
            .decisions
            .get_or_create(&DecisionLabels {
                action: decision.action.as_str(),
                reason: decision.reason,
            })
            .inc();
    }
    for (status, count) in [("firing", admitted.firing), ("resolved", admitted.resolved)] {
        if count > 0 {
            metrics
                .alerts
                .get_or_create(&SourceStatus {
                    source: source.to_owned(),
                    status,
                })
                .inc_by(count);
        }
    }
}

/// Status reads share ingestion admission so they cannot queue unbounded work.
async fn snapshot(
    State(state): State<AppState>,
    Path(source): Path<String>,
) -> Result<Json<Snapshot>, AppError> {
    let _permit = state.admission.enter().await?;
    Ok(Json(state.store.snapshot(source).await?))
}

async fn ready(State(state): State<AppState>) -> Response {
    match state
        .store
        .health()
        .ready(crate::now_ms(), state.ready_window_ms)
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": reason })),
        )
            .into_response(),
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    match state.metrics.render() {
        Ok(text) => (
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "cannot encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use alert_router::model;
use anyhow::{Context, Result, bail};
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;

mod http;
mod store;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("alert_router=info,tower_http=info")),
        )
        .json()
        .init();
    let source = env::var("ALERT_ROUTER_SOURCE").context("ALERT_ROUTER_SOURCE is required")?;
    if source.is_empty()
        || source.len() > 64
        || !source
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("ALERT_ROUTER_SOURCE must be a 1 to 64 character ASCII source identifier");
    }
    let token_file =
        env::var("ALERT_ROUTER_TOKEN_FILE").context("ALERT_ROUTER_TOKEN_FILE is required")?;
    let token =
        std::fs::read_to_string(token_file).context("cannot read bearer credential file")?;
    let token = token.trim();
    if token.len() < 32 || token.len() > 256 || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        bail!("bearer credential must be 32 to 256 printable ASCII characters without whitespace");
    }
    let bind: SocketAddr = env::var("ALERT_ROUTER_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()
        .context("invalid bind address")?;
    let database =
        env::var("ALERT_ROUTER_DATABASE").unwrap_or_else(|_| "data/alerts.sqlite".into());
    let store = store::Store::open(PathBuf::from(database))
        .await
        .context("cannot initialize alert store; parent directory must already exist")?;
    let state = http::AppState {
        store,
        source,
        token: Arc::from(token),
        admission: Arc::new(Semaphore::new(8)),
    };
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .context("cannot bind receiver")?;
    tracing::info!(address = %listener.local_addr()?, mode = "shadow", "alert receiver listening");
    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result { tracing::error!(%error, "interrupt listener failed"); }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "termination listener failed");
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "interrupt listener failed");
                }
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "interrupt listener failed");
    }
}

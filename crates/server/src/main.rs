//! Criers: durable Alertmanager receiver, notification policy and Discord outbox.

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::{sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

mod assessment;
mod config;
mod credentials;
mod delivery;
mod http;
mod http_body;
mod metrics;
mod render;
mod store;
mod tasks;

#[cfg(test)]
mod tests;

use crate::{config::Config, credentials::Credentials, metrics::Metrics, store::Store};

#[derive(Parser)]
#[command(
    version,
    about = "Durable Alertmanager router with explicit notification policy"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the receiver, outbox and background workers.
    Serve {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
    /// Validate configuration and secret files without opening the database or sending.
    Check {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
}

/// Current wall-clock time in Unix milliseconds.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Check { config } => check(&config),
        Command::Serve { config } => {
            init_logging();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("cannot start async runtime")
                .and_then(|runtime| runtime.block_on(serve(&config)))
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if tracing::dispatcher::has_been_set() {
                tracing::error!(error = format!("{error:#}"), "criers stopped with an error");
            } else {
                eprintln!("error: {error:#}");
            }
            ExitCode::FAILURE
        }
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("criers=info,tower_http=warn")),
        )
        .json()
        .init();
}

fn check(path: &Path) -> Result<()> {
    let config = Config::load(path)?;
    println!(
        "configuration OK: mode={}, {} source(s), {} destination(s), assessment={}, policy={}",
        config.mode.as_str(),
        config.sources.len(),
        config.destinations.len(),
        config
            .assessment
            .as_ref()
            .map_or("off", |assessment| assessment.model),
        config.policy_version
    );
    for (id, source) in &config.sources {
        println!(
            "  source {id}: POST /v1/sources/{id}/alertmanager, {} credential file(s), default \
             destination {}, heartbeat {}",
            source.credential_files.len(),
            source.default_destination,
            source
                .heartbeat
                .as_ref()
                .map_or("off".to_owned(), |heartbeat| format!(
                    "{} (stale after {})",
                    heartbeat.alertname,
                    humantime::format_duration(heartbeat.stale_after)
                ))
        );
    }
    for warning in &config.warnings {
        println!("warning: {warning}");
    }
    Ok(())
}

async fn serve(path: &Path) -> Result<()> {
    let config = Arc::new(Config::load(path)?);
    for warning in &config.warnings {
        tracing::warn!(%warning, "configuration warning");
    }
    let credentials = Credentials::load(
        config
            .sources
            .iter()
            .map(|(id, source)| (id.clone(), source.credential_files.clone())),
    )
    .map_err(|(path, error)| anyhow::anyhow!("credential {}: {error}", path.display()))?;
    let credentials = Arc::new(credentials);
    let store = Store::open(config.storage.path.clone(), config.storage.max_bytes)
        .await
        .with_context(|| format!("cannot open database {}", config.storage.path.display()))?;
    let metrics = Arc::new(Metrics::new());
    let wake = Arc::new(Notify::new());
    let draining = Arc::new(AtomicBool::new(false));
    let ready_window_ms =
        i64::try_from(config.storage.health_interval.as_millis()).unwrap_or(i64::MAX / 8) * 3
            + 5000;
    let state = http::AppState {
        config: Arc::clone(&config),
        credentials: Arc::clone(&credentials),
        store: store.clone(),
        admission: Arc::new(http::Admission::new(
            config.server.ingest_concurrency,
            config.server.admission_wait,
        )),
        metrics: Arc::clone(&metrics),
        wake: Arc::clone(&wake),
        draining: Arc::clone(&draining),
        ready_window_ms,
    };
    let ingest_listener = tokio::net::TcpListener::bind(config.server.listen)
        .await
        .with_context(|| format!("cannot bind server.listen {}", config.server.listen))?;
    let ops_listener = tokio::net::TcpListener::bind(config.server.ops_listen)
        .await
        .with_context(|| format!("cannot bind server.ops_listen {}", config.server.ops_listen))?;

    // Finish fallible preparation before background workers can start writing.
    let mut delivery_workers = Vec::new();
    if config.mode == config::Mode::Live {
        let abandoned = store
            .abandon_unconfigured(config.destinations.keys().cloned().collect())
            .await
            .context("cannot reconcile deliveries for removed destinations")?;
        if abandoned > 0 {
            tracing::warn!(
                abandoned,
                "ended open deliveries for destinations no longer configured"
            );
        }
        for (id, destination) in &config.destinations {
            delivery_workers.push(delivery::Worker {
                destination: id.clone(),
                discord: delivery::Discord::new(destination, config.delivery.timeout)
                    .with_context(|| format!("destinations.{id}"))?,
                mention_cooldown: destination.mention_cooldown,
                store: store.clone(),
                metrics: Arc::clone(&metrics),
                wake: Arc::clone(&wake),
                settings: config.delivery,
            });
        }
    }
    let clef = config
        .assessment
        .as_ref()
        .map(|settings| assessment::Clef::new(Arc::clone(settings)))
        .transpose()
        .context("cannot build assessment client")?;

    let cancel = CancellationToken::new();
    let mut workers = JoinSet::new();
    let started_at = now_ms();
    workers.spawn(tasks::observe(
        store.clone(),
        Arc::clone(&config),
        Arc::clone(&metrics),
        ready_window_ms,
        cancel.child_token(),
    ));
    workers.spawn(tasks::retention(
        store.clone(),
        Arc::clone(&config),
        Arc::clone(&metrics),
        cancel.child_token(),
    ));
    workers.spawn(tasks::coverage(
        store.clone(),
        Arc::clone(&config),
        Arc::clone(&wake),
        started_at,
        cancel.child_token(),
    ));
    workers.spawn(tasks::credentials(
        Arc::clone(&credentials),
        Arc::clone(&metrics),
        config.server.credential_reload_interval,
        cancel.child_token(),
    ));
    for worker in delivery_workers {
        workers.spawn(worker.run(cancel.child_token()));
    }
    if let Some(clef) = clef {
        workers.spawn(assessment::run(
            store.clone(),
            clef,
            Arc::clone(&metrics),
            Arc::clone(&wake),
            cancel.child_token(),
        ));
    }

    tracing::info!(
        listen = %ingest_listener.local_addr()?,
        ops = %ops_listener.local_addr()?,
        mode = config.mode.as_str(),
        sources = config.sources.len(),
        policy = %config.policy_version,
        "criers listening"
    );
    let shutdown = CancellationToken::new();
    let ingest_server = axum::serve(ingest_listener, http::ingest_router(state.clone()))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let ops_server = axum::serve(ops_listener, http::ops_router(state))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let mut servers = JoinSet::new();
    servers.spawn(async move { ingest_server.await.context("ingestion server failed") });
    servers.spawn(async move { ops_server.await.context("ops server failed") });

    let failure = tokio::select! {
        () = shutdown_signal() => None,
        Some(result) = servers.join_next() => Some(result),
    };
    tracing::info!("shutting down: admission stopped");
    draining.store(true, std::sync::atomic::Ordering::Relaxed);
    shutdown.cancel();
    let grace = config.server.shutdown_grace;
    // In-flight requests finish their committed transactions before servers return.
    if tokio::time::timeout(grace, async {
        while servers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!("HTTP servers did not drain within the shutdown grace");
        servers.abort_all();
    }
    cancel.cancel();
    if tokio::time::timeout(grace, async {
        while workers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!(
            "workers did not stop within the shutdown grace; in-flight sends resume as possible duplicates after restart"
        );
        workers.abort_all();
    }
    tracing::info!("shutdown complete");
    match failure {
        Some(Ok(Ok(()))) | None => Ok(()),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(error.into()),
    }
}

async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "interrupt listener failed");
            std::future::pending::<()>().await;
        }
    };
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                () = interrupt => {}
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            tracing::error!(%error, "termination listener failed");
            interrupt.await;
        }
    }
}

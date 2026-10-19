//! Discord payloads for visual review, captured from the real delivery path.
//!
//! Run explicitly:
//!
//! ```text
//! CRIERS_PREVIEW_DIR=/tmp/preview cargo nextest run --run-ignored only -E 'test(preview)'
//! ```
//!
//! Representative alerts, coverage changes and summaries go through ingestion,
//! the store, the renderer and the delivery worker into a mock webhook. Each
//! file is one request body exactly as Discord would receive it. All alert data
//! is synthetic.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    TOKEN, USER_ID, fixture,
    fixtures::{Captured, MockProvider},
    with,
};
use crate::{
    config::Config,
    delivery::{Discord, Worker},
    metrics::Metrics,
    render::{EMBEDS_TOTAL_LIMIT, Embed, MAX_EMBEDS, units},
    store::Store,
    summary::DAY_MS,
};

const HOUR: i64 = 3_600_000;

/// One single-source deployment with a live mock webhook.
struct Deployment {
    _directory: tempfile::TempDir,
    config: Arc<Config>,
    store: Store,
    discord: MockProvider,
    seen: usize,
}

impl Deployment {
    async fn new(policy: &str, extra: &str) -> Self {
        let discord = MockProvider::discord(Vec::new()).await;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("cluster.token"), TOKEN).unwrap();
        std::fs::write(root.join("ops.url"), discord.discord_url()).unwrap();
        // Windows end two minutes ago, so a summary reads as on time.
        let at = chrono::Utc::now() - chrono::Duration::minutes(2);
        std::fs::write(
            root.join("criers.toml"),
            format!(
                "mode = \"live\"\n[storage]\npath = \"alerts.sqlite\"\n\
                 [policy.severity]\n{policy}\n\
                 [sources.cluster]\ncredential_files = [\"cluster.token\"]\n\
                 default_destination = \"ops\"\n\
                 [destinations.ops]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n\
                 mention_users = [\"{USER_ID}\"]\nmention_cooldown = \"0s\"\n\
                 [summary]\ndestination = \"ops\"\nat = \"{}\"\n{extra}",
                at.format("%H:%M")
            ),
        )
        .unwrap();
        let config = Arc::new(Config::load(&root.join("criers.toml")).unwrap());
        let store = Store::open(config.storage.path.clone(), config.storage.max_bytes)
            .await
            .unwrap();
        Self {
            _directory: directory,
            config,
            store,
            discord,
            seen: 0,
        }
    }

    async fn ingest(&self, webhook: Value) {
        let webhook: criers_core::Webhook = serde_json::from_value(webhook).unwrap();
        webhook.validate().unwrap();
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        self.store
            .ingest(
                Arc::clone(&self.config),
                "cluster".into(),
                webhook,
                permit,
                |_| {},
            )
            .await
            .unwrap();
    }

    async fn sql(&self, sql: &'static str, value: i64) {
        self.store
            .with_connection(move |connection| {
                connection.execute(sql, [value])?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Delivers everything pending and returns the messages it produced.
    async fn deliver(&mut self, messages: usize) -> Vec<Captured> {
        let destination = &self.config.destinations["ops"];
        let cancel = CancellationToken::new();
        tokio::spawn(
            Worker {
                destination: "ops".into(),
                discord: Discord::new(destination, Duration::from_secs(2)).unwrap(),
                mention_cooldown: destination.mention_cooldown,
                store: self.store.clone(),
                metrics: Arc::new(Metrics::new()),
                wake: Arc::new(Notify::new()),
                settings: self.config.delivery,
            }
            .run(cancel.clone()),
        );
        let requests = self.discord.wait_for(self.seen + messages).await;
        cancel.cancel();
        let new = requests[self.seen..].to_vec();
        self.seen = requests.len();
        new
    }

    /// Queues the summary of the window that ended two minutes ago, with every
    /// alert so far placed inside it and notifications marked sent. Heartbeats,
    /// if configured, have been watched for three hours without arriving.
    async fn summary(&mut self) -> Captured {
        let now = crate::now_ms();
        self.store
            .summary_tick(Arc::clone(&self.config), now)
            .await
            .unwrap();
        self.sql("UPDATE summaries SET window_end = window_end - ?1", DAY_MS)
            .await;
        self.sql(
            "UPDATE occurrences SET first_seen_at = first_seen_at - ?1,
                closed_at = closed_at - ?1",
            HOUR,
        )
        .await;
        self.store
            .coverage_tick(Arc::clone(&self.config), now, now)
            .await
            .unwrap();
        self.sql(
            "UPDATE source_status SET unobserved_since = ?1",
            now - 3 * HOUR,
        )
        .await;
        self.sql(
            "UPDATE outbox SET state = 'sent', completed_at = ?1
             WHERE state = 'pending' AND kind <> 'summary'",
            now,
        )
        .await;
        self.store
            .summary_tick(Arc::clone(&self.config), now)
            .await
            .unwrap();
        self.deliver(1).await.remove(0)
    }
}

/// A synthetic alert.
fn alert(
    alertname: &str,
    severity: &str,
    fingerprint: u64,
    labels: &[(&str, &str)],
    summary: &str,
) -> Value {
    let mut all = json!({"alertname": alertname, "severity": severity});
    for (key, value) in labels {
        all[*key] = json!(value);
    }
    let value = with(fixture(), "/alerts/0/labels", all);
    let value = with(
        value,
        "/alerts/0/fingerprint",
        json!(format!("{fingerprint:016x}")),
    );
    with(value, "/alerts/0/annotations", json!({"summary": summary}))
}

fn resolved(value: Value) -> Value {
    let value = with(value, "/alerts/0/status", json!("resolved"));
    with(value, "/alerts/0/endsAt", json!("2026-10-10T11:15:00Z"))
}

/// Several alerts in one webhook, as Alertmanager groups them.
fn grouped(alerts: Vec<Value>) -> Value {
    with(
        fixture(),
        "/alerts",
        Value::Array(
            alerts
                .into_iter()
                .map(|value| value["alerts"][0].clone())
                .collect(),
        ),
    )
}

/// Writes one captured request and checks it against Discord's limits.
fn write(directory: &Path, name: &str, request: &Captured) {
    let embeds: Vec<Embed> =
        serde_json::from_value(request.body["embeds"].clone()).unwrap_or_default();
    let content = request.body["content"].as_str().unwrap_or_default();
    assert!(
        embeds.len() <= MAX_EMBEDS
            && embeds.iter().all(Embed::within_limits)
            && embeds.iter().map(Embed::units).sum::<usize>() <= EMBEDS_TOTAL_LIMIT
            && units(content) <= crate::render::CONTENT_LIMIT,
        "{name} exceeds a Discord limit"
    );
    std::fs::write(
        directory.join(format!("{name}.json")),
        serde_json::to_string_pretty(&request.body).unwrap() + "\n",
    )
    .unwrap();
}

/// Open and resolved alerts resembling one busy day on a Kubernetes cluster.
async fn busy_day(deployment: &Deployment) {
    let mut fingerprint = 0x1000;
    let mut next = || {
        fingerprint += 1;
        fingerprint
    };
    for pod in ["api-7c9d", "api-8f21", "worker-5b10"] {
        let namespace = if pod.starts_with("api") {
            "payments"
        } else {
            "billing"
        };
        deployment
            .ingest(alert(
                "KubeDeploymentReplicasMismatch",
                "critical",
                next(),
                &[("namespace", namespace), ("pod", pod)],
                "Deployment has not matched the expected number of replicas",
            ))
            .await;
    }
    deployment
        .ingest(alert(
            "BackupStale",
            "critical",
            next(),
            &[("job", "offsite-backup")],
            "Offsite backup is missing, suspended or overdue",
        ))
        .await;
    deployment
        .ingest(alert(
            "PublicEndpointDown",
            "critical",
            next(),
            &[("service", "status-page")],
            "Public status page is unavailable",
        ))
        .await;
    for host in ["worker-1", "worker-2"] {
        deployment
            .ingest(alert(
                "NodeFilesystemAlmostFull",
                "warning",
                next(),
                &[("host", host)],
                "Filesystem has less than 10% space left",
            ))
            .await;
    }
    for name in ["apps", "monitoring", "runners"] {
        deployment
            .ingest(alert(
                "FluxReconciliationFailed",
                "warning",
                next(),
                &[("namespace", "flux-system"), ("deployment", name)],
                "Flux could not reconcile a resource",
            ))
            .await;
    }
    deployment
        .ingest(alert(
            "CertificateExpiringSoon",
            "warning",
            next(),
            &[("namespace", "ingress"), ("container", "edge")],
            "Certificate expires in less than 14 days",
        ))
        .await;
    for host in ["db-1", "db-2", "cache-1", "cache-2"] {
        let target = alert(
            "TargetDown",
            "critical",
            next(),
            &[("host", host)],
            "Prometheus cannot scrape this target",
        );
        deployment.ingest(target.clone()).await;
        deployment.ingest(resolved(target)).await;
    }
    for daemonset in ["kube-proxy", "node-exporter"] {
        let rollout = alert(
            "KubeDaemonSetRolloutStuck",
            "warning",
            next(),
            &[("namespace", "kube-system"), ("daemonset", daemonset)],
            "DaemonSet rollout is stuck",
        );
        deployment.ingest(rollout.clone()).await;
        deployment.ingest(resolved(rollout)).await;
    }
    // Opened before the window: still open, but not new.
    deployment
        .sql(
            "UPDATE occurrences SET first_seen_at = first_seen_at - ?1
             WHERE alertname = 'KubeDeploymentReplicasMismatch' AND labels LIKE '%worker%'",
            DAY_MS,
        )
        .await;
}

#[tokio::test]
#[ignore = "writes Discord preview payloads; set CRIERS_PREVIEW_DIR"]
async fn preview() {
    let directory = PathBuf::from(
        std::env::var("CRIERS_PREVIEW_DIR").expect("CRIERS_PREVIEW_DIR names the output directory"),
    );
    std::fs::create_dir_all(&directory).unwrap();
    let paging = "critical = \"ping\"\nwarning = \"quiet_post\"\ninfo = \"record\"";
    let digesting = "critical = \"ping\"\nwarning = \"digest\"\ninfo = \"record\"";

    // Live notifications.
    let mut alerts = Deployment::new(
        paging,
        "[sources.cluster.heartbeat]\nstale_after = \"5m\"\n",
    )
    .await;
    let critical = alert(
        "KubeDeploymentReplicasMismatch",
        "critical",
        0x0001,
        &[("namespace", "payments"), ("pod", "api-7c9d")],
        "Deployment payments/api has not matched the expected number of replicas",
    );
    alerts.ingest(critical.clone()).await;
    write(
        &directory,
        "01-firing-critical",
        &alerts.deliver(1).await[0],
    );
    alerts
        .ingest(alert(
            "NodeFilesystemAlmostFull",
            "warning",
            0x0002,
            &[("host", "worker-2"), ("mountpoint", "/var")],
            "Filesystem /var on worker-2 has less than 10% space left",
        ))
        .await;
    write(&directory, "02-firing-warning", &alerts.deliver(1).await[0]);
    alerts.ingest(resolved(critical)).await;
    write(
        &directory,
        "03-recovered-critical",
        &alerts.deliver(1).await[0],
    );
    alerts
        .ingest(grouped(vec![
            alert(
                "TargetDown",
                "critical",
                0x0003,
                &[("host", "db-1")],
                "Prometheus cannot scrape db-1",
            ),
            alert(
                "TargetDown",
                "critical",
                0x0004,
                &[("host", "db-2")],
                "Prometheus cannot scrape db-2",
            ),
            alert(
                "PodCrashLooping",
                "warning",
                0x0005,
                &[("namespace", "media"), ("pod", "transcoder-0")],
                "Pod media/transcoder-0 is restarting repeatedly",
            ),
        ]))
        .await;
    write(&directory, "04-grouped-batch", &alerts.deliver(1).await[0]);
    let hostile = "**Disk** <@&123456789012345678> @everyone [link](https://example.invalid) `x` ";
    alerts
        .ingest(alert(
            &format!("VeryLongRuleName{}", "Segment".repeat(40)),
            "error",
            0x0006,
            &[
                ("namespace", &"n".repeat(300)),
                ("pod", "\u{1F525}".repeat(200).as_str()),
            ],
            &format!("{hostile}{}", "\u{1F525} long annotation ".repeat(120)),
        ))
        .await;
    write(
        &directory,
        "05-long-hostile-input",
        &alerts.deliver(1).await[0],
    );
    // Heartbeats arrived, then stopped twenty minutes ago.
    alerts
        .ingest(alert("Watchdog", "none", 0x0007, &[], "Heartbeat"))
        .await;
    let now = crate::now_ms();
    alerts
        .store
        .coverage_tick(Arc::clone(&alerts.config), now, now)
        .await
        .unwrap();
    alerts
        .sql(
            "UPDATE source_status SET last_heartbeat_at = ?1",
            now - 20 * 60_000,
        )
        .await;
    alerts
        .store
        .coverage_tick(Arc::clone(&alerts.config), now, now)
        .await
        .unwrap();
    write(&directory, "06-coverage-lost", &alerts.deliver(1).await[0]);
    alerts
        .ingest(alert("Watchdog", "none", 0x0007, &[], "Heartbeat"))
        .await;
    let later = crate::now_ms();
    alerts
        .store
        .coverage_tick(Arc::clone(&alerts.config), later, now)
        .await
        .unwrap();
    write(
        &directory,
        "07-coverage-restored",
        &alerts.deliver(1).await[0],
    );

    // Daily summaries.
    let mut busy = Deployment::new(digesting, "").await;
    busy_day(&busy).await;
    write(&directory, "10-summary-busy", &busy.summary().await);

    let mut healthy = Deployment::new(digesting, "").await;
    let fixed = alert(
        "TargetDown",
        "critical",
        0x0100,
        &[("host", "db-1")],
        "Prometheus cannot scrape db-1",
    );
    healthy.ingest(fixed.clone()).await;
    healthy.ingest(resolved(fixed)).await;
    write(&directory, "11-summary-healthy", &healthy.summary().await);

    let mut gap = Deployment::new(
        digesting,
        "[sources.cluster.heartbeat]\nstale_after = \"10m\"\n",
    )
    .await;
    gap.ingest(alert(
        "NodeFilesystemAlmostFull",
        "warning",
        0x0200,
        &[("host", "worker-1")],
        "Filesystem has less than 10% space left",
    ))
    .await;
    write(&directory, "12-summary-coverage-gap", &gap.summary().await);

    let mut long = Deployment::new(digesting, "").await;
    for index in 0..40u64 {
        long.ingest(alert(
            &format!("{}{index}", "VeryLongAlertName".repeat(5)),
            if index % 3 == 0 {
                "critical"
            } else {
                "warning"
            },
            0x0300 + index,
            &[
                ("namespace", &"namespace".repeat(8)),
                ("pod", &"\u{1F525}".repeat(30)),
            ],
            &"A long annotation that keeps going. ".repeat(10),
        ))
        .await;
    }
    write(&directory, "13-summary-long-input", &long.summary().await);

    let mut icons = Deployment::new(
        digesting,
        "[destinations.ops.icons]\n\
         critical = \"<:criers_critical:100000000000000001>\"\n\
         warning = \"<:criers_warning:100000000000000002>\"\n\
         info = \"<:criers_info:100000000000000003>\"\n\
         recovered = \"<:criers_recovered:100000000000000004>\"\n\
         coverage = \"<:criers_coverage:100000000000000005>\"\n",
    )
    .await;
    busy_day(&icons).await;
    write(
        &directory,
        "14-summary-busy-custom-icons",
        &icons.summary().await,
    );
}

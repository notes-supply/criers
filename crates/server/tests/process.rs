//! Exercises the compiled `criers` executable with real SQLite and a local mock
//! Discord fixture. The fixture is a test double; nothing leaves the loopback interface.

use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{Router, body::Bytes, extract::State, routing::post};
use serde_json::{Value, json};

const TOKEN: &str = "synthetic-process-token-not-a-live-secret-0001";
const ROTATED: &str = "synthetic-rotated-token-not-a-live-secret-0002";

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_criers")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Mock Discord webhook fixture that records request bodies.
#[derive(Clone, Default)]
struct MockDiscord {
    bodies: Arc<Mutex<Vec<Value>>>,
}

impl MockDiscord {
    async fn start() -> (Self, SocketAddr) {
        let mock = Self::default();
        let app = Router::new()
            .fallback(post(
                |State(mock): State<MockDiscord>, body: Bytes| async move {
                    mock.bodies
                        .lock()
                        .unwrap()
                        .push(serde_json::from_slice(&body).unwrap_or(Value::Null));
                    axum::Json(json!({"id": "300000000000000001"}))
                },
            ))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (mock, address)
    }

    fn count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }
}

struct Deployment {
    directory: tempfile::TempDir,
    config: PathBuf,
    ingest: SocketAddr,
    ops: SocketAddr,
}

impl Deployment {
    fn new(mode: &str, discord: Option<SocketAddr>, extra: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir(root.join("data")).unwrap();
        std::fs::create_dir(root.join("secrets")).unwrap();
        std::fs::write(root.join("secrets/token"), TOKEN).unwrap();
        std::fs::write(root.join("secrets/token.next"), ROTATED).unwrap();
        let url = discord.map_or_else(
            || "https://discord.invalid/api/webhooks/123456789012345678/x".to_owned(),
            |address| format!("http://{address}/api/webhooks/123456789012345678/mock"),
        );
        std::fs::write(root.join("secrets/discord-url"), url).unwrap();
        let ingest = SocketAddr::from(([127, 0, 0, 1], free_port()));
        let ops = SocketAddr::from(([127, 0, 0, 1], free_port()));
        let config = root.join("criers.toml");
        std::fs::write(
            &config,
            format!(
                r#"mode = "{mode}"
[server]
listen = "{ingest}"
ops_listen = "{ops}"
shutdown_grace = "5s"
credential_reload_interval = "1s"
{extra}
[storage]
path = "data/criers.sqlite"
health_interval = "1s"
[sources.prod]
credential_files = ["secrets/token"]
default_destination = "ops"
[destinations.ops]
kind = "discord"
webhook_url_file = "secrets/discord-url"
mention_users = ["123456789012345678"]
"#
            ),
        )
        .unwrap();
        Self {
            directory,
            config,
            ingest,
            ops,
        }
    }

    fn database(&self) -> PathBuf {
        self.directory.path().join("data/criers.sqlite")
    }

    fn spawn(&self) -> Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.path().join("criers.log"))
            .unwrap();
        Command::new(binary())
            .args(["serve", "--config"])
            .arg(&self.config)
            .env("RUST_LOG", "criers=debug")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.directory.path().join("criers.log")).unwrap_or_default()
    }

    async fn wait_ready(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(response) = client
                .get(format!("http://{}/health/ready", self.ops))
                .send()
                .await
                && response.status() == 204
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("criers never became ready:\n{}", self.log());
    }

    async fn post(&self, client: &reqwest::Client, token: &str, payload: &Value) -> (u16, Value) {
        let response = client
            .post(format!(
                "http://{}/v1/sources/prod/alertmanager",
                self.ingest
            ))
            .bearer_auth(token)
            .json(payload)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

fn critical(fingerprint: &str) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../../../examples/critical.json")).unwrap();
    value["alerts"][0]["fingerprint"] = json!(fingerprint);
    value
}

fn terminate(child: &mut Child) -> ExitStatus {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("criers did not exit within 15 seconds of SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn count(database: &Path, sql: &str) -> i64 {
    let connection = rusqlite::Connection::open(database).unwrap();
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn check_validates_without_creating_a_database() {
    let deployment = Deployment::new("live", Some(SocketAddr::from(([127, 0, 0, 1], 9))), "");
    let output = Command::new(binary())
        .args(["check", "--config"])
        .arg(&deployment.config)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success()
            && stdout.contains("configuration OK: mode=live")
            && !stdout.contains(TOKEN)
            && !deployment.database().exists(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn check_reports_an_actionable_error_and_fails() {
    let deployment = Deployment::new("shadow", None, "max_body = 5");
    let output = Command::new(binary())
        .args(["check", "--config"])
        .arg(&deployment.config)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("unknown field `max_body`"),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn live_process_delivers_survives_restart_and_stops_gracefully() {
    let (discord, address) = MockDiscord::start().await;
    let deployment = Deployment::new("live", Some(address), "");
    let client = reqwest::Client::new();
    let mut child = deployment.spawn();
    deployment.wait_ready(&client).await;
    let (status, receipt) = deployment
        .post(&client, TOKEN, &critical("0123456789abcdef"))
        .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while discord.count() < 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let metrics = client
        .get(format!("http://{}/metrics", deployment.ops))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let exit = terminate(&mut child);

    let mut restarted = deployment.spawn();
    deployment.wait_ready(&client).await;
    let (duplicate_status, duplicate) = deployment
        .post(&client, TOKEN, &critical("0123456789abcdef"))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let second_exit = terminate(&mut restarted);
    let sent = count(
        &deployment.database(),
        "SELECT count(*) FROM outbox WHERE state = 'sent'",
    );
    let body = discord.bodies.lock().unwrap()[0].clone();
    assert!(
        status == 202
            && receipt["decisions"][0]["intent_created"] == json!(true)
            && body["allowed_mentions"]["users"] == json!(["123456789012345678"])
            && metrics
                .contains(r#"criers_delivery_attempts_total{destination="ops",outcome="sent"} 1"#)
            && exit.success()
            && duplicate_status == 202
            && duplicate["decisions"][0]["intent_created"] == json!(false)
            && second_exit.success()
            && discord.count() == 1
            && sent == 1,
        "status={status} receipt={receipt} exit={exit:?} duplicate={duplicate} sends={} \
         sent={sent}\n{}",
        discord.count(),
        deployment.log()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_stays_green_while_ingestion_admission_is_saturated() {
    let deployment = Deployment::new("shadow", None, "ingest_concurrency = 1");
    let client = reqwest::Client::new();
    let mut child = deployment.spawn();
    deployment.wait_ready(&client).await;
    let ops = deployment.ops;
    let probe_client = client.clone();
    let probes = tokio::spawn(async move {
        let mut statuses = Vec::new();
        for _ in 0..100 {
            let status = probe_client
                .get(format!("http://{ops}/health/ready"))
                .send()
                .await
                .map(|response| response.status().as_u16())
                .unwrap_or(0);
            statuses.push(status);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        statuses
    });
    let posts: Vec<_> = (0..300)
        .map(|index| {
            let client = client.clone();
            let payload = critical(&format!("{index:016x}"));
            let url = format!("http://{}/v1/sources/prod/alertmanager", deployment.ingest);
            tokio::spawn(async move {
                client
                    .post(url)
                    .bearer_auth(TOKEN)
                    .json(&payload)
                    .send()
                    .await
                    .map(|response| response.status().as_u16())
                    .unwrap_or(0)
            })
        })
        .collect();
    let mut accepted = 0;
    let mut busy = 0;
    for post in posts {
        match post.await.unwrap() {
            202 => accepted += 1,
            503 => busy += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    let statuses = probes.await.unwrap();
    let exit = terminate(&mut child);
    let stored = count(&deployment.database(), "SELECT count(*) FROM occurrences");
    assert!(
        statuses.iter().all(|&status| status == 204)
            && accepted + busy == 300
            && stored == accepted
            && exit.success(),
        "ready={:?} accepted={accepted} busy={busy} stored={stored}",
        statuses
            .iter()
            .filter(|&&status| status != 204)
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_under_load_loses_no_acknowledged_webhook() {
    let deployment = Deployment::new("shadow", None, "");
    let client = reqwest::Client::new();
    let mut child = deployment.spawn();
    deployment.wait_ready(&client).await;
    // Eight sequential streams keep requests in flight when SIGTERM arrives.
    let streams: Vec<_> = (0..8)
        .map(|stream| {
            let client = client.clone();
            let url = format!("http://{}/v1/sources/prod/alertmanager", deployment.ingest);
            tokio::spawn(async move {
                let mut acknowledged = Vec::new();
                for index in 0..500 {
                    let fingerprint = format!("{stream:08x}{index:08x}");
                    let status = client
                        .post(&url)
                        .bearer_auth(TOKEN)
                        .json(&critical(&fingerprint))
                        .send()
                        .await
                        .map(|response| response.status().as_u16());
                    match status {
                        Ok(202) => acknowledged.push(fingerprint),
                        Ok(503) => {}
                        _ => break,
                    }
                }
                acknowledged
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let exit = tokio::task::spawn_blocking(move || terminate(&mut child))
        .await
        .unwrap();
    let mut acknowledged = Vec::new();
    for stream in streams {
        acknowledged.extend(stream.await.unwrap());
    }
    let connection = rusqlite::Connection::open(deployment.database()).unwrap();
    let missing: Vec<&String> = acknowledged
        .iter()
        .filter(|fingerprint| {
            !connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM occurrences WHERE fingerprint = ?1)",
                    [fingerprint.as_str()],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap()
        })
        .collect();
    assert!(
        // Some, but not all, requests were acknowledged: SIGTERM landed mid-traffic.
        exit.success()
            && !acknowledged.is_empty()
            && acknowledged.len() < 4000
            && missing.is_empty(),
        "exit={exit:?} acknowledged={} missing={missing:?}\n{}",
        acknowledged.len(),
        deployment.log()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn credential_rotation_applies_and_invalid_updates_keep_the_last_good_token() {
    let deployment = Deployment::new("shadow", None, "");
    let config = std::fs::read_to_string(&deployment.config)
        .unwrap()
        .replace(
            r#"credential_files = ["secrets/token"]"#,
            r#"credential_files = ["secrets/token", "secrets/token.next"]"#,
        );
    std::fs::write(&deployment.config, config).unwrap();
    let client = reqwest::Client::new();
    let mut child = deployment.spawn();
    deployment.wait_ready(&client).await;
    let (overlap_old, _) = deployment
        .post(&client, TOKEN, &critical("0000000000000001"))
        .await;
    let (overlap_new, _) = deployment
        .post(&client, ROTATED, &critical("0000000000000002"))
        .await;
    // Retire the old token by replacing the first file, as a Secret update would.
    let replacement = "synthetic-third-token-not-a-live-secret-0003";
    std::fs::write(
        deployment.directory.path().join("secrets/token"),
        replacement,
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (retired, _) = deployment
        .post(&client, TOKEN, &critical("0000000000000003"))
        .await;
    // A transient invalid update keeps the last good token and is exposed in metrics.
    std::fs::write(
        deployment.directory.path().join("secrets/token.next"),
        "bad",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (kept, _) = deployment
        .post(&client, ROTATED, &critical("0000000000000004"))
        .await;
    let metrics = client
        .get(format!("http://{}/metrics", deployment.ops))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let exit = terminate(&mut child);
    assert!(
        (overlap_old, overlap_new, retired, kept) == (202, 202, 401, 202)
            && metrics.contains(r#"criers_credential_files_valid{source="prod"} 1"#)
            && metrics.contains(r#"criers_credential_reload_failures_total{source="prod"}"#)
            && exit.success()
            && !deployment.log().contains(TOKEN),
        "{overlap_old} {overlap_new} {retired} {kept}\n{metrics}"
    );
}

//! Shadow assessment against a labelled mock Clef fixture.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Harness, fixture,
    fixtures::{MockProvider, Scripted},
    with,
};
use crate::assessment::{self, Clef};

const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";

fn answer(impact: f64) -> Scripted {
    Scripted::json(
        200,
        json!({
            "result": {
                "model": "clef-flash",
                "answers": {
                    "impact": {"type": "noul", "noul": impact},
                    "urgency": {"type": "score", "score": 1.2, "legend": {"0": "a", "1": "b", "2": "c", "3": "d"},
                        "probabilities": {"0": 0.2, "1": 0.5, "2": 0.2, "3": 0.1}, "confidence": 0.4}
                },
                "usage": {"input_tokens": 120, "output_tokens": 0}
            },
            "success": true, "errors": [], "messages": []
        }),
    )
}

async fn harness(clef: &MockProvider, extra: &str) -> Harness {
    Harness::shadow(&format!(
        "[assessment]\naccount_id = \"{ACCOUNT}\"\napi_token_file = \"clef.token\"\n\
         base_url = \"http://{}/client/v4\"\ntimeout = \"1s\"\nmax_attempts = 1\n{extra}",
        clef.address
    ))
    .await
}

fn spawn(harness: &Harness) -> CancellationToken {
    let cancel = CancellationToken::new();
    let clef = Clef::new(Arc::clone(harness.config.assessment.as_ref().unwrap())).unwrap();
    tokio::spawn(assessment::run(
        harness.store.clone(),
        clef,
        Arc::clone(&harness.state.metrics),
        Arc::clone(&harness.state.wake),
        cancel.clone(),
    ));
    cancel
}

async fn wait_for_outcome(harness: &Harness) -> Option<String> {
    for _ in 0..200 {
        let outcome: Option<String> = harness
            .query("SELECT (SELECT outcome FROM assessments WHERE state = 'done' LIMIT 1)")
            .await;
        if outcome.is_some() {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

fn warning() -> Value {
    let value = with(fixture(), "/alerts/0/labels/severity", json!("warning"));
    let mut value = with(value, "/alerts/0/labels/service", json!("checkout"));
    value["alerts"][0]["labels"]["private_label"] = json!("must-not-leave");
    value
}

#[tokio::test]
async fn warning_is_assessed_with_redacted_context_and_provenance() {
    let clef = MockProvider::start(Vec::new(), answer(0.8)).await;
    let harness = harness(&clef, "").await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    let request = &clef.requests()[0];
    let impact: f64 = harness
        .query("SELECT impact_probability FROM assessments")
        .await;
    let provenance: String = harness
        .query("SELECT requested_model || ' ' || model || ' ' || policy_version FROM assessments")
        .await;
    assert!(
        outcome.as_deref() == Some("assessed")
            && impact == 0.8
            && request.path
                == format!("/client/v4/accounts/{ACCOUNT}/ai/run/@cf/cloudflare/clef-flash")
            && request.authorization.as_deref() == Some("Bearer synthetic-clef-token-not-live")
            && request.body["state"]["labels"]["service"] == "checkout"
            && request.body["state"]["labels"]
                .get("private_label")
                .is_none()
            && provenance.starts_with("clef-flash clef-flash clef-impact-urgency-v1;policy-"),
        "outcome={outcome:?} impact={impact} provenance={provenance} request={request:?}"
    );
}

#[tokio::test]
async fn critical_floor_bypasses_assessment_entirely() {
    let clef = MockProvider::start(Vec::new(), answer(0.1)).await;
    let harness = harness(&clef, "").await;
    let receipt = harness.ingest(fixture()).await;
    let cancel = spawn(&harness);
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    let assessments: i64 = harness.query("SELECT count(*) FROM assessments").await;
    assert_eq!(
        (
            receipt.decisions[0].reason,
            receipt.decisions[0].intent_created,
            assessments,
            clef.requests().len()
        ),
        ("trusted_critical_floor", true, 0, 0)
    );
}

#[tokio::test]
async fn provider_timeout_is_recorded_without_touching_notifications() {
    let clef = MockProvider::start(Vec::new(), answer(0.9).delayed(Duration::from_secs(3))).await;
    let harness = harness(&clef, "").await;
    let receipt = harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    let intents: i64 = harness.query("SELECT count(*) FROM outbox").await;
    assert_eq!(
        (
            receipt.decisions[0].intent_created,
            outcome.as_deref(),
            intents
        ),
        (true, Some("timeout"), 1)
    );
}

#[tokio::test]
async fn malformed_provider_response_is_recorded_as_malformed() {
    let broken = Scripted::json(
        200,
        json!({"result": {"model": "clef-flash", "answers": {}}, "success": true}),
    );
    let clef = MockProvider::start(Vec::new(), broken).await;
    let harness = harness(&clef, "").await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    assert_eq!(outcome.as_deref(), Some("malformed"));
}

#[tokio::test]
async fn assessment_finishing_after_resolution_is_stale_and_announces_nothing() {
    let clef =
        MockProvider::start(Vec::new(), answer(0.99).delayed(Duration::from_millis(500))).await;
    let harness = harness(&clef, "").await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    clef.wait_for(1).await;
    harness
        .ingest(with(warning(), "/alerts/0/status", json!("resolved")))
        .await;
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    let intents: i64 = harness.query("SELECT count(*) FROM outbox").await;
    assert_eq!((outcome.as_deref(), intents), (Some("stale"), 1));
}

#[tokio::test]
async fn full_assessment_queue_skips_instead_of_growing() {
    let clef = MockProvider::start(Vec::new(), answer(0.5)).await;
    let harness = harness(&clef, "max_pending = 1\n").await;
    let mut batch = warning();
    let template = batch["alerts"][0].clone();
    batch["alerts"] = Value::Array(
        (0..3)
            .map(|index| {
                with(
                    template.clone(),
                    "/fingerprint",
                    json!(format!("{index:016x}")),
                )
            })
            .collect(),
    );
    harness.ingest(batch).await;
    let skipped: i64 = harness
        .query("SELECT count(*) FROM assessments WHERE outcome = 'skipped_queue_full'")
        .await;
    let pending: i64 = harness
        .query("SELECT count(*) FROM assessments WHERE state = 'pending'")
        .await;
    assert_eq!((skipped, pending), (2, 1));
}

#[tokio::test]
async fn assessment_in_live_delivery_never_changes_deterministic_routing() {
    let clef = MockProvider::start(Vec::new(), answer(0.99)).await;
    let harness = Harness::live(
        "http://127.0.0.1:9/api/webhooks/123456789012345678/x",
        &format!(
            "[assessment]\naccount_id = \"{ACCOUNT}\"\napi_token_file = \"clef.token\"\n\
             base_url = \"http://{}/client/v4\"\ntimeout = \"1s\"\nmax_attempts = 1\n",
            clef.address
        ),
    )
    .await;
    let mut batch = warning();
    let info = with(
        with(fixture(), "/alerts/0/labels/severity", json!("info")),
        "/alerts/0/fingerprint",
        json!("fedcba9876543210"),
    );
    batch["alerts"]
        .as_array_mut()
        .unwrap()
        .push(info["alerts"][0].clone());
    harness.ingest(batch).await;
    let before: String = harness
        .query("SELECT group_concat(kind || ':' || action || ':' || state) FROM outbox")
        .await;
    let cancel = spawn(&harness);
    let mut assessed = 0;
    for _ in 0..200 {
        assessed = harness
            .query("SELECT count(*) FROM assessments WHERE outcome = 'assessed'")
            .await;
        if assessed == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    let after: String = harness
        .query("SELECT group_concat(kind || ':' || action || ':' || state) FROM outbox")
        .await;
    assert_eq!(
        (assessed, before.as_str(), after.as_str()),
        (2, "notify:quiet_post:pending", "notify:quiet_post:pending")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn finished_result_is_recorded_after_a_transient_write_failure_without_restart() {
    let clef =
        MockProvider::start(Vec::new(), answer(0.7).delayed(Duration::from_millis(300))).await;
    let harness = harness(&clef, "").await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    clef.wait_for(1).await;
    let read_only = |enabled: bool| {
        harness.store.with_connection(move |connection| {
            connection.pragma_update(None, "query_only", enabled)?;
            Ok(())
        })
    };
    read_only(true).await.unwrap();
    // The provider answers while storage rejects writes.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let during: String = harness.query("SELECT state FROM assessments").await;
    let writable_at = crate::now_ms();
    read_only(false).await.unwrap();
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    // The completion time is when the result was durably recorded, so the
    // retention window starts after storage recovered.
    let completed_at: i64 = harness.query("SELECT completed_at FROM assessments").await;
    assert_eq!(
        (
            during.as_str(),
            outcome.as_deref(),
            clef.requests().len(),
            completed_at >= writable_at
        ),
        ("in_flight", Some("assessed"), 1, true),
        "completed_at={completed_at} writable_at={writable_at}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rate_limit_deadline_counts_from_the_delayed_provider_response() {
    let limited = Scripted::json(429, json!({"success": false}))
        .with_header("retry-after", "3")
        .delayed(Duration::from_millis(800));
    let clef = MockProvider::start(vec![limited], answer(0.5)).await;
    let harness = Harness::shadow(&format!(
        "[assessment]\naccount_id = \"{ACCOUNT}\"\napi_token_file = \"clef.token\"\n\
         base_url = \"http://{}/client/v4\"\ntimeout = \"2s\"\nmax_attempts = 2\n",
        clef.address
    ))
    .await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    clef.wait_for(1).await;
    // Observed after the request arrived, so at or after the provider call began.
    let requested_at = crate::now_ms();
    let mut due: Option<(String, i64)> = None;
    for _ in 0..200 {
        let row: Option<String> = harness
            .query(
                "SELECT (SELECT outcome || ' ' || due_at FROM assessments
                 WHERE state = 'pending' AND outcome IS NOT NULL)",
            )
            .await;
        due = row.map(|text| {
            let (outcome, at) = text.split_once(' ').unwrap();
            (outcome.to_owned(), at.parse().unwrap())
        });
        if due.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    let (outcome, due_at) = due.expect("rate-limited retry was not scheduled");
    // Retry-After (3 s) counts from the 800 ms response, not from the request;
    // 100 ms covers the observation delay of `requested_at`.
    assert!(
        outcome == "rate_limited" && due_at >= requested_at + 3_700,
        "outcome={outcome} due_at={due_at} requested_at={requested_at} delta={}",
        due_at - requested_at
    );
}

#[tokio::test]
async fn excluded_alertname_never_reaches_the_provider() {
    let clef = MockProvider::start(Vec::new(), answer(0.4)).await;
    let harness = harness(&clef, "labels = [\"service\"]\n").await;
    harness.ingest(warning()).await;
    let cancel = spawn(&harness);
    wait_for_outcome(&harness).await;
    cancel.cancel();
    let body = clef.requests()[0].body.to_string();
    let fields: String = harness
        .query("SELECT context_fields FROM assessments")
        .await;
    assert!(
        !body.contains("ExampleStorageFailure")
            && fields == r#"["labels.service","annotations.summary"]"#,
        "fields={fields} body={body}"
    );
}

#[tokio::test]
async fn included_alertname_is_truncated_like_every_other_field() {
    let clef = MockProvider::start(Vec::new(), answer(0.4)).await;
    let harness = harness(&clef, "max_value_chars = 16\n").await;
    let long = "VeryLongAlertName".repeat(10);
    harness
        .ingest(with(warning(), "/alerts/0/labels/alertname", json!(long)))
        .await;
    let cancel = spawn(&harness);
    wait_for_outcome(&harness).await;
    cancel.cancel();
    let request = &clef.requests()[0];
    let body = request.body.to_string();
    let fields: String = harness
        .query("SELECT context_fields FROM assessments")
        .await;
    assert!(
        request.body["state"]["labels"]["alertname"] == json!(&long[..16])
            && !body.contains(&long[..17])
            && fields.contains("labels.alertname"),
        "fields={fields} body={body}"
    );
}

fn metric(harness: &Harness, series: &str) -> Option<String> {
    let text = harness.state.metrics.render().unwrap();
    text.lines()
        .find(|line| line.starts_with(series))
        .map(str::to_owned)
}

#[tokio::test]
async fn queue_full_skips_are_counted_in_the_assessment_metric() {
    let clef = MockProvider::start(Vec::new(), answer(0.5)).await;
    let harness = harness(&clef, "max_pending = 1\n").await;
    let mut batch = warning();
    let template = batch["alerts"][0].clone();
    batch["alerts"] = Value::Array(
        (0..3)
            .map(|index| {
                with(
                    template.clone(),
                    "/fingerprint",
                    json!(format!("{index:016x}")),
                )
            })
            .collect(),
    );
    let (status, _) = super::post_to(harness.app(), "example", &batch, super::TOKEN).await;
    assert_eq!(
        (
            status,
            metric(
                &harness,
                r#"criers_assessments_total{outcome="skipped_queue_full"}"#
            )
        ),
        (
            axum::http::StatusCode::ACCEPTED,
            Some(r#"criers_assessments_total{outcome="skipped_queue_full"} 2"#.to_owned())
        )
    );
}

#[tokio::test]
async fn assessments_skipped_for_closed_occurrences_are_counted() {
    let clef = MockProvider::start(Vec::new(), answer(0.5)).await;
    let harness = harness(&clef, "").await;
    harness.ingest(warning()).await;
    harness
        .ingest(with(warning(), "/alerts/0/status", json!("resolved")))
        .await;
    let cancel = spawn(&harness);
    let outcome = wait_for_outcome(&harness).await;
    cancel.cancel();
    assert_eq!(
        (
            outcome.as_deref(),
            clef.requests().len(),
            metric(
                &harness,
                r#"criers_assessments_total{outcome="skipped_closed"}"#
            )
        ),
        (
            Some("skipped_closed"),
            0,
            Some(r#"criers_assessments_total{outcome="skipped_closed"} 1"#.to_owned())
        )
    );
}

#[tokio::test]
async fn queued_assessment_is_not_sent_to_a_different_model_after_a_config_change() {
    let clef = MockProvider::start(Vec::new(), answer(0.5)).await;
    let harness = harness(&clef, "model = \"clef\"\n").await;
    harness.ingest(warning()).await;
    // Restart with the other model configured; the queued job was asked for `clef`.
    let config_path = harness.directory.path().join("criers.toml");
    let text = std::fs::read_to_string(&config_path)
        .unwrap()
        .replace("model = \"clef\"\n", "model = \"clef-flash\"\n");
    std::fs::write(&config_path, text).unwrap();
    let config = Arc::new(crate::config::Config::load(&config_path).unwrap());
    let Harness {
        directory: _directory,
        path,
        store,
        state,
        ..
    } = harness;
    drop(state);
    drop(store);
    let store = crate::store::Store::open(path, config.storage.max_bytes)
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    tokio::spawn(assessment::run(
        store.clone(),
        Clef::new(Arc::clone(config.assessment.as_ref().unwrap())).unwrap(),
        Arc::new(crate::metrics::Metrics::new()),
        Arc::new(tokio::sync::Notify::new()),
        cancel.clone(),
    ));
    let mut row = String::new();
    for _ in 0..200 {
        row = store
            .with_connection(|connection| {
                Ok(connection.query_row(
                    "SELECT state || ':' || coalesce(outcome, '') || ':' || requested_model
                     || ':' || coalesce(model, '') FROM assessments",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        if row.starts_with("done") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    assert_eq!(
        (row.as_str(), clef.requests().len()),
        ("done:skipped_model_changed:clef:", 0),
        "requests={:?}",
        clef.requests()
            .iter()
            .map(|request| &request.path)
            .collect::<Vec<_>>()
    );
}

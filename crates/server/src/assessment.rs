//! Shadow-mode Cloudflare Clef assessment: redacted context, bounded worker and HTTP adapter.
//!
//! Assessment never changes notifications. It runs after the ingestion commit,
//! so provider latency or failure cannot delay deterministic routing.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use criers_core::Alert;
use reqwest::{Client, StatusCode, header};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use tokio::{sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{Assessment, hex_sha256},
    http_body::read_bounded,
    metrics::{Metrics, OutcomeLabels},
    store::{AssessmentFailure, AssessmentJob, Store},
};

/// Version of the fixed question set; recorded with every result.
pub(crate) const QUESTIONS_VERSION: &str = "clef-impact-urgency-v1";

const URGENCY_LEVELS: [&str; 4] = [
    "Informational: no action needed",
    "Minor: can wait for working hours",
    "Major: needs attention soon",
    "Critical: needs immediate response",
];

/// The allowlisted, size-bounded state sent to the provider.
pub(crate) struct Context {
    pub json: String,
    pub sha256: String,
    pub fields: String,
}

#[derive(Serialize)]
struct State<'a> {
    status: &'static str,
    labels: BTreeMap<&'a str, String>,
    annotations: BTreeMap<&'a str, String>,
}

/// Builds the redacted provider context for a firing alert.
///
/// Only configured label and annotation names are included, each truncated to
/// `max_value_chars`. Credentials and other labels never leave the process.
pub(crate) fn context(alert: &Alert, config: &Assessment) -> Result<Context, serde_json::Error> {
    let truncate = |value: &str| {
        value
            .chars()
            .take(config.max_value_chars)
            .collect::<String>()
    };
    let labels: BTreeMap<&str, String> = config
        .labels
        .iter()
        .filter_map(|name| Some((name.as_str(), truncate(alert.labels.get(name)?))))
        .collect();
    let annotations: BTreeMap<&str, String> = config
        .annotations
        .iter()
        .filter_map(|name| Some((name.as_str(), truncate(alert.annotations.get(name)?))))
        .collect();
    let fields: Vec<String> = labels
        .keys()
        .map(|name| format!("labels.{name}"))
        .chain(annotations.keys().map(|name| format!("annotations.{name}")))
        .collect();
    // The alert name is a label like any other: it is sent only if allowlisted.
    let state = State {
        status: "firing",
        labels,
        annotations,
    };
    let json = serde_json::to_string(&state)?;
    Ok(Context {
        sha256: hex_sha256(json.as_bytes()),
        fields: serde_json::to_string(&fields)?,
        json,
    })
}

/// Validated provider answers with provenance.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Answers {
    pub model: String,
    pub impact_probability: f64,
    pub urgency_score: f64,
    pub urgency_probabilities: String,
    pub confidence: f64,
    pub input_tokens: Option<i64>,
    pub latency_ms: i64,
}

/// Why an assessment attempt failed. Outcome names are stable metric labels.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssessError {
    #[error("provider deadline exceeded")]
    Timeout,
    #[error("provider rate limited the request")]
    RateLimited { retry_after: Option<Duration> },
    #[error("provider returned HTTP {0}")]
    Http(u16),
    #[error("malformed provider response: {0}")]
    Malformed(String),
    #[error("transport failure: {0}")]
    Transport(String),
}

impl AssessError {
    pub(crate) fn outcome(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::RateLimited { .. } => "rate_limited",
            Self::Http(_) => "http_error",
            Self::Malformed(_) => "malformed",
            Self::Transport(_) => "transport_error",
        }
    }

    fn retryable(&self) -> bool {
        match self {
            Self::Timeout | Self::RateLimited { .. } | Self::Transport(_) => true,
            Self::Http(status) => *status >= 500,
            Self::Malformed(_) => false,
        }
    }
}

/// HTTP adapter for the Workers AI REST endpoint of Clef or Clef-flash.
pub(crate) struct Clef {
    client: Client,
    config: Arc<Assessment>,
}

#[derive(Deserialize)]
struct Envelope {
    result: Option<Output>,
    #[serde(default)]
    success: bool,
}

#[derive(Deserialize)]
struct Output {
    model: String,
    answers: BTreeMap<String, Value>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct Usage {
    input_tokens: i64,
}

impl Clef {
    pub(crate) fn new(config: Arc<Assessment>) -> reqwest::Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.timeout.min(Duration::from_secs(5)))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("criers/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self { client, config })
    }

    /// Requests impact and urgency probabilities for one redacted context.
    pub(crate) async fn assess(&self, context: &str) -> Result<Answers, AssessError> {
        // The stored context is already JSON; embed it without rebuilding a Value.
        let state: &RawValue = serde_json::from_str(context)
            .map_err(|error| AssessError::Malformed(error.to_string()))?;
        let body = json!({
            "model": self.config.model,
            "state": state,
            "questions": {
                "impact": {
                    "type": "noul",
                    "instructions": "Does this alert indicate current user-facing impact?",
                    "criteria": {
                        "true": "Users or customers are affected now",
                        "false": "No current user-facing impact"
                    }
                },
                "urgency": {
                    "type": "score",
                    "instructions": "How urgently does a human need to respond to this alert?",
                    "criteria": URGENCY_LEVELS
                }
            }
        });
        let started = std::time::Instant::now();
        let response = self
            .client
            .post(self.config.url.clone())
            .bearer_auth(self.config.api_token.expose_secret())
            .json(&body)
            .send()
            .await
            .map_err(classify_transport)?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
            .map(|seconds| Duration::from_secs_f64(seconds.min(3600.0)));
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(AssessError::RateLimited { retry_after });
        }
        if !status.is_success() {
            return Err(AssessError::Http(status.as_u16()));
        }
        let bytes = read_bounded(response, self.config.max_response_bytes)
            .await
            .map_err(|error| match error {
                crate::http_body::BodyError::TooLarge => {
                    AssessError::Malformed("response exceeds max_response_bytes".to_owned())
                }
                crate::http_body::BodyError::Transport(error) => classify_transport(error),
            })?;
        let latency_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        parse(&bytes, latency_ms)
    }
}

fn classify_transport(error: reqwest::Error) -> AssessError {
    if error.is_timeout() {
        AssessError::Timeout
    } else {
        AssessError::Transport(error.without_url().to_string())
    }
}

fn probability(value: Option<&Value>, name: &str) -> Result<f64, AssessError> {
    value
        .and_then(Value::as_f64)
        .filter(|p| (0.0..=1.0).contains(p))
        .ok_or_else(|| AssessError::Malformed(format!("{name} must be a probability")))
}

/// Allowed rounding error when checking that a distribution sums to 1.
const DISTRIBUTION_TOLERANCE: f64 = 0.01;

/// The documented score answer has one probability per requested level, keyed by
/// the zero-based level index as a string, and the values sum to 1.
fn complete_distribution(levels: &serde_json::Map<String, Value>) -> bool {
    if levels.len() != URGENCY_LEVELS.len() {
        return false;
    }
    let mut total = 0.0;
    for index in 0..URGENCY_LEVELS.len() {
        match levels.get(&index.to_string()).and_then(Value::as_f64) {
            Some(probability) if (0.0..=1.0).contains(&probability) => total += probability,
            _ => return false,
        }
    }
    (total - 1.0).abs() <= DISTRIBUTION_TOLERANCE
}

fn parse(bytes: &[u8], latency_ms: i64) -> Result<Answers, AssessError> {
    let malformed = |reason: &str| AssessError::Malformed(reason.to_owned());
    let envelope: Envelope =
        serde_json::from_slice(bytes).map_err(|error| AssessError::Malformed(error.to_string()))?;
    let output = match envelope.result {
        Some(output) if envelope.success => output,
        _ => return Err(malformed("envelope reports failure or has no result")),
    };
    let impact = output
        .answers
        .get("impact")
        .ok_or_else(|| malformed("missing impact"))?;
    if impact.get("type").and_then(Value::as_str) != Some("noul") {
        return Err(malformed("impact answer is not noul"));
    }
    let impact_probability = probability(impact.get("noul"), "impact.noul")?;
    let urgency = output
        .answers
        .get("urgency")
        .ok_or_else(|| malformed("missing urgency"))?;
    if urgency.get("type").and_then(Value::as_str) != Some("score") {
        return Err(malformed("urgency answer is not score"));
    }
    let max_level = (URGENCY_LEVELS.len() - 1) as f64;
    let urgency_score = urgency
        .get("score")
        .and_then(Value::as_f64)
        .filter(|score| (0.0..=max_level).contains(score))
        .ok_or_else(|| malformed("urgency.score is outside the rubric"))?;
    let confidence = probability(urgency.get("confidence"), "urgency.confidence")?;
    let probabilities = urgency
        .get("probabilities")
        .and_then(Value::as_object)
        .filter(|levels| complete_distribution(levels))
        .ok_or_else(|| {
            malformed(
                "urgency.probabilities must give every requested level a probability, summing to 1",
            )
        })?;
    if output.model.is_empty() || output.model.len() > 128 {
        return Err(malformed("model must be 1 to 128 bytes"));
    }
    Ok(Answers {
        model: output.model,
        impact_probability,
        urgency_score,
        urgency_probabilities: serde_json::to_string(probabilities)
            .map_err(|error| AssessError::Malformed(error.to_string()))?,
        confidence,
        input_tokens: output.usage.map(|usage| usage.input_tokens),
        latency_ms,
    })
}

/// Runs bounded concurrent assessments until cancelled.
pub(crate) async fn run(
    store: Store,
    clef: Clef,
    metrics: Arc<Metrics>,
    wake: Arc<Notify>,
    cancel: CancellationToken,
) {
    let clef = Arc::new(clef);
    let concurrency = clef.config.concurrency;
    let max_attempts = clef.config.max_attempts;
    let mut running: JoinSet<()> = JoinSet::new();
    loop {
        let woken = wake.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        while running.try_join_next().is_some() {}
        let capacity = concurrency.saturating_sub(running.len());
        let mut claimed = 0;
        if capacity > 0 {
            match store.claim_assessments(crate::now_ms(), capacity).await {
                Ok(jobs) => {
                    claimed = jobs.len();
                    for job in jobs {
                        running.spawn(assess_one(
                            store.clone(),
                            Arc::clone(&clef),
                            Arc::clone(&metrics),
                            job,
                            max_attempts,
                            cancel.clone(),
                        ));
                    }
                }
                Err(error) => tracing::warn!(%error, "cannot claim assessments"),
            }
        }
        if claimed > 0 && running.len() < concurrency {
            continue;
        }
        tokio::select! {
            () = cancel.cancelled() => break,
            () = woken => {}
            Some(_) = running.join_next(), if !running.is_empty() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
    // In-flight assessments are abandoned; they return to pending on restart.
    running.abort_all();
}

async fn assess_one(
    store: Store,
    clef: Arc<Clef>,
    metrics: Arc<Metrics>,
    job: AssessmentJob,
    max_attempts: u32,
    cancel: CancellationToken,
) {
    let response = clef.assess(&job.context).await;
    // Retry deadlines count from when the provider answered, not when it was asked.
    let responded_at = crate::now_ms();
    let result = match response {
        Ok(answers) => Ok(answers),
        Err(error) => {
            tracing::info!(assessment = job.id, outcome = error.outcome(), %error, "assessment attempt failed");
            let retry_at = error.retryable().then(|| {
                let delay = match &error {
                    AssessError::RateLimited {
                        retry_after: Some(delay),
                    } => *delay,
                    _ => Duration::from_secs(2u64.pow(job.attempts.min(6))),
                };
                responded_at + i64::try_from(delay.as_millis()).unwrap_or(i64::MAX / 4)
            });
            Err(AssessmentFailure {
                outcome: error.outcome(),
                error: error.to_string(),
                retry_at,
            })
        }
    };
    // Keep the obtained result and retry only the write: a row left in flight
    // would hold queue capacity until restart, and asking the provider again
    // would repeat a paid call. Shutdown abandons the write; restart then
    // returns the row to pending.
    let mut attempt = 0u32;
    let outcome = loop {
        // Each write attempt stamps its own completion time, so a result stored
        // after an outage starts its retention window when it became durable.
        match store
            .record_assessment(job.id, result.clone(), crate::now_ms(), max_attempts)
            .await
        {
            Ok(outcome) => break outcome,
            Err(error) => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(assessment = job.id, %error, attempt, "cannot record assessment; retrying");
                let delay = crate::delivery::backoff(
                    attempt,
                    Duration::from_millis(200),
                    Duration::from_secs(10),
                );
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    };
    if let Some(outcome) = outcome {
        metrics
            .assessments
            .get_or_create(&OutcomeLabels { outcome })
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(answers: Value) -> Vec<u8> {
        json!({
            "result": {"model": "clef-flash", "answers": answers, "usage": {"input_tokens": 90, "output_tokens": 0}},
            "success": true, "errors": [], "messages": []
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn documented_answer_shapes_parse_into_separate_probabilities() {
        let bytes = response(json!({
            "impact": {"type": "noul", "noul": 0.82},
            "urgency": {"type": "score", "score": 2.4,
                "legend": {"0": "a", "1": "b", "2": "c", "3": "d"},
                "probabilities": {"0": 0.05, "1": 0.1, "2": 0.25, "3": 0.6}, "confidence": 0.7}
        }));
        let answers = parse(&bytes, 12).unwrap();
        assert_eq!(
            (
                answers.impact_probability,
                answers.urgency_score,
                answers.input_tokens
            ),
            (0.82, 2.4, Some(90))
        );
    }

    fn urgency(probabilities: Value) -> Vec<u8> {
        response(json!({
            "impact": {"type": "noul", "noul": 0.5},
            "urgency": {"type": "score", "score": 1.0,
                "legend": {"0": "a", "1": "b", "2": "c", "3": "d"},
                "probabilities": probabilities, "confidence": 0.5}
        }))
    }

    #[test]
    fn empty_urgency_distribution_is_malformed() {
        assert!(matches!(
            parse(&urgency(json!({})), 1),
            Err(AssessError::Malformed(_))
        ));
    }

    #[test]
    fn partial_urgency_distribution_is_malformed() {
        let bytes = urgency(json!({"0": 0.5, "3": 0.5}));
        assert!(matches!(parse(&bytes, 1), Err(AssessError::Malformed(_))));
    }

    #[test]
    fn urgency_distribution_with_unknown_levels_is_malformed() {
        let bytes = urgency(json!({"a": 0.25, "b": 0.25, "c": 0.25, "d": 0.25}));
        assert!(matches!(parse(&bytes, 1), Err(AssessError::Malformed(_))));
    }

    #[test]
    fn urgency_distribution_that_does_not_sum_to_one_is_malformed() {
        let bytes = urgency(json!({"0": 0.1, "1": 0.1, "2": 0.1, "3": 0.1}));
        assert!(matches!(parse(&bytes, 1), Err(AssessError::Malformed(_))));
    }

    #[test]
    fn probability_outside_unit_interval_is_malformed() {
        let bytes = response(json!({
            "impact": {"type": "noul", "noul": 1.5},
            "urgency": {"type": "score", "score": 1.0, "legend": {}, "probabilities": {}, "confidence": 0.5}
        }));
        assert!(matches!(parse(&bytes, 1), Err(AssessError::Malformed(_))));
    }

    #[test]
    fn failed_envelope_is_malformed() {
        let bytes = br#"{"result":null,"success":false,"errors":[{"code":5007,"message":"x"}]}"#;
        assert!(matches!(parse(bytes, 1), Err(AssessError::Malformed(_))));
    }
}

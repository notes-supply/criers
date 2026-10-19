//! Durable outbox delivery to Discord webhooks.
//!
//! One worker per destination is the single owner of that destination's rows.
//! Rows are claimed and marked in flight before each request, so a crash leaves
//! a recoverable record instead of a silent gap.

use std::{sync::Arc, time::Duration};

use reqwest::{Client, StatusCode, Url};
use secrecy::ExposeSecret;
use serde::Serialize;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{Delivery, Destination},
    http_body::{BodyError, read_bounded},
    metrics::{DestinationOutcome, Metrics},
    render::{CONTENT_LIMIT, Embed, HEADLINE_LIMIT, Part},
    store::{Batch, Claim, ClaimLimits, Completion, Store},
};

const MAX_RESPONSE_BYTES: usize = 65_536;
const IDLE_POLL: Duration = Duration::from_secs(5);
const MIN_WAIT: Duration = Duration::from_millis(50);
const MENTION_COALESCED: &str = "(mention coalesced; recently pinged)";

/// What a Discord request achieved.
#[derive(Debug, PartialEq)]
pub(crate) enum Outcome {
    Sent {
        remote_id: Option<String>,
        status: u16,
    },
    RateLimited {
        retry_after: Duration,
        status: u16,
        /// Discord's global limit applies to every webhook this process sends to.
        global: bool,
    },
    Retry {
        status: Option<u16>,
        error: String,
        ambiguous: bool,
    },
    Permanent {
        status: Option<u16>,
        error: String,
    },
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Sent { .. } => "sent",
            Self::RateLimited { .. } => "rate_limited",
            Self::Retry {
                ambiguous: true, ..
            } => "ambiguous",
            Self::Retry {
                ambiguous: false, ..
            } => "retryable",
            Self::Permanent { .. } => "permanent",
        }
    }
}

#[derive(Serialize)]
struct AllowedMentions<'a> {
    parse: [&'a str; 0],
    users: &'a [String],
    roles: &'a [String],
}

#[derive(Serialize)]
struct Message<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    content: &'a str,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    embeds: &'a [&'a Embed],
    allowed_mentions: AllowedMentions<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
}

/// One rendered Discord message for a batch.
#[derive(Debug)]
pub(crate) struct Rendered<'a> {
    /// Mentions, the pinging card's headline and any plain lines from earlier releases.
    pub content: String,
    pub embeds: Vec<&'a Embed>,
    /// Whether this message may mention the destination's recipients.
    pub mention: bool,
}

/// One Discord webhook with its explicit mention recipients.
pub(crate) struct Discord {
    client: Client,
    url: Url,
    mention_users: Vec<String>,
    mention_roles: Vec<String>,
    username: Option<String>,
    mention_prefix: String,
}

impl Discord {
    pub(crate) fn new(destination: &Destination, timeout: Duration) -> anyhow::Result<Self> {
        let secret = destination
            .webhook_url
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("live destinations need a webhook URL"))?;
        let mut url = Url::parse(secret.expose_secret())?;
        url.query_pairs_mut().append_pair("wait", "true");
        let client = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("criers/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let mention_prefix = destination
            .mention_users
            .iter()
            .map(|id| format!("<@{id}>"))
            .chain(
                destination
                    .mention_roles
                    .iter()
                    .map(|id| format!("<@&{id}>")),
            )
            .collect::<Vec<_>>()
            .join(" ");
        Ok(Self {
            client,
            url,
            mention_users: destination.mention_users.clone(),
            mention_roles: destination.mention_roles.clone(),
            username: destination.username.clone(),
            mention_prefix,
        })
    }

    /// Content left for plain lines of earlier releases after the largest
    /// possible mention and headline, in UTF-16 units.
    pub(crate) fn line_budget(&self) -> usize {
        CONTENT_LIMIT
            - crate::render::units(&self.mention_prefix).max(MENTION_COALESCED.len())
            - HEADLINE_LIMIT
            - 3
    }

    /// Renders a batch: one embed per card, in order. Mentions are explicit IDs
    /// only; `parse` is always empty.
    pub(crate) fn message<'a>(&self, batch: &'a Batch) -> Rendered<'a> {
        let mention = batch.ping && batch.mention_allowed() && !self.mention_prefix.is_empty();
        let mut lines: Vec<&str> = Vec::with_capacity(batch.parts.len() + 1);
        let lead;
        if mention {
            lead = match &batch.headline {
                Some(headline) => format!("{} {headline}", self.mention_prefix),
                None => self.mention_prefix.clone(),
            };
            lines.push(&lead);
        } else if batch.ping && !self.mention_prefix.is_empty() {
            lines.push(MENTION_COALESCED);
        }
        let mut embeds = Vec::with_capacity(batch.parts.len());
        for part in &batch.parts {
            match part {
                Part::Card(card) => embeds.push(&card.embed),
                Part::Line(line) => lines.push(line),
            }
        }
        Rendered {
            content: lines.join("\n"),
            embeds,
            mention,
        }
    }

    /// Posts one message with `wait=true` and classifies the response.
    pub(crate) async fn send(&self, rendered: &Rendered<'_>) -> Outcome {
        let empty: &[String] = &[];
        let message = Message {
            content: &rendered.content,
            embeds: &rendered.embeds,
            allowed_mentions: AllowedMentions {
                parse: [],
                users: if rendered.mention {
                    &self.mention_users
                } else {
                    empty
                },
                roles: if rendered.mention {
                    &self.mention_roles
                } else {
                    empty
                },
            },
            username: self.username.as_deref(),
        };
        let response = match self
            .client
            .post(self.url.clone())
            .json(&message)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return transport_outcome(error),
        };
        let status = response.status();
        let headers = response.headers();
        let retry_after = headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .and_then(seconds);
        // Discord marks global limits with X-RateLimit-Scope: global or X-RateLimit-Global.
        let global = headers.contains_key("x-ratelimit-global")
            || headers
                .get("x-ratelimit-scope")
                .is_some_and(|scope| scope.as_bytes().eq_ignore_ascii_case(b"global"));
        let body = read_bounded(response, MAX_RESPONSE_BYTES).await;
        classify(
            status,
            Limit {
                retry_after,
                global,
            },
            body,
        )
    }
}

fn transport_outcome(error: reqwest::Error) -> Outcome {
    let builder = error.is_builder();
    // Without a connection nothing reached Discord; afterwards the message may exist.
    let ambiguous = !error.is_connect() && !builder;
    // The request URL contains the webhook token and must never be stored or logged.
    let error = error.without_url().to_string();
    if builder {
        Outcome::Permanent {
            status: None,
            error,
        }
    } else {
        Outcome::Retry {
            status: None,
            error,
            ambiguous,
        }
    }
}

#[derive(serde::Deserialize)]
struct Created {
    id: String,
}

#[derive(serde::Deserialize)]
struct RateLimit {
    retry_after: f64,
    #[serde(default)]
    global: bool,
}

/// Rate-limit hints from response headers.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Limit {
    /// The `Retry-After` header.
    pub retry_after: Option<Duration>,
    /// A header marked the limit as global.
    pub global: bool,
}

fn seconds(value: f64) -> Option<Duration> {
    (value.is_finite() && value >= 0.0).then(|| Duration::from_secs_f64(value.min(86_400.0)))
}

/// Classifies a response; `retry_after` is the parsed `Retry-After` header.
pub(crate) fn classify(
    status: StatusCode,
    headers: Limit,
    body: Result<Vec<u8>, BodyError>,
) -> Outcome {
    let code = status.as_u16();
    if status.is_success() {
        // Discord accepted the message; a missing or unreadable body loses only the receipt.
        let remote_id = body
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Created>(&bytes).ok())
            .map(|created| created.id)
            .filter(|id| {
                !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_digit())
            });
        return Outcome::Sent {
            remote_id,
            status: code,
        };
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        let limit = body
            .ok()
            .and_then(|bytes| serde_json::from_slice::<RateLimit>(&bytes).ok());
        let retry_after = limit
            .as_ref()
            .and_then(|limit| seconds(limit.retry_after))
            .into_iter()
            .chain(headers.retry_after)
            .max()
            .unwrap_or(Duration::from_secs(1));
        return Outcome::RateLimited {
            retry_after,
            status: code,
            global: headers.global || limit.is_some_and(|limit| limit.global),
        };
    }
    match code {
        408 | 503 => Outcome::Retry {
            status: Some(code),
            error: format!("HTTP {code}"),
            ambiguous: false,
        },
        // A gateway or server error may arrive after the message was created.
        500..=599 => Outcome::Retry {
            status: Some(code),
            error: format!("HTTP {code}"),
            ambiguous: true,
        },
        _ => Outcome::Permanent {
            status: Some(code),
            error: format!("HTTP {code}"),
        },
    }
}

/// Exponential backoff with equal jitter, capped.
pub(crate) fn backoff(failures: u32, base: Duration, max: Duration) -> Duration {
    let ceiling = base
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(20)))
        .min(max);
    let half = ceiling / 2;
    half + half.mul_f64(fastrand::f64())
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX / 4)
}

/// Delivers one destination's live outbox until cancelled.
pub(crate) struct Worker {
    pub destination: String,
    pub discord: Discord,
    pub mention_cooldown: Duration,
    pub store: Store,
    pub metrics: Arc<Metrics>,
    pub wake: Arc<Notify>,
    pub settings: Delivery,
}

impl Worker {
    pub(crate) async fn run(self, cancel: CancellationToken) {
        let limits = ClaimLimits {
            max_rows: self.settings.batch_max,
            budget_chars: self.discord.line_budget(),
            mention_cooldown_ms: millis(self.mention_cooldown),
        };
        let mut failures = 0u32;
        while !cancel.is_cancelled() {
            // Registering before the claim means a commit during the claim still wakes us.
            let woken = self.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let now = crate::now_ms();
            let wait = match self
                .store
                .claim_deliveries(self.destination.clone(), now, limits)
                .await
            {
                Ok(Claim::Batch(batch)) => {
                    failures = 0;
                    self.deliver(batch, &cancel).await;
                    continue;
                }
                Ok(Claim::Wait(until)) => {
                    failures = 0;
                    until.map_or(IDLE_POLL, |until| {
                        Duration::from_millis(u64::try_from(until - now).unwrap_or(0))
                            .clamp(MIN_WAIT, IDLE_POLL)
                    })
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    tracing::error!(destination = %self.destination, %error, "cannot claim deliveries");
                    backoff(
                        failures,
                        Duration::from_millis(500),
                        Duration::from_secs(30),
                    )
                }
            };
            tokio::select! {
                () = cancel.cancelled() => break,
                () = woken => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn deliver(&self, batch: Batch, cancel: &CancellationToken) {
        let outcome = self.discord.send(&self.discord.message(&batch)).await;
        self.metrics
            .delivery_attempts
            .get_or_create(&DestinationOutcome {
                destination: self.destination.clone(),
                outcome: outcome.label(),
            })
            .inc();
        let now = crate::now_ms();
        let settings = &self.settings;
        let completion = match outcome {
            Outcome::Sent { remote_id, status } => Completion::Sent { remote_id, status },
            Outcome::RateLimited {
                retry_after,
                status,
                global,
            } => {
                let jitter = Duration::from_millis(fastrand::u64(0..250));
                let delay = retry_after.min(settings.max_rate_limit_wait) + jitter;
                tracing::warn!(destination = %self.destination, delay_ms = millis(delay), global, "rate limited");
                Completion::RateLimited {
                    retry_at: now + millis(delay),
                    status,
                    global,
                }
            }
            Outcome::Retry {
                status,
                error,
                ambiguous,
            } => {
                let delay = backoff(
                    batch.max_failures,
                    settings.backoff_base,
                    settings.backoff_max,
                );
                tracing::warn!(destination = %self.destination, ?status, %error, ambiguous, rows = batch.ids.len(), "delivery attempt failed; retrying");
                Completion::Retry {
                    retry_at: now + millis(delay),
                    status,
                    error,
                    ambiguous,
                }
            }
            Outcome::Permanent { status, error } => {
                tracing::error!(destination = %self.destination, ?status, %error, rows = batch.ids.len(), "delivery rejected permanently");
                Completion::Permanent { status, error }
            }
        };
        if batch.possibly_duplicate {
            tracing::info!(destination = %self.destination, rows = batch.ids.len(), "retried rows may duplicate an earlier ambiguous send");
        }
        // Keep trying to record the outcome: rows left in flight would block later
        // intents of their occurrences. Only shutdown abandons the attempt, and
        // restart then retries those rows as possible duplicates.
        let mut attempt = 0u32;
        loop {
            let result = self
                .store
                .complete_deliveries(
                    self.destination.clone(),
                    batch.ids.clone(),
                    batch.mention,
                    completion.clone(),
                    settings.max_attempts,
                )
                .await;
            let finished = match result {
                Ok(finished) => finished,
                Err(error) => {
                    attempt = attempt.saturating_add(1);
                    tracing::error!(destination = %self.destination, %error, attempt, "cannot record delivery outcome");
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        () = tokio::time::sleep(backoff(attempt, Duration::from_millis(200), Duration::from_secs(10))) => {}
                    }
                    continue;
                }
            };
            for (state, count) in [
                ("sent", finished.sent),
                ("failed", finished.failed),
                ("ambiguous", finished.ambiguous),
            ] {
                self.metrics.delivery_ended(&self.destination, state, count);
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_uses_the_longer_of_body_and_header() {
        let outcome = classify(
            StatusCode::TOO_MANY_REQUESTS,
            Limit {
                retry_after: Some(Duration::from_secs(3)),
                global: false,
            },
            Ok(br#"{"message":"limited","retry_after":1.5,"global":false}"#.to_vec()),
        );
        assert_eq!(
            outcome,
            Outcome::RateLimited {
                retry_after: Duration::from_secs(3),
                status: 429,
                global: false
            }
        );
    }

    #[test]
    fn global_flag_in_the_body_marks_a_global_limit() {
        let outcome = classify(
            StatusCode::TOO_MANY_REQUESTS,
            Limit::default(),
            Ok(br#"{"message":"limited","retry_after":2,"global":true}"#.to_vec()),
        );
        assert!(
            matches!(outcome, Outcome::RateLimited { global: true, .. }),
            "{outcome:?}"
        );
    }

    #[test]
    fn global_scope_header_marks_a_global_limit_without_a_body() {
        let outcome = classify(
            StatusCode::TOO_MANY_REQUESTS,
            Limit {
                retry_after: Some(Duration::from_secs(1)),
                global: true,
            },
            Err(BodyError::TooLarge),
        );
        assert!(
            matches!(outcome, Outcome::RateLimited { global: true, .. }),
            "{outcome:?}"
        );
    }

    #[test]
    fn missing_webhook_is_permanent() {
        let outcome = classify(StatusCode::NOT_FOUND, Limit::default(), Ok(Vec::new()));
        assert!(matches!(
            outcome,
            Outcome::Permanent {
                status: Some(404),
                ..
            }
        ));
    }

    #[test]
    fn gateway_timeout_is_an_ambiguous_retry() {
        let outcome = classify(
            StatusCode::GATEWAY_TIMEOUT,
            Limit::default(),
            Ok(Vec::new()),
        );
        assert!(matches!(
            outcome,
            Outcome::Retry {
                ambiguous: true,
                ..
            }
        ));
    }

    #[test]
    fn success_without_a_readable_body_is_still_sent() {
        let outcome = classify(StatusCode::OK, Limit::default(), Err(BodyError::TooLarge));
        assert_eq!(
            outcome,
            Outcome::Sent {
                remote_id: None,
                status: 200
            }
        );
    }

    #[test]
    fn backoff_is_capped_and_jittered_within_half_to_full_ceiling() {
        let delay = backoff(30, Duration::from_secs(1), Duration::from_secs(300));
        assert!(
            (Duration::from_secs(150)..=Duration::from_secs(300)).contains(&delay),
            "{delay:?}"
        );
    }
}

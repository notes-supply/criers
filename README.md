# Alert router

Working name. An event-driven Rust service that receives Alertmanager webhooks,
persists occurrence history, and proposes notifications using explicit policy.

**Current state: runnable shadow-only foundation, not production notification routing.**
It cannot send a Discord message or contact a model. No cloud credentials are
required. The product name and public repository are not finalized yet.

## Implemented

- Axum receiver with a required bearer credential, loopback default, body and
  field bounds, and bounded request/database admission.
- SQLite WAL with `synchronous=FULL`. Events, lifecycle changes, policy decisions,
  and shadow outbox intents commit together before HTTP `202`.
- Occurrence identity: configured source, normalized fingerprint, canonical start
  timestamp. Repeats do not create another notification intent. A new start is a
  new occurrence. Late firing does not reopen a resolved occurrence.
- Individual firing/resolved status governs mixed batches. Absence does not mean
  resolution. `truncatedAlerts` is retained as a coverage gap.
- Versioned deterministic defaults: critical proposes a ping, warning a quiet
  post, info a record. Missing or unknown severity conservatively proposes a
  ping. Only an explicitly trusted Alertmanager should hold the source credential.
- Quiet recovery for a previously notifiable occurrence; priority escalation is
  durable and deduplicated.
- Authenticated status endpoint and separate process/storage health endpoints.

An intent is not a delivery receipt. Shadow intents remain shadow records and
must never be bulk-promoted into live sends later.

## Run locally

Install [Mise](https://mise.jdx.dev/), trust the repository, then install its pinned
tools with `mise install --locked rust aqua:nextest-rs/nextest/cargo-nextest`.

Create `data/` and a credential file outside the repository. Use at least 32
random printable ASCII characters and restrict the file to its owner. Set:

- `ALERT_ROUTER_SOURCE=example`: trusted producer identity, not taken from labels.
- `ALERT_ROUTER_TOKEN_FILE=/path/to/private-token`: required credential file.
- `ALERT_ROUTER_DATABASE=data/alerts.sqlite`: optional database path. Its parent
  directory must exist; use a private directory for real alert data.
- `ALERT_ROUTER_BIND=127.0.0.1:8080`: optional listen address.
- `RUST_LOG=alert_router=info,tower_http=info`: optional tracing filter.

Start with `mise run dev`. Send the synthetic [critical webhook](examples/critical.json)
to `POST /v1/alertmanager` with `Content-Type: application/json` and a matching
`Authorization: Bearer ...` header. The first accepted request reports `ping` and
`intent_created: true`, always in `shadow` mode. Sending the same occurrence
again reports no new intent, including after restarting the process.

`GET /v1/status` uses the same credential. `GET /health/live` checks process
responsiveness; `GET /health/ready` performs a rollback-only database write.
Neither proves monitoring coverage, Discord delivery, or model availability.
Quiet incoming traffic always leaves source coverage unknown until an independent
heartbeat contract is implemented.

`mise run ci` checks formatting, strict Clippy, nextest, separate doctests, and a
development build using the dated compiler pin in `mise.toml`.

## Admission and failure behavior

Each webhook is limited to 1 MiB and 512 alerts, with at most 64 labels and 64
annotations per alert, 256-byte keys, and 8192-byte values. Invalid input fails
without committing any part of the batch. Oversized bodies return `413`, invalid
supported fields `422`, and busy or unavailable storage `503`.

Configure upstream grouping to stay within these bounds. This service never
silently slices a batch to fit; a rejected batch needs upstream correction,
not assumptions that every producer retries every 4xx response. Configure
Alertmanager `max_alerts: 0` to avoid intentional truncation, and separately
monitor upstream delivery failures.

Eight admitted authenticated requests and eight outstanding blocking database
jobs bound application work. A timed-out request can still commit; retried
observations are therefore notification-idempotent, not event-log-idempotent.
The database guard remains held by a running job even if its HTTP caller exits.
The 30-second request timeout does not make an ambiguous write a failed write.

The main database is capped at 65,536 SQLite pages, approximately 256 MiB with
the normal 4096-byte page size. WAL, credentials, and backups need additional
disk space. Full storage returns a retryable failure rather than false success.
Automatic retention and disk-capacity metrics are not implemented yet.

## Architecture boundary

Run alongside an **independent external watchdog plane**, outside the cluster it
observes. The router handles the watchdog's incoming observations and delivery
policy; it does not replace detection or demand another identical watchdog.
Process liveness cannot prove that the watched cluster is healthy. A source
heartbeat must distinguish unknown coverage from a confirmed outage.

The single-process SQLite design is restart-durable, not active-active HA.
Use a local or block-backed filesystem, not a shared network filesystem. Backups
must use a SQLite-safe mechanism, and database loss is genuine history loss:
Alertmanager repeats can only restore still-firing observations, not resolved
events or prior delivery history.

## Next vertical slice

1. Independently observed source heartbeats and explicit coverage-gap routing.
2. Allowlisted context and optional Cloudflare Clef **shadow assessments**.
   Keep source severity, assessed impact, and notification action distinct.
   A trusted urgent floor must never wait for or be vetoed by the model.
3. Explicitly enabled Discord delivery with a durable pending outbox, backoff,
   rate-limit handling, remote identifiers, and ambiguous-send accounting.
4. Retention, backup/restore verification, tested OCI packaging, and generic
   Kubernetes manifests. Private deployment settings belong outside this project.

No generic second input protocol, incident platform, acknowledgment UI, automatic
remediation, or agent framework is needed for this initial scope. The optional
digest becomes a view over stored observations, not the urgency scheduler.

## Upstream contracts

- [Alertmanager webhook configuration](https://prometheus.io/docs/alerting/latest/configuration/#webhook_config)
- [SQLite WAL constraints](https://sqlite.org/wal.html)
- [Cloudflare Clef Flash](https://developers.cloudflare.com/workers-ai/models/clef-flash/)

All checked-in examples are synthetic. No private infrastructure configuration,
conversation transcripts, recipient identifiers, or credentials belong here.

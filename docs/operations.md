# Operating Criers

This page is the operator contract: what Criers expects from Alertmanager,
what each response means, how delivery behaves under failure, and what the
Kubernetes packaging assumes.

## Alertmanager

Each Alertmanager setup is a named source. Its identity is the URL path plus a
credential configured for that source; labels and annotations never select a
source, a destination or a mention.

```yaml
receivers:
  - name: criers
    webhook_configs:
      - url: http://criers.criers.svc:8080/v1/sources/prod/alertmanager
        send_resolved: true
        max_alerts: 512
        timeout: 10s
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/secrets/criers-token
```

- **HA peers.** Configure every peer of one Alertmanager cluster with the same
  source URL and credential. Peers already deduplicate notifications through
  gossip; when both still send, Criers deduplicates by source, fingerprint and
  `startsAt`.
- **Batch size.** Criers admits at most 512 alerts and `server.max_body_bytes`
  (1 MiB by default) per request. Keep `max_alerts` at 512 or lower. Alertmanager
  reports omitted alerts in `truncatedAlerts`, which Criers records as a coverage
  gap. It never treats missing alerts as resolved.
- **Receivers.** `sources.<id>.receivers` maps Alertmanager receiver names to
  destinations. Other receivers use `default_destination`.
- **Heartbeat.** Route an always-firing `Watchdog` alert to the same receiver
  with a short `repeat_interval` (for example `1m`), and set
  `sources.<id>.heartbeat.stale_after` to several intervals. Heartbeats update
  source freshness without storing the payload.
- **Latency.** Alertmanager's `group_wait`, `group_interval` and
  `repeat_interval` still apply before Criers sees an alert.

## HTTP contract

Ingestion listens on `server.listen`; probes and metrics listen on
`server.ops_listen`.

| Response | Meaning | Alertmanager |
| --- | --- | --- |
| `202` | Committed to SQLite, including decisions and intents | done |
| `401` | Unknown source or a credential not valid for this source | not retried |
| `413` | Body over `server.max_body_bytes` | not retried |
| `422` | Unsupported version, more than 512 alerts, malformed identity or fields over limits | not retried |
| `503` + `Retry-After` | Admission full, shutting down, or storage unavailable | retried |

Alertmanager retries 5xx responses within a notification. It does not retry a
4xx, but a failed notification is not recorded in its notification log, so the
next `group_interval` flush sends the group again. A transient 4xx therefore
delays an alert rather than losing it; a request that is permanently too large
fails on every flush until `max_alerts` or `max_body_bytes` changes. Criers never
answers a permanent problem with 503, so oversize input cannot cause a retry loop.

`GET /v1/sources/<id>/status` returns counts and freshness for that source, with
the same credential. It shares ingestion's admission limit.

## Policy

`[policy.severity]` maps trusted severity labels to `record`, `quiet_post` or
`ping`. Missing or unmapped severities use `unknown_severity`, which defaults to
`ping`. Severities in `critical_floor` create a delivery intent immediately and
are never sent to assessment. A source with `trust_severity = false` always gets
`unknown_severity` and never reaches the floor.

Within one occurrence (source, fingerprint, `startsAt`) escalation only goes up:
warning then critical sends again, critical then warning does not. A later
`startsAt` is a new occurrence. An older `startsAt` than one already seen is
recorded as superseded and changes nothing.

## Delivery

Only `mode = "live"` sends. Intents recorded in shadow mode stay shadow when the
mode changes. As a consequence, an alert that is already firing when you switch
to live, and was already recorded in shadow mode, is not paged at the cutover;
only a recurrence, an escalation or a new alert produces a live message. Review
the firing alerts in Alertmanager when you switch.

- **One owner.** Each destination has one worker. It marks rows in flight in
  SQLite before every request and records the outcome after it, retrying that
  record until it succeeds or Criers shuts down. Open deliveries for a
  destination removed from the configuration end as failed (or ambiguous) at
  the next live startup.
- **Discord.** Requests use `?wait=true` and an explicit `allowed_mentions`
  with `parse: []`. Only configured user and role IDs can be mentioned; alert
  text is escaped so it cannot form mentions or links.
- **Rate limits.** A 429 pauses the destination for the longer of `Retry-After`
  and the body's `retry_after`, capped by `delivery.max_rate_limit_wait`, plus
  jitter. The pause is stored, so it survives a restart.
- **Retries.** Transport errors and 5xx retry with capped, jittered exponential
  backoff up to `delivery.max_attempts`. Other 4xx responses (bad request,
  unknown webhook, forbidden) fail permanently.
- **Ambiguous sends.** A timeout after the request was sent, a 500, 502 or 504,
  or a crash while a send was in flight may have delivered the message. Criers
  records that, retries anyway and marks the row as a possible duplicate. If
  retries run out it ends as `ambiguous`, not `failed`. Expect occasional
  duplicate messages; Criers prefers a duplicate to a silent loss.
- **Ordering.** Messages for one occurrence go out in order. A recovery waits
  until every earlier send for that occurrence has finished.
- **Recovery.** A resolution produces a recovery message only if a firing
  message was delivered or may have been. If the alert resolves before its first
  send was attempted, the pending send is cancelled and nothing is posted; the
  decision history keeps `resolution_without_notification`.
- **Storms.** Due rows for a destination are batched into one message (up to
  `delivery.batch_max` lines within Discord's 2000-character limit). A batch
  mentions recipients at most once, and within `mention_cooldown` of the last
  mention a ping is posted with a "mention coalesced" note instead. Every alert
  is still posted.

## Assessment

`[assessment]` enables Cloudflare Clef or Clef-flash through the Workers AI
REST endpoint. It runs only in shadow mode: results are recorded, notifications
never change, and provider failure never delays routing because assessment
starts after the webhook commit.

Only the labels and annotations listed in the configuration are sent, each
truncated to `max_value_chars`. Results store the impact probability, urgency
score and probabilities, confidence, requested and reported model, question set
and policy version, the context hash and the list of fields sent. Outcomes are
`assessed`, `stale` (the occurrence resolved first), `timeout`, `rate_limited`,
`http_error`, `malformed`, `transport_error`, `skipped_closed` and
`skipped_queue_full` when `max_pending` is reached.

## Credentials and rotation

Each `credential_files` entry holds one token of 32 to 256 printable characters.
Sources may not share a token. Criers rereads the files every
`credential_reload_interval`, never per request. To rotate:

1. Add the new token as a second file and roll out the configuration.
2. Move Alertmanager to the new token.
3. Replace the old file's content with the new token, or remove it and roll out.

If a file becomes unreadable or invalid, Criers keeps that file's last valid
token and increments `criers_credential_reload_failures_total`. Configuration
changes otherwise need an ordinary restart.

## Storage and retention

SQLite runs in WAL mode with full synchronous commits. `storage.max_size_mib`
caps the main database file; the WAL is truncated to 64 MiB after checkpoints,
so size the volume above the cap.

Retention runs every `retention_interval`. Each transaction deletes at most
`retention_batch` decisions, webhook records, closed occurrences, finished
deliveries and finished assessments; deleting a closed occurrence also removes
its own few remaining child rows. It removes webhook metadata and decisions
older than `retention`, resolved or superseded occurrences closed before then,
and finished deliveries and assessments. It keeps unresolved occurrences, any
occurrence with a pending or in-flight delivery, and the delivery records of
alerts that are still firing, because their recovery depends on them.

The replay horizon equals `retention`: a webhook replaying a resolved occurrence
older than that is treated as new. Keep `retention` well above Alertmanager's
`--data.retention` (120h by default). Repeated observations of one occurrence
are compacted into a counter instead of new decision rows.

When the database is full, ingestion returns 503 and readiness fails. Deleted
rows free pages for reuse, so retention or a larger cap recovers without manual
repair.

## Health and metrics

- `/health/live` reports that the process is serving.
- `/health/ready` fails when the most recent storage write failed, or when
  neither an ingestion commit nor the background probe succeeded within three
  probe intervals. The probe commits a small write and test-allocates pages
  every `storage.health_interval`, so a full or read-only database is detected
  without traffic. Readiness never takes an ingestion slot, so a busy receiver
  stays ready.
- `/metrics` exposes OpenMetrics. Labels are configured source and destination
  IDs and fixed outcome or reason names; never fingerprints or alert text.

| Metric | Use |
| --- | --- |
| `criers_ingest_requests_total{source,outcome}` | accepted, unauthorized, invalid, too_large, body_error, busy, draining, storage_error |
| `criers_ingest_duration_seconds` | accepted request time including the commit |
| `criers_decisions_total{action,reason}` | policy outcomes |
| `criers_source_heartbeat_age_seconds`, `criers_source_coverage{source,state}` | coverage: unknown, healthy, stale, never_seen |
| `criers_delivery_pending`, `criers_delivery_oldest_pending_age_seconds` | outbox backlog per destination |
| `criers_delivery_attempts_total{destination,outcome}` | sent, rate_limited, retryable, ambiguous, permanent |
| `criers_assessments_total{outcome}`, `criers_assessment_queue` | shadow assessment |
| `criers_storage_used_bytes`, `criers_storage_limit_bytes` | storage headroom |
| `criers_credential_files_valid`, `criers_credential_reload_failures_total` | rotation health |

Coverage changes create durable notifications to the heartbeat destination: no
heartbeat within `stale_after` of startup (`never_seen`), heartbeats stopped
(`stale`), and heartbeats resumed. They do not affect readiness. An outside
watchdog is still needed to notice that Criers itself is down.

## Shutdown

On SIGTERM or SIGINT Criers stops admitting webhooks (503), lets in-flight
requests finish their commits, then stops workers. Each phase waits at most
`server.shutdown_grace`. A send cut off by the deadline stays in flight and is
retried after restart as a possible duplicate.

## Kubernetes

[deploy/kubernetes](../deploy/kubernetes) is a minimal kustomization: one
replica with the `Recreate` strategy, a ConfigMap, two Secrets mounted through a
projected volume (so updates reach the files), a PersistentVolumeClaim, probes,
resources, and a non-root, read-only-root container. Replace the placeholder
image and secret values; the placeholders fail validation on purpose.

Run exactly one Criers per database. The claim requests `ReadWriteOncePod`,
which needs a CSI driver that supports it and then prevents a second pod from
mounting the volume. Plain `ReadWriteOnce` only restricts mounting to one node.
Criers also takes an exclusive lock file next to the database, which stops a
second process on the same node but does not fence writers on other nodes over
network storage.

Build the image with `mise run image`. It compiles with the toolchain pinned in
`mise.toml` on Debian 13 and runs on distroless Debian 13 as UID 65532, with
`/var/lib/criers` pre-owned by that user.

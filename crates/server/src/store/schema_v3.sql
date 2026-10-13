-- Test fixture: schema 3 exactly as Criers 0.1.0 (23cb50e) created it.
-- Used only by the upgrade regression; never applied by the runtime.

CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

-- Allocation probe target; rows are only ever inserted inside rolled-back transactions.
CREATE TABLE probe (payload BLOB NOT NULL) STRICT;

-- Webhook metadata. Payloads are not retained: occurrences keep compact state.
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    receiver TEXT NOT NULL,
    group_key TEXT NOT NULL,
    alerts INTEGER NOT NULL,
    truncated_alerts INTEGER NOT NULL
) STRICT;
CREATE INDEX events_received ON events(received_at);
CREATE INDEX events_source ON events(source);

CREATE TABLE occurrences (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('firing', 'resolved', 'superseded')),
    highest_action INTEGER NOT NULL,
    alertname TEXT NOT NULL,
    severity TEXT,
    labels TEXT NOT NULL,
    summary TEXT NOT NULL,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    observations INTEGER NOT NULL,
    closed_at INTEGER,
    UNIQUE(source, fingerprint, starts_at)
) STRICT;
CREATE INDEX occurrences_closed ON occurrences(closed_at) WHERE closed_at IS NOT NULL;

-- Changes only; repeated identical observations are compacted into occurrence counters.
CREATE TABLE decisions (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    occurrence_id INTEGER NOT NULL REFERENCES occurrences(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    intent_id INTEGER
) STRICT;
CREATE INDEX decisions_event ON decisions(event_id);
CREATE INDEX decisions_occurrence ON decisions(occurrence_id);

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    occurrence_id INTEGER REFERENCES occurrences(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('notify', 'recovery', 'coverage')),
    action TEXT NOT NULL CHECK (action IN ('quiet_post', 'ping')),
    mode TEXT NOT NULL CHECK (mode IN ('shadow', 'live')),
    state TEXT NOT NULL CHECK (state IN
        ('shadow', 'pending', 'in_flight', 'sent', 'ambiguous', 'failed', 'superseded')),
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    due_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    throttled INTEGER NOT NULL DEFAULT 0,
    possibly_delivered INTEGER NOT NULL DEFAULT 0,
    last_status INTEGER,
    last_error TEXT,
    remote_id TEXT,
    completed_at INTEGER,
    CHECK ((mode = 'shadow') = (state = 'shadow'))
) STRICT;
-- One intent per occurrence, kind, action and destination: a recovery goes to
-- each destination that was notified.
CREATE UNIQUE INDEX outbox_intent ON outbox(occurrence_id, kind, action, destination)
    WHERE occurrence_id IS NOT NULL;
CREATE INDEX outbox_open ON outbox(destination, due_at, id)
    WHERE state IN ('pending', 'in_flight');
CREATE INDEX outbox_occurrence ON outbox(occurrence_id);
CREATE INDEX outbox_source ON outbox(source);
CREATE INDEX outbox_completed ON outbox(completed_at) WHERE completed_at IS NOT NULL;

CREATE TABLE destination_state (
    destination TEXT PRIMARY KEY,
    paused_until INTEGER NOT NULL DEFAULT 0,
    last_mention_at INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE assessments (
    id INTEGER PRIMARY KEY,
    occurrence_id INTEGER NOT NULL UNIQUE REFERENCES occurrences(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'in_flight', 'done')),
    outcome TEXT,
    context TEXT NOT NULL,
    context_sha256 TEXT NOT NULL,
    context_fields TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    requested_model TEXT NOT NULL,
    model TEXT,
    impact_probability REAL,
    urgency_score REAL,
    urgency_probabilities TEXT,
    confidence REAL,
    input_tokens INTEGER,
    latency_ms INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    due_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    error TEXT
) STRICT;
CREATE INDEX assessments_open ON assessments(due_at, id) WHERE state IN ('pending', 'in_flight');
CREATE INDEX assessments_done ON assessments(completed_at) WHERE state = 'done';

CREATE TABLE source_status (
    source TEXT PRIMARY KEY,
    last_event_at INTEGER,
    truncated_alerts INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at INTEGER,
    coverage TEXT NOT NULL DEFAULT 'unknown'
        CHECK (coverage IN ('unknown', 'healthy', 'stale', 'never_seen')),
    coverage_changed_at INTEGER,
    -- The configured heartbeat alertname that last_heartbeat_at and coverage refer to.
    heartbeat_alertname TEXT
) STRICT;

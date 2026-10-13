-- Upgrades a schema-3 database to schema 4. Runs in one transaction; every
-- existing row, ID and delivery state is kept.

-- Summaries are delivered through the outbox. SQLite cannot widen a CHECK
-- constraint in place, so the table is rebuilt with identical columns and rows.
CREATE TABLE outbox_v4 (
    id INTEGER PRIMARY KEY,
    occurrence_id INTEGER REFERENCES occurrences(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('notify', 'recovery', 'coverage', 'summary')),
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
INSERT INTO outbox_v4(id, occurrence_id, source, destination, kind, action, mode, state,
    content, created_at, due_at, attempts, throttled, possibly_delivered, last_status,
    last_error, remote_id, completed_at)
SELECT id, occurrence_id, source, destination, kind, action, mode, state,
    content, created_at, due_at, attempts, throttled, possibly_delivered, last_status,
    last_error, remote_id, completed_at
FROM outbox;
DROP TABLE outbox;
ALTER TABLE outbox_v4 RENAME TO outbox;
CREATE UNIQUE INDEX outbox_intent ON outbox(occurrence_id, kind, action, destination)
    WHERE occurrence_id IS NOT NULL;
CREATE INDEX outbox_open ON outbox(destination, due_at, id)
    WHERE state IN ('pending', 'in_flight');
CREATE INDEX outbox_occurrence ON outbox(occurrence_id);
CREATE INDEX outbox_source ON outbox(source);
CREATE INDEX outbox_completed ON outbox(completed_at) WHERE completed_at IS NOT NULL;

-- Digest ranks between record (0) and quiet_post, which moves up one place.
UPDATE occurrences SET highest_action = highest_action + 1 WHERE highest_action > 0;
CREATE INDEX occurrences_open ON occurrences(first_seen_at) WHERE closed_at IS NULL;

CREATE TABLE heartbeat_gaps (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    -- NULL when the heartbeat had never been recorded and monitoring's start is unknown.
    started_at INTEGER,
    ended_at INTEGER NOT NULL
) STRICT;
CREATE INDEX heartbeat_gaps_source ON heartbeat_gaps(source, ended_at);
CREATE INDEX heartbeat_gaps_ended ON heartbeat_gaps(ended_at);

CREATE TABLE summaries (
    window_end INTEGER PRIMARY KEY,
    window_start INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    outbox_id INTEGER REFERENCES outbox(id) ON DELETE SET NULL
) STRICT;

-- When monitoring of the configured heartbeat began, until its first heartbeat
-- arrives. Schema 3 kept that start only for a watched source still in its
-- `unknown` state (coverage_changed_at is not moved while unknown). For a
-- `never_seen` source it is lost: such a source stays without a marker and is
-- reported as never having had a recorded heartbeat, with no invented start.
ALTER TABLE source_status ADD COLUMN unobserved_since INTEGER;
UPDATE source_status SET unobserved_since = coverage_changed_at
WHERE heartbeat_alertname IS NOT NULL AND last_heartbeat_at IS NULL AND coverage = 'unknown';

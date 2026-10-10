PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
-- Limit main database pages, not total disk consumption including WAL and backups.
PRAGMA max_page_count = 65536;

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS occurrences (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('firing', 'resolved')),
    highest_action INTEGER NOT NULL,
    UNIQUE(source, fingerprint, starts_at)
) STRICT;

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES events(id),
    occurrence_id INTEGER NOT NULL REFERENCES occurrences(id),
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    intent_created INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    occurrence_id INTEGER NOT NULL REFERENCES occurrences(id),
    kind TEXT NOT NULL,
    action TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode = 'shadow'),
    created_at TEXT NOT NULL,
    UNIQUE(occurrence_id, kind)
) STRICT;

CREATE TABLE IF NOT EXISTS source_status (
    source TEXT PRIMARY KEY,
    last_event_at TEXT NOT NULL,
    truncated_alerts INTEGER NOT NULL
) STRICT;

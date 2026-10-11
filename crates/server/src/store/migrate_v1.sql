-- Upgrades a schema-1 database written by an earlier build of this release to
-- schema 2. Runs in one transaction; rows and their states are untouched.

-- A recovery goes to each notified destination, so the intent key includes it.
-- Widening a unique index cannot conflict with rows the narrower one admitted.
DROP INDEX outbox_intent;
CREATE UNIQUE INDEX outbox_intent ON outbox(occurrence_id, kind, action, destination)
    WHERE occurrence_id IS NOT NULL;

-- Indexes added after the first schema-1 build.
CREATE INDEX IF NOT EXISTS events_source ON events(source);
CREATE INDEX IF NOT EXISTS outbox_source ON outbox(source);
CREATE INDEX IF NOT EXISTS assessments_done ON assessments(completed_at) WHERE state = 'done';

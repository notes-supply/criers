-- Upgrades a schema-2 database to schema 3. Runs in one transaction.

-- The configured heartbeat alertname that last_heartbeat_at and coverage refer
-- to. Existing rows have no recorded identity, so their freshness is discarded
-- once and coverage returns to the startup grace period.
ALTER TABLE source_status ADD COLUMN heartbeat_alertname TEXT;

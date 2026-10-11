-- Upgrades the original shadow-only scaffold (user_version 0) to the current schema.
-- Runs inside one transaction after schema.sql has created the new tables, with the
-- scaffold tables copied to temp.legacy_* and dropped beforehand.

INSERT INTO events(id, source, received_at, receiver, group_key, alerts, truncated_alerts)
SELECT
    id,
    source,
    coalesce(CAST(round(unixepoch(received_at, 'subsec') * 1000) AS INTEGER), 0),
    CASE WHEN json_valid(payload) THEN coalesce(json_extract(payload, '$.receiver'), '') ELSE '' END,
    CASE WHEN json_valid(payload) THEN coalesce(json_extract(payload, '$.groupKey'), '') ELSE '' END,
    CASE WHEN json_valid(payload) THEN coalesce(json_array_length(payload, '$.alerts'), 0) ELSE 0 END,
    CASE WHEN json_valid(payload) THEN coalesce(json_extract(payload, '$.truncatedAlerts'), 0) ELSE 0 END
FROM temp.legacy_events;

CREATE TEMP TABLE legacy_seen AS
SELECT d.occurrence_id AS id,
    min(e.received_at) AS first_seen,
    max(e.received_at) AS last_seen,
    count(*) AS observations,
    max(e.id) AS event_id
FROM temp.legacy_decisions d
JOIN events e ON e.id = d.event_id
GROUP BY d.occurrence_id;

INSERT INTO occurrences(id, source, fingerprint, starts_at, status, highest_action, alertname,
    severity, labels, summary, first_seen_at, last_seen_at, observations, closed_at)
SELECT
    o.id, o.source, o.fingerprint, o.starts_at, o.status, o.highest_action,
    coalesce(json_extract(alert.value, '$.labels.alertname'), ''),
    json_extract(alert.value, '$.labels.severity'),
    coalesce(json_extract(alert.value, '$.labels'), '{}'),
    coalesce(substr(json_extract(alert.value, '$.annotations.summary'), 1, 512), ''),
    coalesce(seen.first_seen, 0),
    coalesce(seen.last_seen, 0),
    coalesce(seen.observations, 0),
    CASE WHEN o.status = 'resolved' THEN coalesce(seen.last_seen, 0) END
FROM temp.legacy_occurrences o
LEFT JOIN temp.legacy_seen seen ON seen.id = o.id
LEFT JOIN temp.legacy_events le ON le.id = seen.event_id AND json_valid(le.payload)
LEFT JOIN json_each(le.payload, '$.alerts') alert
    ON lower(json_extract(alert.value, '$.fingerprint')) = o.fingerprint
GROUP BY o.id;

-- The scaffold never marked older starts of a fingerprint as superseded.
UPDATE occurrences SET status = 'superseded', closed_at = last_seen_at
WHERE status = 'firing' AND EXISTS (SELECT 1 FROM occurrences newer
    WHERE newer.source = occurrences.source AND newer.fingerprint = occurrences.fingerprint
    AND newer.starts_at > occurrences.starts_at);

INSERT INTO decisions(id, event_id, occurrence_id, action, reason, policy_version)
SELECT id, event_id, occurrence_id, action, reason, policy_version
FROM temp.legacy_decisions;

-- Scaffold intents were shadow-only and stay shadow: they are never promoted to live sends.
INSERT INTO outbox(id, occurrence_id, source, destination, kind, action, mode, state, content,
    created_at, due_at, completed_at)
SELECT
    b.id, b.occurrence_id, o.source, '',
    CASE b.kind WHEN 'recovery' THEN 'recovery' ELSE 'notify' END,
    CASE WHEN b.action IN ('quiet_post', 'ping') THEN b.action ELSE 'quiet_post' END,
    'shadow', 'shadow', '',
    coalesce(CAST(round(unixepoch(b.created_at, 'subsec') * 1000) AS INTEGER), 0),
    coalesce(CAST(round(unixepoch(b.created_at, 'subsec') * 1000) AS INTEGER), 0),
    coalesce(CAST(round(unixepoch(b.created_at, 'subsec') * 1000) AS INTEGER), 0)
FROM temp.legacy_outbox b
JOIN temp.legacy_occurrences o ON o.id = b.occurrence_id;

INSERT INTO source_status(source, last_event_at, truncated_alerts)
SELECT source,
    CAST(round(unixepoch(last_event_at, 'subsec') * 1000) AS INTEGER),
    truncated_alerts
FROM temp.legacy_source_status;

DROP TABLE temp.legacy_seen;
DROP TABLE temp.legacy_events;
DROP TABLE temp.legacy_occurrences;
DROP TABLE temp.legacy_decisions;
DROP TABLE temp.legacy_outbox;
DROP TABLE temp.legacy_source_status;

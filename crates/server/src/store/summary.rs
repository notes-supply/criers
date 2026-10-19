//! Builds and queues the scheduled summary inside one store transaction.
//!
//! Every query is bounded by the reporting window through an index: open
//! occurrences, occurrences closed since the window began, heartbeat gaps,
//! truncated events and finished deliveries. Nothing scans the whole history,
//! and only what a summary can display is read into memory: section totals are
//! aggregated in SQL and at most [`MAX_GROUP_LINES`] groups per section are read.

use criers_core::Action;
use rusqlite::{OptionalExtension, Transaction, params};

use super::{FirstDue, HeartbeatStatus, NewIntent, StoreError, insert_intent, watch_heartbeat};
use crate::{
    config::{Config, Heartbeat},
    render::{Tier, tier_sql},
    summary::{
        Count, Coverage, Group, MAX_GAP_LINES, MAX_GROUP_LINES, Ongoing, Report, Schedule, Section,
    },
};

/// Closed heartbeat gaps counted per source and summary. A flapping source
/// cannot make one summary unbounded; beyond this the count says "at least".
pub(crate) const MAX_GAPS: i64 = 10_000;

/// What a summary tick did. Each variant carries the summary cursor: the end of
/// the latest reported window.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SummaryTick {
    /// Reporting starts at this window end; nothing was queued.
    Started(i64),
    /// The latest window up to this end is already reported.
    Current(i64),
    /// A summary of the window ending here was queued.
    Queued(i64),
}

impl SummaryTick {
    pub(crate) fn cursor(&self) -> i64 {
        match *self {
            Self::Started(end) | Self::Current(end) | Self::Queued(end) => end,
        }
    }
}

/// Queues at most one summary, covering everything since the previous one.
///
/// The first tick only records where reporting starts. After downtime a single
/// summary covers every missed day rather than one per day; its start is
/// clamped to the retention cutoff, the oldest point history is complete for.
pub(super) fn tick(
    transaction: &Transaction<'_>,
    config: &Config,
    now: i64,
) -> Result<Option<SummaryTick>, StoreError> {
    let Some(settings) = &config.summary else {
        return Ok(None);
    };
    let end = Schedule::new(settings.at).latest(now);
    let cursor: Option<i64> =
        transaction.query_row("SELECT max(window_end) FROM summaries", [], |row| {
            row.get(0)
        })?;
    let start = match cursor {
        None => {
            transaction.execute(
                "INSERT INTO summaries(window_end, window_start, created_at) VALUES (?1, ?1, ?2)",
                params![end, now],
            )?;
            return Ok(Some(SummaryTick::Started(end)));
        }
        Some(cursor) if cursor >= end => return Ok(Some(SummaryTick::Current(cursor))),
        Some(cursor) => {
            let retained = now.saturating_sub(
                i64::try_from(config.storage.retention.as_millis()).unwrap_or(i64::MAX),
            );
            cursor.max(retained).min(end)
        }
    };
    // Coverage is read only for heartbeats being watched, whichever of this and the
    // coverage tick runs first after a configuration change.
    for (source, settings) in &config.sources {
        if let Some(heartbeat) = &settings.heartbeat {
            watch_heartbeat(transaction, source, heartbeat, now)?;
        }
    }
    let report = report(transaction, config, start, end, now)?;
    let intent = NewIntent {
        occurrence: None,
        source: "",
        destination: &settings.destination,
        kind: "summary",
        action: Action::QuietPost,
        content: report.card(&config.icons(&settings.destination)).stored(),
    };
    let outbox = insert_intent(transaction, config.mode, &intent, now)?;
    transaction.execute(
        "INSERT INTO summaries(window_end, window_start, created_at, outbox_id)
         VALUES (?1, ?2, ?3, ?4)",
        params![end, start, now, outbox],
    )?;
    Ok(Some(SummaryTick::Queued(end)))
}

fn report(
    transaction: &Transaction<'_>,
    config: &Config,
    start: i64,
    end: i64,
    now: i64,
) -> Result<Report, StoreError> {
    let mut report = Report {
        start,
        end,
        generated_at: now,
        ..Report::default()
    };
    report.open = section(transaction, "open", start, end)?;
    report.resolved = section(transaction, "resolved", start, end)?;
    // History can hold sources no longer configured; name sources whenever the
    // listed groups are not all from the only configured one.
    report.show_source = config.sources.len() > 1
        || report
            .open
            .groups
            .iter()
            .chain(&report.resolved.groups)
            .any(|group| !config.sources.contains_key(&group.source));
    for (source, settings) in &config.sources {
        if let Some(heartbeat) = &settings.heartbeat {
            let coverage = coverage(transaction, source, heartbeat, start, end)?;
            if coverage.gap_count > 0 || coverage.ongoing.is_some() {
                report.coverage.push(coverage);
            }
        }
    }
    report.truncated = transaction
        .prepare_cached(
            "SELECT source, sum(truncated_alerts) FROM events
             WHERE received_at >= ?1 AND received_at < ?2 AND truncated_alerts > 0
             GROUP BY source ORDER BY source",
        )?
        .query_map(params![start, end], |row| Ok((row.get(0)?, count(row, 1)?)))?
        .collect::<Result<_, _>>()?;
    (report.failed, report.uncertain) = transaction.query_row(
        "SELECT coalesce(sum(state = 'failed'), 0), coalesce(sum(state = 'ambiguous'), 0)
         FROM outbox WHERE completed_at >= ?1 AND completed_at < ?2
         AND mode = 'live' AND state IN ('failed', 'ambiguous')",
        params![start, end],
        |row| Ok((count(row, 0)?, count(row, 1)?)),
    )?;
    Ok(report)
}

/// A SQL count; counts are never negative.
fn count(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    Ok(u64::try_from(row.get::<_, i64>(index)?).unwrap_or(0))
}

/// Occurrences that notify (digest or higher) with their section and tier: open
/// if it started before the window end and had not closed by then, resolved if
/// it resolved inside the window. Both branches of the union use an index: the
/// open one on first_seen_at for unclosed occurrences, the closed one on
/// closed_at. Parameters: ?1 window start, ?2 window end, ?3 digest rank.
macro_rules! candidates {
    () => {
        concat!(
            "WITH candidates AS (
            SELECT 'open' AS category, source, alertname, severity, highest_action,
                first_seen_at, summary, labels, ",
            tier_sql!(),
            " AS tier
            FROM occurrences
            WHERE closed_at IS NULL AND first_seen_at < ?2 AND highest_action >= ?3
            UNION ALL
            SELECT CASE WHEN closed_at >= ?2 THEN 'open' ELSE 'resolved' END, source,
                alertname, severity, highest_action, first_seen_at, summary, labels, ",
            tier_sql!(),
            "
            FROM occurrences
            WHERE closed_at >= ?1 AND first_seen_at < ?2 AND highest_action >= ?3
            AND (closed_at >= ?2 OR status = 'resolved')
        ) "
        )
    };
}

/// The resource an alert is about; [`crate::render::resource`] follows the same
/// order of preference, and a test keeps the two in agreement.
macro_rules! context {
    ($labels:literal) => {
        concat!(
            "coalesce(nullif(concat_ws('/', json_extract(",
            $labels,
            ", '$.namespace'),
                coalesce(json_extract(",
            $labels,
            ", '$.pod'),
                    json_extract(",
            $labels,
            ", '$.deployment'),
                    json_extract(",
            $labels,
            ", '$.statefulset'),
                    json_extract(",
            $labels,
            ", '$.daemonset'),
                    json_extract(",
            $labels,
            ", '$.job_name'),
                    json_extract(",
            $labels,
            ", '$.container'))), ''),
            json_extract(",
            $labels,
            ", '$.host'),
            json_extract(",
            $labels,
            ", '$.service'),
            json_extract(",
            $labels,
            ", '$.instance'),
            json_extract(",
            $labels,
            ", '$.job'))"
        )
    };
}

/// One section: true totals per tier from an aggregate, and each tier's most
/// important groups (highest action, then size) read in display order, at most
/// [`MAX_GROUP_LINES`] per tier.
fn section(
    transaction: &Transaction<'_>,
    category: &str,
    start: i64,
    end: i64,
) -> Result<Section, StoreError> {
    let digest = Action::Digest.rank();
    let mut counts = [Count::default(); 3];
    let mut rows = transaction.prepare_cached(concat!(
        candidates!(),
        "SELECT tier, coalesce(sum(size), 0), coalesce(sum(fresh), 0), count(*)
         FROM (SELECT tier, count(*) AS size, sum(first_seen_at >= ?1) AS fresh
            FROM candidates WHERE category = ?4
            GROUP BY source, alertname, severity, tier)
         GROUP BY tier"
    ))?;
    let mut totals = rows.query(params![start, end, digest, category])?;
    while let Some(row) = totals.next()? {
        counts[Tier::from_index(row.get(0)?).index()] = Count {
            occurrences: count(row, 1)?,
            new: count(row, 2)?,
            groups: count(row, 3)?,
        };
    }
    let groups = transaction
        .prepare_cached(concat!(
            candidates!(),
            "SELECT source, alertname, severity, tier, size, fresh, summary, first, last
             FROM (SELECT source, alertname, severity, tier, count(*) AS size,
                    coalesce(sum(first_seen_at >= ?1), 0) AS fresh, max(summary) AS summary,
                    min(",
            context!("labels"),
            ") AS first, max(",
            context!("labels"),
            ") AS last,
                    row_number() OVER (PARTITION BY tier ORDER BY max(highest_action) DESC,
                        count(*) DESC, alertname, severity, source) AS rank
                FROM candidates WHERE category = ?4
                GROUP BY source, alertname, severity, tier)
             WHERE rank <= ?5
             ORDER BY tier, rank"
        ))?
        .query_map(
            params![start, end, digest, category, MAX_GROUP_LINES as i64],
            |row| {
                let size = count(row, 4)?;
                let summary: Option<String> = row.get(6)?;
                let mut contexts: Vec<String> = Vec::with_capacity(2);
                for index in [7, 8] {
                    if let Some(context) = row.get::<_, Option<String>>(index)?
                        && !contexts.contains(&context)
                    {
                        contexts.push(context);
                    }
                }
                Ok(Group {
                    source: row.get(0)?,
                    alertname: row.get(1)?,
                    severity: row.get(2)?,
                    tier: Tier::from_index(row.get(3)?),
                    count: size,
                    new: count(row, 5)?,
                    contexts,
                    summary: summary.filter(|_| size == 1),
                })
            },
        )?
        .collect::<Result<_, _>>()?;
    Ok(Section { counts, groups })
}

/// Heartbeat coverage of one source inside the window.
///
/// Closed gaps come from `heartbeat_gaps`, counted up to [`MAX_GAPS`]. The state
/// at the window end comes from the latest heartbeat, independent of that cap:
/// a gap that closed after the window end was open at it, and a last heartbeat
/// older than `stale_after` at the window end means a gap is still open. Without
/// any heartbeat of the configured alertname there is no evidence of health.
fn coverage(
    transaction: &Transaction<'_>,
    source: &str,
    heartbeat: &Heartbeat,
    start: i64,
    end: i64,
) -> Result<Coverage, StoreError> {
    let stale_after = i64::try_from(heartbeat.stale_after.as_millis()).unwrap_or(i64::MAX);
    let (gap_count, gap_ms): (u64, i64) = transaction
        .prepare_cached(
            "SELECT count(*),
                coalesce(sum(min(ended_at, ?3) - max(coalesce(started_at, ?2), ?2)), 0)
             FROM (SELECT started_at, ended_at FROM heartbeat_gaps
                WHERE source = ?1 AND ended_at > ?2 AND ended_at < ?3
                ORDER BY ended_at LIMIT ?4)",
        )?
        .query_row(params![source, start, end, MAX_GAPS + 1], |row| {
            Ok((count(row, 0)?, row.get(1)?))
        })?;
    let capped = gap_count > MAX_GAPS as u64;
    let gaps = transaction
        .prepare_cached(
            "SELECT started_at, ended_at FROM heartbeat_gaps
             WHERE source = ?1 AND ended_at > ?2 AND ended_at < ?3
             ORDER BY ended_at LIMIT ?4",
        )?
        .query_map(params![source, start, end, MAX_GAP_LINES as i64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<_, _>>()?;
    // An unknown start (NULL) precedes any window end.
    let closed_later: Option<Option<i64>> = transaction
        .prepare_cached(
            "SELECT started_at FROM heartbeat_gaps
             WHERE source = ?1 AND ended_at >= ?2 AND (started_at IS NULL OR started_at < ?2)
             ORDER BY ended_at LIMIT 1",
        )?
        .query_row(params![source, end], |row| row.get(0))
        .optional()?;
    let status = HeartbeatStatus::read(transaction, source)?;
    let last = status
        .as_ref()
        .and_then(|status| status.last_for(heartbeat));
    let first_due = status
        .as_ref()
        .and_then(|status| status.first_due_for(heartbeat, stale_after));
    let ongoing = match (closed_later, last) {
        // Whether a heartbeat preceded this gap is not recorded, so say only
        // that none arrived.
        (Some(since), _) => Some(Ongoing {
            since,
            recorded: false,
        }),
        (None, Some(last)) => (last < end && end - last > stale_after).then_some(Ongoing {
            since: Some(last + stale_after),
            recorded: true,
        }),
        // No heartbeat of the configured alertname yet: overdue once the startup
        // grace after monitoring began ran out, or since an unknown time if that
        // start was never recorded or the source was never watched.
        (None, None) => match first_due {
            Some(FirstDue::At(due)) => (due < end).then_some(Ongoing {
                since: Some(due),
                recorded: false,
            }),
            Some(FirstDue::Unknown) | None => Some(Ongoing {
                since: None,
                recorded: false,
            }),
        },
    };
    Ok(Coverage {
        source: source.to_owned(),
        gaps,
        gap_count: gap_count.min(MAX_GAPS as u64),
        capped,
        gap_ms,
        ongoing,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::tests::Harness;

    #[tokio::test]
    async fn alerts_of_a_removed_source_are_named_when_one_source_remains() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("token"), crate::tests::TOKEN).unwrap();
        std::fs::write(
            root.join("ops.url"),
            "https://discord.invalid/api/webhooks/1/x",
        )
        .unwrap();
        std::fs::write(
            root.join("criers.toml"),
            "[storage]\npath = \"alerts.sqlite\"\n[sources.example]\n\
             credential_files = [\"token\"]\ndefault_destination = \"ops\"\n\
             [destinations.ops]\nkind = \"discord\"\nwebhook_url_file = \"ops.url\"\n\
             [summary]\ndestination = \"ops\"\nat = \"00:00\"\n",
        )
        .unwrap();
        let config = Arc::new(Config::load(&root.join("criers.toml")).unwrap());
        let store =
            crate::store::Store::open(config.storage.path.clone(), config.storage.max_bytes)
                .await
                .unwrap();
        let now = crate::now_ms();
        let SummaryTick::Started(end) = store
            .summary_tick(Arc::clone(&config), now - crate::summary::DAY_MS)
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("reporting did not start");
        };
        // The same alert from the configured source and from one since removed.
        store
            .with_connection(move |connection| {
                connection.execute(
                    "INSERT INTO occurrences(source, fingerprint, starts_at, status,
                        highest_action, alertname, severity, labels, summary, first_seen_at,
                        last_seen_at, observations)
                     SELECT source, '000000000000000' || n, '2026-10-12T00:00:00.000000000Z',
                        'firing', 3, 'DiskFull', 'critical', '{}', '', ?1, ?1, 1
                     FROM (SELECT 'example' AS source, 1 AS n UNION ALL SELECT 'retired', 2)",
                    [end + 1],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        store.summary_tick(Arc::clone(&config), now).await.unwrap();
        let text: String = store
            .with_connection(|connection| {
                Ok(connection.query_row(
                    "SELECT content FROM outbox WHERE kind = 'summary'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        let text = crate::render::Part::parse(text).text();
        assert!(
            text.contains("example · **DiskFull**") && text.contains("retired · **DiskFull**"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_section_reads_at_most_the_listed_groups_but_counts_all_of_them() {
        let harness = Harness::shadow("").await;
        let section = harness
            .store
            .with_connection(|connection| {
                connection.execute(
                    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 4999)
                     INSERT INTO occurrences(source, fingerprint, starts_at, status,
                        highest_action, alertname, severity, labels, summary, first_seen_at,
                        last_seen_at, observations)
                     SELECT 'example', printf('%016x', i), '2026-10-12T00:00:00.000000000Z',
                        'firing', 1, 'Name' || i, 'warning', '{}', '', 1000, 1000, 1
                     FROM n",
                    [],
                )?;
                let transaction = connection.transaction()?;
                section(&transaction, "open", 0, 2000)
            })
            .await
            .unwrap();
        let total = section.total();
        assert_eq!(
            (total.occurrences, total.groups, section.groups.len()),
            (5000, 5000, MAX_GROUP_LINES)
        );
    }

    /// The SQL used for summaries and the Rust used for alert cards must agree.
    #[test]
    fn tier_and_resource_rules_agree_between_sql_and_cards() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        let severities = [
            Some("critical"),
            Some("CRITICAL"),
            Some("warning"),
            Some("Info"),
            Some("error"),
            Some(""),
            None,
        ];
        let actions = [
            Action::Record,
            Action::Digest,
            Action::QuietPost,
            Action::Ping,
        ];
        for severity in severities {
            for action in actions {
                let sql: i64 = connection
                    .query_row(
                        concat!(
                            "SELECT ",
                            tier_sql!(),
                            " FROM (SELECT ?1 AS severity, ?2 AS highest_action)"
                        ),
                        rusqlite::params![severity, action.rank()],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    Tier::from_index(sql),
                    Tier::of(severity, action),
                    "{severity:?} {action:?}"
                );
            }
        }
        let label_sets = [
            &[
                ("namespace", "media"),
                ("pod", "sonarr-0"),
                ("host", "iris"),
            ][..],
            &[("namespace", "media"), ("deployment", "sonarr")][..],
            &[("namespace", "media")][..],
            &[("container", "app"), ("instance", "10.0.0.1:9100")][..],
            &[("host", "iris"), ("service", "web"), ("job", "node")][..],
            &[
                ("service", "web"),
                ("instance", "10.0.0.1:9100"),
                ("job", "node"),
            ][..],
            &[("instance", "10.0.0.1:9100"), ("job", "node")][..],
            &[("job", "node")][..],
            &[("namespace", ""), ("host", "iris")][..],
            &[][..],
        ];
        for labels in label_sets {
            let map: std::collections::BTreeMap<String, String> = labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            let json = serde_json::to_string(&map).unwrap();
            let sql: Option<String> = connection
                .query_row(concat!("SELECT ", context!("?1")), [json], |row| row.get(0))
                .unwrap();
            assert_eq!(sql, crate::render::resource(&map), "{labels:?}");
        }
    }
}

//! Builds and queues the scheduled summary inside one store transaction.
//!
//! Every query is bounded by the reporting window through an index: open
//! occurrences, occurrences closed since the window began, heartbeat gaps,
//! truncated events and finished deliveries. Nothing scans the whole history,
//! and only what a summary can display is read into memory: section totals are
//! aggregated in SQL and at most [`MAX_GROUP_LINES`] groups per section are read.

use criers_core::Action;
use rusqlite::{OptionalExtension, Transaction, params};

use super::{HeartbeatStatus, NewIntent, StoreError, insert_intent};
use crate::{
    config::{Config, Heartbeat},
    summary::{
        Coverage, Group, MAX_GAP_LINES, MAX_GROUP_LINES, Ongoing, Report, Schedule, Section,
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
    let report = report(transaction, config, start, end, now)?;
    let intent = NewIntent {
        occurrence: None,
        source: "",
        destination: &settings.destination,
        kind: "summary",
        action: Action::QuietPost,
        content: report.render(),
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
        show_source: config.sources.len() > 1,
        ..Report::default()
    };
    report.open = section(transaction, "open", start, end)?;
    report.resolved = section(transaction, "resolved", start, end)?;
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

/// Occurrences that notify (digest or higher) with their section: open if it
/// started before the window end and had not closed by then, resolved if it
/// resolved inside the window. Both branches of the union use an index: the
/// open one on first_seen_at for unclosed occurrences, the closed one on
/// closed_at. Parameters: ?1 window start, ?2 window end, ?3 digest rank.
macro_rules! candidates {
    () => {
        "WITH candidates AS (
            SELECT 'open' AS category, source, alertname, severity, highest_action,
                first_seen_at, summary, labels
            FROM occurrences
            WHERE closed_at IS NULL AND first_seen_at < ?2 AND highest_action >= ?3
            UNION ALL
            SELECT CASE WHEN closed_at >= ?2 THEN 'open' ELSE 'resolved' END, source,
                alertname, severity, highest_action, first_seen_at, summary, labels
            FROM occurrences
            WHERE closed_at >= ?1 AND first_seen_at < ?2 AND highest_action >= ?3
            AND (closed_at >= ?2 OR status = 'resolved')
        ) "
    };
}

/// The resource an alert is about, in the old digest's order of preference.
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
            ", '$.instance'),
            json_extract(",
            $labels,
            ", '$.job'))"
        )
    };
}

/// One section: its true totals from an aggregate, and its most important groups
/// (highest action, then size) read in display order with a limit.
fn section(
    transaction: &Transaction<'_>,
    category: &str,
    start: i64,
    end: i64,
) -> Result<Section, StoreError> {
    let digest = Action::Digest.rank();
    let (total, new, group_count) = transaction
        .prepare_cached(concat!(
            candidates!(),
            "SELECT count(*), coalesce(sum(first_seen_at >= ?1), 0),
                count(DISTINCT source || char(31) || alertname || char(31)
                    || coalesce(severity, char(30)))
             FROM candidates WHERE category = ?4"
        ))?
        .query_row(params![start, end, digest, category], |row| {
            Ok((count(row, 0)?, count(row, 1)?, count(row, 2)?))
        })?;
    let groups = transaction
        .prepare_cached(concat!(
            candidates!(),
            "SELECT source, alertname, severity, count(*) AS size,
                coalesce(sum(first_seen_at >= ?1), 0), max(summary),
                min(",
            context!("labels"),
            "), max(",
            context!("labels"),
            ")
             FROM candidates WHERE category = ?4
             GROUP BY source, alertname, severity
             ORDER BY max(highest_action) DESC, size DESC, alertname, severity, source
             LIMIT ?5"
        ))?
        .query_map(
            params![start, end, digest, category, MAX_GROUP_LINES as i64],
            |row| {
                let size = count(row, 3)?;
                let summary: Option<String> = row.get(5)?;
                let mut contexts: Vec<String> = Vec::with_capacity(2);
                for index in [6, 7] {
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
                    count: size,
                    new: count(row, 4)?,
                    contexts,
                    summary: summary.filter(|_| size == 1),
                })
            },
        )?
        .collect::<Result<_, _>>()?;
    Ok(Section {
        total,
        new,
        group_count,
        groups,
    })
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
            "SELECT count(*), coalesce(sum(min(ended_at, ?3) - max(started_at, ?2)), 0)
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
    let closed_later: Option<i64> = transaction
        .prepare_cached(
            "SELECT started_at FROM heartbeat_gaps
             WHERE source = ?1 AND ended_at >= ?2 AND started_at < ?2
             ORDER BY ended_at LIMIT 1",
        )?
        .query_row(params![source, end], |row| row.get(0))
        .optional()?;
    let status = HeartbeatStatus::read(transaction, source)?;
    let last = status
        .as_ref()
        .and_then(|status| status.last_for(heartbeat));
    let ongoing = match (closed_later, last) {
        (Some(since), _) => Some(Ongoing {
            since,
            recorded: true,
        }),
        (None, Some(last)) => (last < end && end - last > stale_after).then_some(Ongoing {
            since: last + stale_after,
            recorded: true,
        }),
        (None, None) => {
            let since = status
                .and_then(|status| status.changed_at)
                .map_or(start, |changed| changed.max(start));
            (end - since > stale_after).then_some(Ongoing {
                since,
                recorded: false,
            })
        }
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
    use super::*;
    use crate::tests::Harness;

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
        assert_eq!(
            (section.total, section.group_count, section.groups.len()),
            (5000, 5000, MAX_GROUP_LINES)
        );
    }
}

//! Builds and queues the scheduled summary inside one store transaction.
//!
//! Every query is bounded by the reporting window through an index: open
//! occurrences, occurrences closed since the window began, coverage transitions,
//! truncated events and finished deliveries. Nothing scans the whole history.

use std::collections::BTreeMap;

use criers_core::Action;
use rusqlite::{OptionalExtension, Transaction, params};

use super::{NewIntent, StoreError, insert_intent};
use crate::{
    config::Config,
    summary::{Gap, Group, Report, Schedule},
};

/// Coverage transitions read per summary; a flapping source cannot make one
/// summary unbounded. Gaps beyond it are not shown.
const MAX_TRANSITIONS: i64 = 10_000;
/// Example resources fetched per group.
const SAMPLES: i64 = 3;

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
    for (category, group) in groups(transaction, start, end)? {
        match category.as_str() {
            "open" => report.open.push(group),
            _ => report.resolved.push(group),
        }
    }
    report.gaps = gaps(transaction, config, start, end)?;
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

/// Occurrences that notify (digest or higher), grouped by section, source,
/// alert name and severity, with up to [`SAMPLES`] newest examples per group.
///
/// An occurrence is open if it started before the window end and had not
/// closed by then; it is resolved if it resolved inside the window. Both
/// branches of the union use an index: the open one on first_seen_at for
/// unclosed occurrences, the closed one on closed_at.
fn groups(
    transaction: &Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<Vec<(String, Group)>, StoreError> {
    let mut statement = transaction.prepare_cached(
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
        ),
        ranked AS (
            SELECT category, source, alertname, severity, first_seen_at, summary,
                -- The resource an alert is about, in the old digest's order of preference.
                coalesce(
                    nullif(concat_ws('/', json_extract(labels, '$.namespace'),
                        coalesce(json_extract(labels, '$.pod'),
                            json_extract(labels, '$.deployment'),
                            json_extract(labels, '$.statefulset'),
                            json_extract(labels, '$.daemonset'),
                            json_extract(labels, '$.job_name'),
                            json_extract(labels, '$.container'))), ''),
                    json_extract(labels, '$.host'),
                    json_extract(labels, '$.instance'),
                    json_extract(labels, '$.job')) AS context,
                row_number() OVER newest AS sample,
                count(*) OVER whole AS total,
                sum(first_seen_at >= ?1) OVER whole AS fresh,
                max(highest_action) OVER whole AS rank
            FROM candidates
            WINDOW whole AS (PARTITION BY category, source, alertname, severity),
                newest AS (whole ORDER BY first_seen_at DESC)
        )
        SELECT category, source, alertname, severity, rank, total, fresh, summary, context
        FROM ranked WHERE sample <= ?4
        ORDER BY category, source, alertname, severity, sample",
    )?;
    let mut rows = statement.query(params![start, end, Action::Digest.rank(), SAMPLES])?;
    let mut groups: Vec<(String, Group)> = Vec::new();
    while let Some(row) = rows.next()? {
        let category: String = row.get(0)?;
        let source: String = row.get(1)?;
        let alertname: String = row.get(2)?;
        let severity: Option<String> = row.get(3)?;
        let context: Option<String> = row.get(8)?;
        let same = groups.last().is_some_and(|(last, group)| {
            *last == category
                && group.source == source
                && group.alertname == alertname
                && group.severity == severity
        });
        if !same {
            let total = count(row, 5)?;
            let summary: String = row.get(7)?;
            groups.push((
                category,
                Group {
                    source,
                    alertname,
                    severity,
                    rank: row.get(4)?,
                    count: total,
                    new: count(row, 6)?,
                    contexts: Vec::with_capacity(SAMPLES as usize),
                    summary: (total == 1).then_some(summary),
                },
            ));
        }
        if let (Some(context), Some((_, group))) = (context, groups.last_mut())
            && !group.contexts.contains(&context)
        {
            group.contexts.push(context);
        }
    }
    Ok(groups)
}

/// Heartbeat gaps of configured sources, clipped to the window.
///
/// The state at the window start is the `previous` state of the first
/// transition at or after it, or the current state if nothing changed since;
/// so retention of older transitions never hides an ongoing gap.
fn gaps(
    transaction: &Transaction<'_>,
    config: &Config,
    start: i64,
    end: i64,
) -> Result<Vec<Gap>, StoreError> {
    let mut gaps = Vec::new();
    let mut transitions: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
    {
        let mut statement = transaction.prepare_cached(
            "SELECT source, state, at FROM coverage_history
             WHERE at >= ?1 AND at < ?2 ORDER BY at, id LIMIT ?3",
        )?;
        let mut rows = statement.query(params![start, end, MAX_TRANSITIONS])?;
        while let Some(row) = rows.next()? {
            transitions
                .entry(row.get(0)?)
                .or_default()
                .push((row.get(1)?, row.get(2)?));
        }
    }
    for (source, settings) in &config.sources {
        if settings.heartbeat.is_none() {
            continue;
        }
        let first_after: Option<String> = transaction
            .prepare_cached(
                "SELECT previous FROM coverage_history WHERE at >= ?1 AND source = ?2
                 ORDER BY at, id LIMIT 1",
            )?
            .query_row(params![start, source], |row| row.get(0))
            .optional()?;
        let mut state = match first_after {
            Some(previous) => previous,
            None => transaction
                .query_row(
                    "SELECT coverage FROM source_status WHERE source = ?1",
                    [source],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or_else(|| "unknown".to_owned()),
        };
        let mut since = start;
        for (next, at) in transitions.remove(source).unwrap_or_default() {
            if next != state {
                if is_gap(&state) {
                    gaps.push(Gap {
                        source: source.clone(),
                        state: state.clone(),
                        from: since,
                        until: Some(at),
                    });
                }
                state = next;
                since = at;
            }
        }
        if is_gap(&state) {
            gaps.push(Gap {
                source: source.clone(),
                state,
                from: since,
                until: None,
            });
        }
    }
    Ok(gaps)
}

fn is_gap(state: &str) -> bool {
    matches!(state, "stale" | "never_seen")
}

//! The daily summary: reporting windows and a bounded rendering of one window.
//!
//! A summary is built from committed history by the store and delivered through
//! the outbox like any other quiet post. This module is pure: it decides where
//! windows end and how a report reads, never what is stored.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::render::escape;

/// One day in milliseconds.
pub(crate) const DAY_MS: i64 = 86_400_000;

/// Upper bound on a rendered summary, in characters.
///
/// It fits one Discord message even when the destination's largest mention
/// prefix is reserved, so a summary is never split or rejected for length.
pub(crate) const MAX_CHARS: usize = 1500;

/// Groups listed per section; the store reads no more than this many.
pub(crate) const MAX_GROUP_LINES: usize = 12;
/// Closed heartbeat gaps listed per source before they are summarized.
pub(crate) const MAX_GAP_LINES: usize = 3;
/// Coverage lines for sources and truncation shown before the rest is folded.
const MAX_COVERAGE_LINES: usize = 5;
/// A report generated this long after its window ended is marked delayed.
const DELAYED_AFTER_MS: i64 = 15 * 60_000;

/// When reporting windows end: every day at a fixed UTC time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Schedule {
    offset_ms: i64,
}

impl Schedule {
    /// A schedule ending each window `at` after 00:00 UTC.
    pub(crate) fn new(at: Duration) -> Self {
        Self {
            offset_ms: i64::try_from(at.as_millis()).unwrap_or(0) % DAY_MS,
        }
    }

    /// The latest window end at or before `now`.
    ///
    /// ```text
    /// at 06:00, now 05:59 on day 2 -> 06:00 on day 1
    /// at 06:00, now 06:00 on day 2 -> 06:00 on day 2
    /// ```
    pub(crate) fn latest(self, now: i64) -> i64 {
        (now - self.offset_ms).div_euclid(DAY_MS) * DAY_MS + self.offset_ms
    }
}

/// `HH:MM` for a time of day.
pub(crate) fn time_of_day(at: Duration) -> String {
    let minutes = at.as_secs() / 60;
    format!("{:02}:{:02}", minutes / 60 % 24, minutes % 60)
}

/// Occurrences sharing a source, alert name and severity within one section.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Group {
    pub source: String,
    pub alertname: String,
    pub severity: Option<String>,
    pub count: u64,
    /// Occurrences that started inside the window.
    pub new: u64,
    /// Up to two distinct example resources.
    pub contexts: Vec<String>,
    /// The summary annotation, shown only for a single occurrence.
    pub summary: Option<String>,
}

/// One section's totals and its most important groups.
///
/// Totals cover every occurrence and group in the section; `groups` holds at
/// most [`MAX_GROUP_LINES`], already in display order. The rest is folded into
/// one counted line, so totals always equal what the lines account for.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct Section {
    pub total: u64,
    pub new: u64,
    pub group_count: u64,
    pub groups: Vec<Group>,
}

/// Heartbeat coverage of one source inside the window.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct Coverage {
    pub source: String,
    /// The earliest closed gaps, as start and end, at most [`MAX_GAP_LINES`].
    /// A gap without a start had no recorded heartbeat before its end at all.
    pub gaps: Vec<(Option<i64>, i64)>,
    /// Closed gaps counted; if `capped`, there are more than this.
    pub gap_count: u64,
    pub capped: bool,
    /// Time without heartbeats inside the window across the counted gaps.
    pub gap_ms: i64,
    /// A gap still open at the window end.
    pub ongoing: Option<Ongoing>,
}

/// A heartbeat gap open at the window end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Ongoing {
    /// `None` when no heartbeat was ever recorded and monitoring's start is unknown.
    pub since: Option<i64>,
    /// Whether the gap is known to follow a received heartbeat. Otherwise the
    /// report says only that no heartbeat was recorded since.
    pub recorded: bool,
}

/// Everything one summary reports.
#[derive(Debug, Default)]
pub(crate) struct Report {
    pub start: i64,
    pub end: i64,
    pub generated_at: i64,
    /// Whether to name sources; unnecessary with a single configured source.
    pub show_source: bool,
    /// Firing at the window end, including occurrences resolved later.
    pub open: Section,
    /// Resolved inside the window. Superseded occurrences are not resolutions.
    pub resolved: Section,
    /// Sources with a heartbeat gap in the window; healthy sources are omitted.
    pub coverage: Vec<Coverage>,
    /// Alerts Alertmanager reported as omitted, per source.
    pub truncated: Vec<(String, u64)>,
    /// Live notifications that ended failed inside the window.
    pub failed: u64,
    /// Live notifications that ended ambiguous inside the window.
    pub uncertain: u64,
}

impl Report {
    /// Renders the report within [`MAX_CHARS`], folding the least important
    /// group lines into a counted "more" line until it fits.
    pub(crate) fn render(&self) -> String {
        let mut text = String::new();
        for limit in (0..=MAX_GROUP_LINES).rev() {
            text = self.render_with(limit);
            if text.chars().count() <= MAX_CHARS {
                return text;
            }
        }
        // Unreachable with the fixed line caps; the bound holds regardless.
        text.chars()
            .take(MAX_CHARS - 3)
            .chain("...".chars())
            .collect()
    }

    fn render_with(&self, group_lines: usize) -> String {
        let mut lines = vec![self.title()];
        section(
            &mut lines,
            "Open",
            &self.open,
            group_lines,
            true,
            self.show_source,
        );
        section(
            &mut lines,
            "Resolved",
            &self.resolved,
            group_lines,
            false,
            self.show_source,
        );
        let coverage = self.coverage_lines();
        if coverage.is_empty() {
            lines.push("**Coverage gaps:** none".to_owned());
        } else {
            lines.push("**Coverage gaps**".to_owned());
            lines.extend(coverage);
        }
        lines.join("\n")
    }

    fn title(&self) -> String {
        let mut title = format!(
            "**Summary** {} to {} UTC",
            timestamp(self.start, "%Y-%m-%d %H:%M"),
            timestamp(self.end, "%Y-%m-%d %H:%M")
        );
        if self.generated_at - self.end > DELAYED_AFTER_MS {
            title.push_str(&format!(
                " (delayed, generated {})",
                timestamp(self.generated_at, "%m-%d %H:%M")
            ));
        }
        title
    }

    fn coverage_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for coverage in self.coverage.iter().take(MAX_COVERAGE_LINES) {
            let source = &coverage.source;
            if coverage.capped || coverage.gap_count > MAX_GAP_LINES as u64 {
                let at_least = if coverage.capped { "at least " } else { "" };
                let first = coverage
                    .gaps
                    .first()
                    .and_then(|(from, _)| *from)
                    .map_or_else(String::new, |from| {
                        format!(", first {}", timestamp(from, "%m-%d %H:%M"))
                    });
                lines.push(format!(
                    "- {source}: {at_least}{} heartbeat gaps, {at_least}{} without \
                     heartbeats{first}",
                    coverage.gap_count,
                    minutes(coverage.gap_ms)
                ));
            } else {
                lines.extend(coverage.gaps.iter().map(|(from, until)| match from {
                    Some(from) => format!(
                        "- {source}: no heartbeat {} to {} ({})",
                        timestamp(*from, "%m-%d %H:%M"),
                        timestamp(*until, "%H:%M"),
                        minutes(until - from)
                    ),
                    None => format!(
                        "- {source}: no heartbeat ever recorded before {}",
                        timestamp(*until, "%m-%d %H:%M")
                    ),
                }));
            }
            if let Some(ongoing) = coverage.ongoing {
                lines.push(match (ongoing.since, ongoing.recorded) {
                    (Some(since), true) => format!(
                        "- {source}: heartbeat stale since {}, ongoing at window end",
                        timestamp(since, "%m-%d %H:%M")
                    ),
                    (Some(since), false) => format!(
                        "- {source}: no heartbeat recorded since {}, ongoing at window end",
                        timestamp(since, "%m-%d %H:%M")
                    ),
                    (None, _) => {
                        format!("- {source}: no heartbeat ever recorded, ongoing at window end")
                    }
                });
            }
        }
        if self.coverage.len() > MAX_COVERAGE_LINES {
            lines.push(format!(
                "- +{} more sources with heartbeat gaps",
                self.coverage.len() - MAX_COVERAGE_LINES
            ));
        }
        lines.extend(
            self.truncated
                .iter()
                .take(MAX_COVERAGE_LINES)
                .map(|(source, count)| {
                    format!("- {source}: {count} alerts omitted by Alertmanager")
                }),
        );
        if self.truncated.len() > MAX_COVERAGE_LINES {
            lines.push(format!(
                "- +{} more sources with omitted alerts",
                self.truncated.len() - MAX_COVERAGE_LINES
            ));
        }
        if self.failed + self.uncertain > 0 {
            lines.push(format!(
                "- notifications: {} failed, {} may not have arrived",
                self.failed, self.uncertain
            ));
        }
        lines
    }
}

fn section(
    lines: &mut Vec<String>,
    title: &str,
    section: &Section,
    limit: usize,
    mark_new: bool,
    show_source: bool,
) {
    lines.push(if mark_new && section.new > 0 {
        format!("**{title}: {}** ({} new)", section.total, section.new)
    } else {
        format!("**{title}: {}**", section.total)
    });
    let shown = &section.groups[..limit.min(section.groups.len())];
    lines.extend(
        shown
            .iter()
            .map(|group| group_line(group, mark_new, show_source)),
    );
    let more_groups = section.group_count.saturating_sub(shown.len() as u64);
    if more_groups > 0 {
        let shown_alerts: u64 = shown.iter().map(|group| group.count).sum();
        lines.push(format!(
            "- +{more_groups} more alert names ({} alerts)",
            section.total.saturating_sub(shown_alerts)
        ));
    }
}

fn group_line(group: &Group, mark_new: bool, show_source: bool) -> String {
    let mut line = String::from("- ");
    if show_source {
        line.push_str(&group.source);
        line.push_str(": ");
    }
    line.push_str(&escape(&group.alertname, 80));
    line.push_str(&format!(
        " [{}]",
        escape(group.severity.as_deref().unwrap_or("unknown"), 24)
    ));
    if group.count > 1 {
        line.push_str(&format!(" x{}", group.count));
    }
    if mark_new && group.new > 0 {
        if group.new == group.count {
            line.push_str(", new");
        } else {
            line.push_str(&format!(", {} new", group.new));
        }
    }
    if !group.contexts.is_empty() {
        let contexts: Vec<String> = group
            .contexts
            .iter()
            .map(|context| escape(context, 48))
            .collect();
        line.push_str(": ");
        line.push_str(&contexts.join(", "));
    }
    if let Some(summary) = group
        .summary
        .as_deref()
        .filter(|summary| !summary.is_empty())
    {
        line.push_str(" - ");
        line.push_str(&escape(summary, 100));
    }
    line
}

fn timestamp(millis: i64, format: &str) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .map_or_else(|| millis.to_string(), |at| at.format(format).to_string())
}

fn minutes(millis: i64) -> String {
    let minutes = u64::try_from(millis).unwrap_or(0).div_ceil(60_000);
    humantime::format_duration(Duration::from_secs(minutes * 60)).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    // 2026-10-13T06:00:00Z
    const SIX: i64 = 1_791_871_200_000;

    fn group(alertname: &str, count: u64, new: u64) -> Group {
        Group {
            source: "apollo".to_owned(),
            alertname: alertname.to_owned(),
            severity: Some("warning".to_owned()),
            count,
            new,
            contexts: vec!["media/sonarr".to_owned()],
            summary: None,
        }
    }

    fn report() -> Report {
        Report {
            start: SIX - DAY_MS,
            end: SIX,
            generated_at: SIX + 1000,
            ..Report::default()
        }
    }

    #[test]
    fn latest_window_end_is_inclusive_at_the_scheduled_time() {
        let schedule = Schedule::new(Duration::from_secs(6 * 3600));
        assert_eq!(
            (
                schedule.latest(SIX - 1),
                schedule.latest(SIX),
                schedule.latest(SIX + DAY_MS - 1)
            ),
            (SIX - DAY_MS, SIX, SIX)
        );
    }

    #[test]
    fn time_of_day_is_zero_padded() {
        assert_eq!(time_of_day(Duration::from_secs(6 * 3600 + 5 * 60)), "06:05");
    }

    #[test]
    fn listed_groups_and_the_folded_line_add_up_to_the_section_totals() {
        let mut report = report();
        report.open = Section {
            total: 4_998,
            new: 40,
            group_count: 4_000,
            groups: (0..MAX_GROUP_LINES)
                .map(|index| group(&format!("Alert{index:02}"), 3, 1))
                .collect(),
        };
        let text = report.render();
        assert!(
            text.contains("**Open: 4998** (40 new)")
                && text.contains("- +3988 more alert names (4962 alerts)"),
            "{text}"
        );
    }

    #[test]
    fn huge_reports_fit_one_message() {
        let mut report = report();
        let long = "N".repeat(500);
        let groups = |count| {
            (0..MAX_GROUP_LINES)
                .map(|index| group(&format!("{long}{index}"), count, 1))
                .collect()
        };
        report.open = Section {
            total: 5000,
            new: 5000,
            group_count: 5000,
            groups: groups(1),
        };
        report.resolved = Section {
            total: 10_000,
            new: 0,
            group_count: 5000,
            groups: groups(2),
        };
        report.coverage = (0..100)
            .map(|index| Coverage {
                source: format!("source{index}"),
                gaps: vec![(Some(SIX - DAY_MS), SIX - DAY_MS + 60_000); MAX_GAP_LINES],
                gap_count: 10_000,
                capped: true,
                gap_ms: 20 * HOUR,
                ongoing: Some(Ongoing {
                    since: Some(SIX - HOUR),
                    recorded: true,
                }),
            })
            .collect();
        report.truncated = (0..50).map(|index| (format!("source{index}"), 7)).collect();
        report.failed = 3;
        let text = report.render();
        assert!(
            text.chars().count() <= MAX_CHARS && text.contains("**Resolved: 10000**"),
            "{} chars:\n{text}",
            text.chars().count()
        );
    }

    #[test]
    fn untrusted_alert_text_cannot_mention_or_link() {
        let mut report = report();
        let mut hostile = group("@everyone", 1, 1);
        hostile.contexts = vec!["<@123456789012345678>".to_owned()];
        hostile.summary = Some("[click](https://example.invalid) @here".to_owned());
        report.open = Section {
            total: 1,
            new: 1,
            group_count: 1,
            groups: vec![hostile],
        };
        let text = report.render();
        assert!(
            !text.contains("@everyone")
                && !text.contains("@here")
                && !text.contains("<@1")
                && !text.contains("]("),
            "{text}"
        );
    }

    #[test]
    fn resolved_occurrences_are_never_counted_as_open() {
        let mut report = report();
        report.resolved = Section {
            total: 2,
            new: 2,
            group_count: 1,
            groups: vec![group("DiskFull", 2, 2)],
        };
        let text = report.render();
        assert!(
            text.contains("**Open: 0**") && text.contains("**Resolved: 2**"),
            "{text}"
        );
    }

    #[test]
    fn a_late_report_says_when_it_was_generated() {
        let mut report = report();
        report.generated_at = SIX + 12 * HOUR;
        assert!(
            report.render().contains("(delayed, generated 10-13 18:00)"),
            "{}",
            report.render()
        );
    }

    #[test]
    fn gap_durations_round_up_to_whole_minutes() {
        let render = |span: i64| {
            let mut report = report();
            report.coverage = vec![Coverage {
                source: "apollo".to_owned(),
                gaps: vec![(Some(SIX - 3 * HOUR), SIX - 3 * HOUR + span)],
                gap_count: 1,
                ..Coverage::default()
            }];
            report.render()
        };
        let (exact, longer) = (render(119 * 60_000), render(119 * 60_000 + 1));
        assert!(
            exact.contains("(1h 59m)") && longer.contains("(2h)"),
            "{exact}\n{longer}"
        );
    }

    #[test]
    fn capped_gap_counts_say_at_least() {
        let mut report = report();
        report.coverage = vec![Coverage {
            source: "apollo".to_owned(),
            gaps: vec![(Some(SIX - 20 * HOUR), SIX - 20 * HOUR + 60_000)],
            gap_count: 10_000,
            capped: true,
            gap_ms: 10 * HOUR,
            ongoing: None,
        }];
        let text = report.render();
        assert!(
            text.contains(
                "- apollo: at least 10000 heartbeat gaps, at least 10h without heartbeats"
            ) && !text.contains("ongoing"),
            "{text}"
        );
    }
}

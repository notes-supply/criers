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

/// Group lines shown per section before the rest is folded into one line.
const MAX_GROUP_LINES: usize = 12;
/// Coverage gap and truncation lines shown before the rest is folded.
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
    /// Highest action rank among the group's occurrences; orders the section.
    pub rank: u8,
    pub count: u64,
    /// Occurrences that started inside the window.
    pub new: u64,
    /// Distinct example resources, newest first.
    pub contexts: Vec<String>,
    /// The summary annotation, shown only for a single occurrence.
    pub summary: Option<String>,
}

/// A period without fresh heartbeats, clipped to the window.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Gap {
    pub source: String,
    /// `stale` or `never_seen`.
    pub state: String,
    pub from: i64,
    /// `None` while the gap was still open at the window end.
    pub until: Option<i64>,
}

/// Everything one summary reports. Section totals are derived from the groups,
/// so a header can never disagree with the lines beneath it.
#[derive(Debug, Default)]
pub(crate) struct Report {
    pub start: i64,
    pub end: i64,
    pub generated_at: i64,
    /// Whether to name sources; unnecessary with a single configured source.
    pub show_source: bool,
    /// Firing at the window end, including occurrences resolved later.
    pub open: Vec<Group>,
    /// Resolved inside the window. Superseded occurrences are not resolutions.
    pub resolved: Vec<Group>,
    pub gaps: Vec<Gap>,
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
        let mut lines: Vec<String> = self
            .gaps
            .iter()
            .take(MAX_COVERAGE_LINES)
            .map(|gap| {
                let what = if gap.state == "never_seen" {
                    "no heartbeat since startup"
                } else {
                    "heartbeat stale"
                };
                let span = match gap.until {
                    Some(until) => format!(
                        "{} to {} ({})",
                        timestamp(gap.from, "%m-%d %H:%M"),
                        timestamp(until, "%H:%M"),
                        minutes(until - gap.from)
                    ),
                    None => format!(
                        "since {}, ongoing at window end",
                        timestamp(gap.from, "%m-%d %H:%M")
                    ),
                };
                format!("- {}: {what} {span}", gap.source)
            })
            .collect();
        if self.gaps.len() > MAX_COVERAGE_LINES {
            lines.push(format!(
                "- +{} more heartbeat gaps",
                self.gaps.len() - MAX_COVERAGE_LINES
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
    groups: &[Group],
    limit: usize,
    mark_new: bool,
    show_source: bool,
) {
    let total: u64 = groups.iter().map(|group| group.count).sum();
    let new: u64 = groups.iter().map(|group| group.new).sum();
    lines.push(if mark_new && new > 0 {
        format!("**{title}: {total}** ({new} new)")
    } else {
        format!("**{title}: {total}**")
    });
    let mut ordered: Vec<&Group> = groups.iter().collect();
    ordered.sort_by(|a, b| {
        (b.rank, b.count).cmp(&(a.rank, a.count)).then_with(|| {
            (&a.alertname, &a.severity, &a.source).cmp(&(&b.alertname, &b.severity, &b.source))
        })
    });
    lines.extend(
        ordered
            .iter()
            .take(limit)
            .map(|group| group_line(group, mark_new, show_source)),
    );
    if ordered.len() > limit {
        let rest = &ordered[limit..];
        let alerts: u64 = rest.iter().map(|group| group.count).sum();
        lines.push(format!(
            "- +{} more alert names ({alerts} alerts)",
            rest.len()
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
            rank: 1,
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
    fn section_totals_match_the_lines_and_the_folded_remainder() {
        let mut report = report();
        report.open = (0..40)
            .map(|index| group(&format!("Alert{index:02}"), 3, 1))
            .collect();
        let text = report.render();
        let shown: u64 = text
            .lines()
            .filter(|line| line.starts_with("- Alert"))
            .map(|line| {
                line.split(" x")
                    .nth(1)
                    .and_then(|rest| rest.split(',').next())
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum();
        let folded: u64 = text
            .lines()
            .find_map(|line| line.strip_prefix("- +"))
            .and_then(|line| line.split('(').nth(1))
            .and_then(|count| count.split(' ').next())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            (text.contains("**Open: 120** (40 new)"), shown + folded),
            (true, 120),
            "{text}"
        );
    }

    #[test]
    fn huge_reports_fit_one_message() {
        let mut report = report();
        let long = "N".repeat(500);
        report.open = (0..5000)
            .map(|index| group(&format!("{long}{index}"), 1, 1))
            .collect();
        report.resolved = (0..5000)
            .map(|index| group(&format!("{long}{index}"), 2, 0))
            .collect();
        report.gaps = (0..100)
            .map(|index| Gap {
                source: "apollo".to_owned(),
                state: "stale".to_owned(),
                from: SIX - DAY_MS + index * 60_000,
                until: Some(SIX - DAY_MS + index * 60_000 + 30_000),
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
        report.open = vec![hostile];
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
        report.resolved = vec![group("DiskFull", 2, 2)];
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
    fn an_ongoing_gap_is_reported_as_ongoing() {
        let mut report = report();
        report.gaps = vec![Gap {
            source: "apollo".to_owned(),
            state: "stale".to_owned(),
            from: SIX - 2 * HOUR,
            until: None,
        }];
        assert!(
            report
                .render()
                .contains("- apollo: heartbeat stale since 10-13 04:00, ongoing at window end"),
            "{}",
            report.render()
        );
    }
}

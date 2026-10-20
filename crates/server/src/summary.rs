//! The daily summary: reporting windows and how one window's report reads.
//!
//! A summary is built from committed history by the store and delivered through
//! the outbox like any other quiet post. This module is pure: it decides where
//! windows end and how a report reads, never what is stored.

use std::time::Duration;

use crate::render::{
    Card, Embed, FIELD_VALUE_LIMIT, Field, Footer, Icons, Tier, Tone, discord_time, duration,
    escape, units,
};

/// One day in milliseconds.
pub(crate) const DAY_MS: i64 = 86_400_000;

/// Groups listed per tier; the store reads no more than this many per tier.
pub(crate) const MAX_GROUP_LINES: usize = 8;
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

/// Occurrences sharing a source, alert name, severity and tier within one section.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Group {
    pub source: String,
    pub alertname: String,
    pub severity: Option<String>,
    pub tier: Tier,
    pub count: u64,
    /// Occurrences that started inside the window.
    pub new: u64,
    /// Up to two distinct example resources.
    pub contexts: Vec<String>,
    /// The summary annotation, shown only for a single occurrence.
    pub summary: Option<String>,
}

/// Occurrences, those that started inside the window, and their groups.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Count {
    pub occurrences: u64,
    pub new: u64,
    pub groups: u64,
}

impl std::ops::Add for Count {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            occurrences: self.occurrences + other.occurrences,
            new: self.new + other.new,
            groups: self.groups + other.groups,
        }
    }
}

/// One section's totals per tier and its most important groups.
///
/// Counts cover every occurrence and group in the section; `groups` holds at
/// most [`MAX_GROUP_LINES`] per tier, in display order. Groups that are not
/// shown are folded into one counted line, so totals always equal what the
/// lines account for.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct Section {
    /// Indexed by [`Tier::index`].
    pub counts: [Count; 3],
    pub groups: Vec<Group>,
}

impl Section {
    pub(crate) fn count(&self, tier: Tier) -> Count {
        self.counts[tier.index()]
    }

    pub(crate) fn total(&self) -> Count {
        self.counts
            .into_iter()
            .fold(Count::default(), |sum, count| sum + count)
    }
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
    /// Renders the report as one embed within Discord's limits.
    ///
    /// The overall state, per-tier totals, new and recovered counts and every
    /// coverage or delivery problem are always present. Group lines are folded
    /// into counted lines until each field and the whole embed fit.
    pub(crate) fn card(&self, icons: &Icons) -> Card {
        let coverage = self.coverage_field(icons);
        let mut card = self.card_with(icons, MAX_GROUP_LINES, coverage.clone());
        for limit in (0..MAX_GROUP_LINES).rev() {
            if card.embed.within_limits() {
                break;
            }
            card = self.card_with(icons, limit, coverage.clone());
        }
        card
    }

    fn card_with(&self, icons: &Icons, group_lines: usize, coverage: Option<Field>) -> Card {
        let open = self.open.total();
        let resolved = self.resolved.total();
        let tone = self.tone();
        let mut fields = Vec::with_capacity(5);
        for tier in Tier::ALL {
            let count = self.open.count(tier);
            if count.occurrences == 0 {
                continue;
            }
            let mut name = format!(
                "{} {} · {} open",
                icons.get(tier.tone()),
                tier.tone().label(),
                count.occurrences
            );
            if count.new > 0 {
                name.push_str(&format!(" · {} new", count.new));
            }
            let groups = self.open.groups.iter().filter(|group| group.tier == tier);
            fields.push(group_field(name, groups, count, group_lines, |group| {
                self.group_line(group, true)
            }));
        }
        if resolved.occurrences > 0 {
            let name = format!(
                "{} {} · {}",
                icons.get(Tone::Recovered),
                Tone::Recovered.label(),
                resolved.occurrences
            );
            fields.push(group_field(
                name,
                self.resolved.groups.iter(),
                resolved,
                group_lines,
                |group| self.group_line(group, false),
            ));
        }
        fields.extend(coverage);
        let title = if self.end - self.start == DAY_MS {
            "Daily summary"
        } else {
            "Summary"
        };
        Card {
            headline: format!(
                "{title}: {} open, {} recovered",
                open.occurrences, resolved.occurrences
            ),
            embed: Embed {
                title: Some(title.to_owned()),
                description: Some(self.description(icons, tone)),
                color: tone.color(),
                fields,
                footer: self.healthy_coverage().then(|| Footer {
                    text: "No heartbeat gaps".to_owned(),
                }),
                timestamp: chrono::DateTime::from_timestamp_millis(self.end)
                    .map(|end| end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            },
        }
    }

    /// The most pressing state: open critical alerts, then a coverage or
    /// delivery problem, then open warnings or info, else all clear.
    fn tone(&self) -> Tone {
        if self.open.count(Tier::Critical).occurrences > 0 {
            Tone::Critical
        } else if !self.healthy_coverage() {
            Tone::Coverage
        } else if self.open.count(Tier::Warning).occurrences > 0 {
            Tone::Warning
        } else if self.open.count(Tier::Info).occurrences > 0 {
            Tone::Info
        } else {
            Tone::Recovered
        }
    }

    fn healthy_coverage(&self) -> bool {
        self.coverage.is_empty() && self.truncated.is_empty() && self.failed + self.uncertain == 0
    }

    fn description(&self, icons: &Icons, tone: Tone) -> String {
        let open = self.open.total();
        let resolved = self.resolved.total();
        let mut status = if open.occurrences > 0 {
            String::new()
        } else if self.healthy_coverage() {
            "**All clear**".to_owned()
        } else {
            "**No open alerts**".to_owned()
        };
        if open.occurrences > 0 {
            let tiers: Vec<String> = Tier::ALL
                .into_iter()
                .map(|tier| (tier, self.open.count(tier).occurrences))
                .filter(|(_, occurrences)| *occurrences > 0)
                .map(|(tier, occurrences)| {
                    format!("{occurrences} {}", tier.tone().label().to_lowercase())
                })
                .collect();
            status = format!("**{} open** ({})", open.occurrences, tiers.join(", "));
        }
        if open.new > 0 {
            status.push_str(&format!(" · {} new", open.new));
        }
        if resolved.occurrences > 0 {
            status.push_str(&format!(" · {} recovered", resolved.occurrences));
        }
        if !self.coverage.is_empty() {
            status.push_str(" · **coverage gaps**");
        }
        let end_style = if self.end - self.start <= DAY_MS {
            't'
        } else {
            'f'
        };
        let mut description = format!(
            "{} {status}\n{} – {}",
            icons.get(tone),
            discord_time(self.start, 'f'),
            discord_time(self.end, end_style)
        );
        if self.generated_at - self.end > DELAYED_AFTER_MS {
            description.push_str(&format!(
                "\nDelayed · generated {}",
                discord_time(self.generated_at, 'R')
            ));
        }
        description
    }

    fn group_line(&self, group: &Group, open: bool) -> String {
        let mut line = String::new();
        if self.show_source {
            line.push_str(&escape(&group.source, 32));
            line.push_str(" · ");
        }
        line.push_str(&format!("**{}**", escape(&group.alertname, 80)));
        if group.count > 1 {
            line.push_str(&format!(" ×{}", group.count));
        }
        let label = group.severity.as_deref().unwrap_or("");
        if !label.is_empty() && !label.eq_ignore_ascii_case(group.tier.tone().label()) {
            line.push_str(&format!(" ({})", escape(label, 24)));
        } else if !open && group.tier != Tier::Warning {
            // Recovered groups share one field; warning is the unmarked default.
            line.push_str(&format!(" · {}", group.tier.tone().label().to_lowercase()));
        }
        if open && group.new > 0 {
            if group.new == group.count {
                line.push_str(" · new");
            } else {
                line.push_str(&format!(" · {} new", group.new));
            }
        }
        if !group.contexts.is_empty() {
            let contexts: Vec<String> = group
                .contexts
                .iter()
                .map(|context| escape(context, 48))
                .collect();
            line.push_str(" · ");
            line.push_str(&contexts.join(", "));
        }
        if let Some(summary) = group
            .summary
            .as_deref()
            .filter(|summary| !summary.trim().is_empty())
        {
            line.push_str(" — ");
            line.push_str(&escape(summary, 90));
        }
        line
    }

    /// Coverage, omitted alerts and delivery problems, with as much gap detail
    /// as fits one field; `None` when there are none.
    fn coverage_field(&self, icons: &Icons) -> Option<Field> {
        if self.healthy_coverage() {
            return None;
        }
        let value = [Detail::Gaps, Detail::Sources, Detail::Totals]
            .into_iter()
            .map(|detail| self.coverage_lines(detail).join("\n"))
            .find(|value| units(value) <= FIELD_VALUE_LIMIT)
            .unwrap_or_default();
        Some(Field {
            name: format!("{} Coverage", icons.get(Tone::Coverage)),
            value,
            inline: false,
        })
    }

    fn coverage_lines(&self, detail: Detail) -> Vec<String> {
        let mut lines = Vec::new();
        let shown = MAX_COVERAGE_LINES.min(self.coverage.len());
        match detail {
            Detail::Gaps => {
                for coverage in &self.coverage[..shown] {
                    gap_lines(&mut lines, coverage);
                }
                more_sources(&mut lines, &self.coverage[shown..]);
            }
            Detail::Sources => {
                lines.extend(self.coverage[..shown].iter().map(|coverage| {
                    format!(
                        "{}: {}",
                        escape(&coverage.source, 64),
                        gap_totals(std::slice::from_ref(coverage))
                    )
                }));
                more_sources(&mut lines, &self.coverage[shown..]);
            }
            Detail::Totals => {
                if !self.coverage.is_empty() {
                    lines.push(format!(
                        "Heartbeat gaps on {} sources: {}",
                        self.coverage.len(),
                        gap_totals(&self.coverage)
                    ));
                }
            }
        }
        if detail != Detail::Totals && self.truncated.len() <= MAX_COVERAGE_LINES {
            lines.extend(self.truncated.iter().map(|(source, count)| {
                format!(
                    "{}: {count} alerts omitted by Alertmanager",
                    escape(source, 64)
                )
            }));
        } else if !self.truncated.is_empty() {
            let omitted: u64 = self.truncated.iter().map(|(_, count)| count).sum();
            lines.push(format!(
                "{omitted} alerts omitted by Alertmanager across {} sources",
                self.truncated.len()
            ));
        }
        if self.failed + self.uncertain > 0 {
            lines.push(format!(
                "Notifications: {} failed, {} may not have arrived",
                self.failed, self.uncertain
            ));
        }
        lines
    }
}

/// How much coverage detail a rendering keeps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Detail {
    /// Each gap, per source.
    Gaps,
    /// One counted line per source.
    Sources,
    /// One counted line for all sources.
    Totals,
}

/// A field of group lines that fits [`FIELD_VALUE_LIMIT`]: at most `limit`
/// lines, then one line counting every group and occurrence not listed.
fn group_field<'a>(
    name: String,
    groups: impl Iterator<Item = &'a Group>,
    count: Count,
    limit: usize,
    line: impl Fn(&Group) -> String,
) -> Field {
    let mut value = String::new();
    let (mut shown_groups, mut shown_occurrences) = (0u64, 0u64);
    let more = |groups: u64, occurrences: u64| {
        let (groups, occurrences) = (
            count.groups.saturating_sub(groups),
            count.occurrences.saturating_sub(occurrences),
        );
        (groups > 0).then(|| format!("+{groups} more ({occurrences} alerts)"))
    };
    for group in groups.take(limit) {
        let next = line(group);
        let after = (shown_groups + 1, shown_occurrences + group.count);
        let reserve = more(after.0, after.1).map_or(0, |line| units(&line) + 1);
        let separator = usize::from(!value.is_empty());
        if units(&value) + separator + units(&next) + reserve > FIELD_VALUE_LIMIT {
            break;
        }
        if separator == 1 {
            value.push('\n');
        }
        value.push_str(&next);
        (shown_groups, shown_occurrences) = after;
    }
    if let Some(line) = more(shown_groups, shown_occurrences) {
        if !value.is_empty() {
            value.push('\n');
        }
        value.push_str(&line);
    }
    Field {
        name,
        value,
        inline: false,
    }
}

fn gap_lines(lines: &mut Vec<String>, coverage: &Coverage) {
    let source = escape(&coverage.source, 64);
    if coverage.capped || coverage.gap_count > MAX_GAP_LINES as u64 {
        let at_least = if coverage.capped { "at least " } else { "" };
        let first = coverage
            .gaps
            .first()
            .and_then(|(from, _)| *from)
            .map_or_else(String::new, |from| {
                format!(", first {}", discord_time(from, 'f'))
            });
        lines.push(format!(
            "{source}: {at_least}{} heartbeat gaps, {at_least}{} without heartbeats{first}",
            coverage.gap_count,
            minutes(coverage.gap_ms)
        ));
    } else {
        lines.extend(coverage.gaps.iter().map(|(from, until)| match from {
            Some(from) => format!(
                "{source}: no heartbeat {} – {} ({})",
                discord_time(*from, 'f'),
                discord_time(*until, 't'),
                minutes(until - from)
            ),
            None => format!(
                "{source}: no heartbeat ever recorded before {}",
                discord_time(*until, 'f')
            ),
        }));
    }
    if let Some(ongoing) = coverage.ongoing {
        lines.push(match (ongoing.since, ongoing.recorded) {
            (Some(since), true) => format!(
                "{source}: heartbeat stale since {}, ongoing at window end",
                discord_time(since, 'f')
            ),
            (Some(since), false) => format!(
                "{source}: no heartbeat recorded since {}, ongoing at window end",
                discord_time(since, 'f')
            ),
            (None, _) => format!("{source}: no heartbeat ever recorded, ongoing at window end"),
        });
    }
}

fn more_sources(lines: &mut Vec<String>, rest: &[Coverage]) {
    if !rest.is_empty() {
        lines.push(format!(
            "+{} more sources with heartbeat gaps: {}",
            rest.len(),
            gap_totals(rest)
        ));
    }
}

/// Closed gaps, time without heartbeats and open gaps across `coverage`.
fn gap_totals(coverage: &[Coverage]) -> String {
    let at_least = if coverage.iter().any(|coverage| coverage.capped) {
        "at least "
    } else {
        ""
    };
    let gaps: u64 = coverage.iter().map(|coverage| coverage.gap_count).sum();
    let gap_ms: i64 = coverage.iter().map(|coverage| coverage.gap_ms).sum();
    let ongoing = coverage
        .iter()
        .filter(|coverage| coverage.ongoing.is_some())
        .count();
    format!(
        "{at_least}{gaps} closed gaps, {at_least}{} without heartbeats, {ongoing} ongoing",
        minutes(gap_ms)
    )
}

fn minutes(millis: i64) -> String {
    duration(Duration::from_millis(u64::try_from(millis).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{EMBEDS_TOTAL_LIMIT, Part};

    const HOUR: i64 = 3_600_000;
    // 2026-10-13T06:00:00Z
    const SIX: i64 = 1_791_871_200_000;

    fn group(alertname: &str, tier: Tier, count: u64, new: u64) -> Group {
        Group {
            source: "apollo".to_owned(),
            alertname: alertname.to_owned(),
            severity: Some(tier.tone().label().to_lowercase()),
            tier,
            count,
            new,
            contexts: vec!["media/sonarr".to_owned()],
            summary: None,
        }
    }

    fn count(occurrences: u64, new: u64, groups: u64) -> Count {
        Count {
            occurrences,
            new,
            groups,
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

    fn text(report: &Report) -> String {
        Part::Card(report.card(&Icons::default())).text()
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
    fn critical_and_warning_issues_are_separate_with_true_totals() {
        let mut report = report();
        report.open = Section {
            counts: [count(9, 2, 3), count(4_989, 38, 3_997), Count::default()],
            groups: (0..3)
                .map(|index| group(&format!("Page{index}"), Tier::Critical, 3, 1))
                .chain(
                    (0..MAX_GROUP_LINES)
                        .map(|index| group(&format!("Warn{index:02}"), Tier::Warning, 3, 1)),
                )
                .collect(),
        };
        let card = report.card(&Icons::default());
        let names: Vec<&str> = card
            .embed
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let text = Part::Card(card.clone()).text();
        assert!(
            card.embed.color == Tone::Critical.color()
                && names
                    == [
                        "\u{1F534} Critical · 9 open · 2 new",
                        "\u{1F7E0} Warning · 4989 open · 38 new"
                    ]
                && text.contains("**4998 open** (9 critical, 4989 warning) · 40 new")
                && text.contains("+3989 more (4965 alerts)"),
            "{text}"
        );
    }

    #[test]
    fn a_quiet_window_is_all_clear_and_says_coverage_is_healthy() {
        let card = report().card(&Icons::default());
        assert_eq!(
            (
                card.embed.color,
                card.embed.description.as_deref(),
                card.embed.fields.len(),
                card.embed.footer.map(|footer| footer.text)
            ),
            (
                Tone::Recovered.color(),
                Some("\u{1F7E2} **All clear**\n<t:1791784800:f> – <t:1791871200:t>"),
                0,
                Some("No heartbeat gaps".to_owned())
            )
        );
    }

    #[test]
    fn a_coverage_gap_outranks_open_warnings_and_is_never_called_healthy() {
        let mut report = report();
        report.open = Section {
            counts: [Count::default(), count(1, 0, 1), Count::default()],
            groups: vec![group("Disk", Tier::Warning, 1, 0)],
        };
        report.coverage = vec![Coverage {
            source: "apollo".to_owned(),
            ongoing: Some(Ongoing {
                since: None,
                recorded: false,
            }),
            ..Coverage::default()
        }];
        let card = report.card(&Icons::default());
        let text = Part::Card(card.clone()).text();
        assert!(
            card.embed.color == Tone::Coverage.color()
                && card.embed.footer.is_none()
                && text.contains("**coverage gaps**")
                && text.contains(
                    "\u{1F50C} Coverage\napollo: no heartbeat ever recorded, ongoing at window end"
                ),
            "{text}"
        );
    }

    #[test]
    fn recoveries_are_green_and_never_counted_as_open() {
        let mut report = report();
        report.resolved = Section {
            counts: [count(2, 2, 1), Count::default(), Count::default()],
            groups: vec![group("DiskFull", Tier::Critical, 2, 2)],
        };
        let text = text(&report);
        assert!(
            text.contains("**All clear** · 2 recovered")
                && text.contains("Recovered · 2\n**DiskFull** ×2 · critical · media/sonarr"),
            "{text}"
        );
    }

    #[test]
    fn huge_reports_fit_one_embed_with_every_total() {
        let mut report = report();
        let long = "\u{1F525}".repeat(500);
        let groups = |tier, count| {
            (0..MAX_GROUP_LINES)
                .map(|index| {
                    let mut group = group(&format!("{long}{index}"), tier, count, 1);
                    group.summary = Some(long.clone());
                    group.contexts = vec![long.clone(), format!("{long}2")];
                    group
                })
                .collect::<Vec<_>>()
        };
        report.show_source = true;
        report.open = Section {
            counts: [
                count(5000, 5000, 5000),
                count(5000, 0, 5000),
                count(5000, 1, 5000),
            ],
            groups: [Tier::Critical, Tier::Warning, Tier::Info]
                .into_iter()
                .flat_map(|tier| groups(tier, 1))
                .collect(),
        };
        report.resolved = Section {
            counts: [count(10_000, 0, 5000), Count::default(), Count::default()],
            groups: groups(Tier::Critical, 2),
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
        let card = report.card(&Icons::default());
        let text = Part::Card(card.clone()).text();
        assert!(
            card.embed.within_limits()
                && card.embed.units() <= EMBEDS_TOTAL_LIMIT
                && text.contains("**15000 open** (5000 critical, 5000 warning, 5000 info)")
                && text.contains("Recovered · 10000")
                && text.contains("350 alerts omitted by Alertmanager across 50 sources")
                && text.contains("Notifications: 3 failed, 0 may not have arrived"),
            "{} units:\n{text}",
            card.embed.units()
        );
    }

    #[test]
    fn untrusted_alert_text_cannot_mention_or_link() {
        let mut report = report();
        let mut hostile = group("@everyone", Tier::Warning, 1, 1);
        hostile.contexts = vec!["<@123456789012345678>".to_owned()];
        hostile.summary = Some("[click](https://example.invalid) @here <t:1:R>".to_owned());
        hostile.severity = Some("`warning`".to_owned());
        report.open = Section {
            counts: [Count::default(), count(1, 1, 1), Count::default()],
            groups: vec![hostile],
        };
        let text = text(&report);
        assert!(
            !text.contains("@everyone")
                && !text.contains("@here")
                && !text.contains("<@1")
                && !text.contains("](")
                && !text.contains("<t:1:R>"),
            "{text}"
        );
    }

    #[test]
    fn a_late_report_says_when_it_was_generated() {
        let mut report = report();
        report.generated_at = SIX + 12 * HOUR;
        let text = text(&report);
        assert!(
            text.contains("\nDelayed · generated <t:1791914400:R>"),
            "{text}"
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
            text(&report)
        };
        let (exact, longer) = (render(119 * 60_000), render(119 * 60_000 + 1));
        assert!(
            exact.contains("(1h 59m)") && longer.contains("(2h)"),
            "{exact}\n{longer}"
        );
    }

    #[test]
    fn dense_coverage_keeps_every_diagnostic_total_within_its_field() {
        let mut report = report();
        report.coverage = (0..5)
            .map(|index| Coverage {
                source: format!("{index}{}", "s".repeat(63)),
                gaps: (0..3)
                    .map(|gap| (Some(SIX - (20 - gap) * HOUR), SIX - (19 - gap) * HOUR))
                    .collect(),
                gap_count: 3,
                capped: false,
                gap_ms: 3 * HOUR,
                ongoing: Some(Ongoing {
                    since: Some(SIX - HOUR),
                    recorded: true,
                }),
            })
            .collect();
        report.truncated = (0..5)
            .map(|index| (format!("{index}{}", "s".repeat(63)), 7))
            .collect();
        report.failed = 2;
        report.uncertain = 1;
        let card = report.card(&Icons::default());
        let coverage = &card.embed.fields[0].value;
        assert!(
            card.embed.within_limits()
                && coverage.contains("5 sources")
                && coverage.contains("35 alerts omitted")
                && coverage.contains("Notifications: 2 failed, 1 may not have arrived"),
            "{coverage}"
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
        let text = text(&report);
        assert!(
            text.contains("apollo: at least 10000 heartbeat gaps, at least 10h without heartbeats")
                && !text.contains("ongoing"),
            "{text}"
        );
    }
}

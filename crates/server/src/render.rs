//! Discord presentation: one embed per notification, under Discord's limits.
//!
//! Alert titles, annotations and labels are untrusted. Text shown with Markdown
//! goes through [`escape`]; footers, which Discord shows verbatim, go through
//! [`plain`]. Cards are rendered when their intent is recorded and stored in the
//! outbox, so a retried or restarted send posts exactly what was decided.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use criers_core::Action;
use serde::{Deserialize, Serialize};

/// Discord's message content limit.
pub(crate) const CONTENT_LIMIT: usize = 2000;
/// Embeds one message may carry.
pub(crate) const MAX_EMBEDS: usize = 10;
/// Discord's limit on the combined text of every embed in one message.
pub(crate) const EMBEDS_TOTAL_LIMIT: usize = 6000;
// Discord's limits on each part of one embed.
pub(crate) const TITLE_LIMIT: usize = 256;
pub(crate) const DESCRIPTION_LIMIT: usize = 4096;
pub(crate) const MAX_FIELDS: usize = 25;
pub(crate) const FIELD_NAME_LIMIT: usize = 256;
pub(crate) const FIELD_VALUE_LIMIT: usize = 1024;
pub(crate) const FOOTER_LIMIT: usize = 2048;
/// Upper bound on a card headline, which leads a mentioning message's content.
pub(crate) const HEADLINE_LIMIT: usize = 160;

/// Text length as Discord limits count it.
///
/// Counting UTF-16 code units never undercounts: it is at least the number of
/// characters, and characters outside the Basic Multilingual Plane count twice.
pub(crate) fn units(text: &str) -> usize {
    text.encode_utf16().count()
}

/// A Discord embed, serialized as Discord's webhook API expects it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub(crate) struct Embed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub color: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<Field>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer: Option<Footer>,
    /// RFC 3339; Discord shows it in each reader's own time zone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Field {
    pub name: String,
    pub value: String,
    pub inline: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Footer {
    pub text: String,
}

impl Embed {
    /// The text Discord counts against [`EMBEDS_TOTAL_LIMIT`].
    pub(crate) fn units(&self) -> usize {
        let field_units: usize = self
            .fields
            .iter()
            .map(|field| units(&field.name) + units(&field.value))
            .sum();
        self.title.as_deref().map_or(0, units)
            + self.description.as_deref().map_or(0, units)
            + field_units
            + self.footer.as_ref().map_or(0, |footer| units(&footer.text))
    }

    /// Whether every per-part limit holds. The aggregate is checked per message.
    pub(crate) fn within_limits(&self) -> bool {
        self.title
            .as_deref()
            .is_none_or(|title| units(title) <= TITLE_LIMIT)
            && self
                .description
                .as_deref()
                .is_none_or(|description| units(description) <= DESCRIPTION_LIMIT)
            && self.fields.len() <= MAX_FIELDS
            && self.fields.iter().all(|field| {
                (1..=FIELD_NAME_LIMIT).contains(&units(&field.name))
                    && (1..=FIELD_VALUE_LIMIT).contains(&units(&field.value))
            })
            && self
                .footer
                .as_ref()
                .is_none_or(|footer| units(&footer.text) <= FOOTER_LIMIT)
            && self.units() <= EMBEDS_TOTAL_LIMIT
    }
}

/// One outbox row's share of a Discord message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Card {
    /// Escaped one-line text that follows the mention when this card pings, so
    /// push notifications say what happened.
    pub headline: String,
    pub embed: Embed,
}

impl Card {
    /// The outbox `content` column value.
    pub(crate) fn stored(&self) -> String {
        serde_json::to_string(self).expect("a card holds only strings and integers")
    }
}

/// A claimed outbox row as it will be sent.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Part {
    Card(Card),
    /// A plain text line written by an earlier release; it is sent unchanged.
    Line(String),
}

impl Part {
    /// Reads a stored `content` value. Earlier releases stored Markdown text,
    /// which never starts with `{`.
    pub(crate) fn parse(content: String) -> Self {
        if content.starts_with('{')
            && let Ok(card) = serde_json::from_str::<Card>(&content)
        {
            return Self::Card(card);
        }
        Self::Line(content)
    }
}

/// How urgently an alert reads, from its severity label.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Tier {
    Critical,
    Warning,
    Info,
}

/// [`Tier::of`] over the `severity` and `highest_action` columns, as 0, 1 or 2
/// for [`Tier::ALL`]. A test keeps the two in agreement.
macro_rules! tier_sql {
    () => {
        "(CASE lower(coalesce(severity, ''))
            WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 WHEN 'info' THEN 2
            ELSE CASE WHEN highest_action >= 3 THEN 0 WHEN highest_action >= 1 THEN 1
                ELSE 2 END END)"
    };
}
pub(crate) use tier_sql;

impl Tier {
    /// Display order, and the index [`tier_sql!`] yields.
    pub(crate) const ALL: [Self; 3] = [Self::Critical, Self::Warning, Self::Info];

    /// The known severity labels name their tier; any other label is placed by
    /// what the policy decided to do with it.
    pub(crate) fn of(severity: Option<&str>, action: Action) -> Self {
        let label = severity.unwrap_or("");
        if label.eq_ignore_ascii_case("critical") {
            Self::Critical
        } else if label.eq_ignore_ascii_case("warning") {
            Self::Warning
        } else if label.eq_ignore_ascii_case("info") {
            Self::Info
        } else {
            match action {
                Action::Ping => Self::Critical,
                Action::QuietPost | Action::Digest => Self::Warning,
                Action::Record => Self::Info,
            }
        }
    }

    /// The tier [`tier_sql!`] yields as `index`.
    pub(crate) fn from_index(index: i64) -> Self {
        match index {
            0 => Self::Critical,
            1 => Self::Warning,
            _ => Self::Info,
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Critical => 0,
            Self::Warning => 1,
            Self::Info => 2,
        }
    }

    pub(crate) fn tone(self) -> Tone {
        match self {
            Self::Critical => Tone::Critical,
            Self::Warning => Tone::Warning,
            Self::Info => Tone::Info,
        }
    }

    /// Whether a severity label adds nothing to this tier's name.
    fn names(self, severity: Option<&str>) -> bool {
        severity.is_none_or(|label| label.eq_ignore_ascii_case(self.tone().label()))
    }
}

/// The visual meaning of a message part: its colour, icon and label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Tone {
    Critical,
    Warning,
    Info,
    Recovered,
    Coverage,
}

impl Tone {
    pub(crate) fn color(self) -> u32 {
        match self {
            Self::Critical => 0x00E5_484D,
            Self::Warning => 0x00F5_A524,
            Self::Info => 0x003E_8ED0,
            Self::Recovered => 0x0030_A46C,
            Self::Coverage => 0x008E_4EC6,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Critical => "Critical",
            Self::Warning => "Warning",
            Self::Info => "Info",
            Self::Recovered => "Recovered",
            Self::Coverage => "Coverage",
        }
    }

    /// The configuration key and suggested custom emoji name suffix.
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Warning => "warning",
            Self::Info => "info",
            Self::Recovered => "recovered",
            Self::Coverage => "coverage",
        }
    }

    /// Shown when the destination has no custom emoji for this tone.
    fn unicode(self) -> &'static str {
        match self {
            Self::Critical => "\u{1F534}",
            Self::Warning => "\u{1F7E0}",
            Self::Info => "\u{1F535}",
            Self::Recovered => "\u{1F7E2}",
            Self::Coverage => "\u{1F50C}",
        }
    }
}

/// A destination's custom emoji per tone, as `<:name:id>` or `<a:name:id>`
/// markup. Tones without one use a Unicode emoji, so messages read well in any
/// server without configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Icons {
    custom: BTreeMap<&'static str, String>,
}

impl Icons {
    /// Icons from `(tone key, markup)` pairs that configuration already validated.
    pub(crate) fn new(custom: impl IntoIterator<Item = (Tone, String)>) -> Self {
        Self {
            custom: custom
                .into_iter()
                .map(|(tone, markup)| (tone.key(), markup))
                .collect(),
        }
    }

    pub(crate) fn get(&self, tone: Tone) -> &str {
        self.custom
            .get(tone.key())
            .map_or(tone.unicode(), String::as_str)
    }
}

/// Whether `text` is Discord custom emoji markup, `<:name:id>` or `<a:name:id>`.
pub(crate) fn is_custom_emoji(text: &str) -> bool {
    let Some(inner) = text
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
    else {
        return false;
    };
    let inner = inner.strip_prefix('a').unwrap_or(inner);
    let mut parts = inner.split(':');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(""), Some(name), Some(id), None)
            if (2..=32).contains(&name.len())
                && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                && (17..=20).contains(&id.len())
                && id.bytes().all(|byte| byte.is_ascii_digit())
    )
}

/// The subject of an occurrence notification.
pub(crate) struct Subject<'a> {
    pub source: &'a str,
    pub fingerprint: &'a str,
    pub alertname: &'a str,
    pub severity: Option<&'a str>,
    pub summary: &'a str,
    pub labels: &'a BTreeMap<String, String>,
    pub started_at: DateTime<Utc>,
}

impl Subject<'_> {
    fn title(&self) -> String {
        let text = if self.summary.trim().is_empty() {
            self.alertname
        } else {
            self.summary
        };
        escape(text, 200)
    }

    /// `Critical`, or `Critical (error)` when the label is not the tier's name.
    fn severity(&self, tier: Tier) -> String {
        match self.severity {
            Some(label) if !tier.names(Some(label)) => {
                format!("{} ({})", tier.tone().label(), escape(label, 24))
            }
            _ => tier.tone().label().to_owned(),
        }
    }

    /// `icon **label** · detail · ... · resource`.
    fn status_line(&self, icons: &Icons, tone: Tone, label: &str, details: &[String]) -> String {
        let mut line = format!("{} **{label}**", icons.get(tone));
        for detail in details {
            line.push_str(" · ");
            line.push_str(detail);
        }
        if let Some(resource) = resource(self.labels) {
            line.push_str(" · ");
            line.push_str(&escape(&resource, 80));
        }
        line
    }

    fn embed(&self, tone: Tone, description: String, at: DateTime<Utc>) -> Embed {
        Embed {
            title: Some(self.title()),
            description: Some(description),
            color: tone.color(),
            fields: Vec::new(),
            footer: Some(Footer {
                text: format!(
                    "{} · {} · {}",
                    plain(self.source, 40),
                    plain(self.alertname, 120),
                    plain(self.fingerprint, 32)
                ),
            }),
            timestamp: Some(rfc3339(at)),
        }
    }
}

/// A firing announcement. `action` is what the policy decided to send.
pub(crate) fn firing(subject: &Subject<'_>, action: Action, icons: &Icons) -> Card {
    let tier = Tier::of(subject.severity, action);
    let tone = tier.tone();
    let description =
        subject.status_line(icons, tone, &subject.severity(tier), &["firing".to_owned()]);
    Card {
        headline: headline(tone.label(), &subject.title()),
        embed: subject.embed(tone, description, subject.started_at),
    }
}

/// A recovery announcement. `highest` is the most the occurrence was notified at.
pub(crate) fn resolved(
    subject: &Subject<'_>,
    highest: Action,
    ended_at: DateTime<Utc>,
    icons: &Icons,
) -> Card {
    let tier = Tier::of(subject.severity, highest);
    let mut details = vec![subject.severity(tier).to_lowercase()];
    if let Ok(lasted) = (ended_at - subject.started_at).to_std() {
        details.push(format!("lasted {}", duration(lasted)));
    }
    let description =
        subject.status_line(icons, Tone::Recovered, Tone::Recovered.label(), &details);
    Card {
        headline: headline(Tone::Recovered.label(), &subject.title()),
        embed: subject.embed(Tone::Recovered, description, ended_at),
    }
}

/// Coverage transitions are generated from configuration, not alert text.
pub(crate) fn coverage_stale(source: &str, last_heartbeat: i64, now: i64, icons: &Icons) -> Card {
    coverage(
        source,
        (Tone::Coverage, "Coverage lost"),
        format!("No heartbeats from {}", escape(source, 80)),
        format!(
            "{} **Coverage lost** · last heartbeat {}\nAlerts from this source may not be arriving.",
            icons.get(Tone::Coverage),
            discord_time(last_heartbeat, 'R')
        ),
        now,
    )
}

pub(crate) fn coverage_never_seen(source: &str, waited: Duration, now: i64, icons: &Icons) -> Card {
    coverage(
        source,
        (Tone::Coverage, "Coverage unknown"),
        format!("No heartbeat yet from {}", escape(source, 80)),
        format!(
            "{} **Coverage unknown** · none received within {}",
            icons.get(Tone::Coverage),
            duration(waited)
        ),
        now,
    )
}

pub(crate) fn coverage_recovered(source: &str, now: i64, icons: &Icons) -> Card {
    coverage(
        source,
        (Tone::Recovered, "Coverage restored"),
        format!("Heartbeats resumed from {}", escape(source, 80)),
        format!("{} **Coverage restored**", icons.get(Tone::Recovered)),
        now,
    )
}

fn coverage(
    source: &str,
    (tone, label): (Tone, &str),
    title: String,
    description: String,
    now: i64,
) -> Card {
    Card {
        headline: headline(label, &title),
        embed: Embed {
            title: Some(title),
            description: Some(description),
            color: tone.color(),
            fields: Vec::new(),
            footer: Some(Footer {
                text: plain(&format!("{source} · heartbeat"), 120),
            }),
            timestamp: DateTime::from_timestamp_millis(now).map(rfc3339),
        },
    }
}

/// `Label: text`, already escaped, within [`HEADLINE_LIMIT`].
pub(crate) fn headline(label: &str, escaped: &str) -> String {
    let text = format!("{label}: {escaped}");
    if units(&text) <= HEADLINE_LIMIT {
        return text;
    }
    let mut shortened = String::with_capacity(HEADLINE_LIMIT);
    for character in text.chars() {
        if units(&shortened) + character.len_utf16() > HEADLINE_LIMIT - 3 {
            break;
        }
        shortened.push(character);
    }
    // Never end on a lone escaping backslash.
    if shortened
        .chars()
        .rev()
        .take_while(|character| *character == '\\')
        .count()
        % 2
        == 1
    {
        shortened.pop();
    }
    shortened.push_str("...");
    shortened
}

/// The resource an alert is about, in the summary's order of preference: a
/// namespaced workload, else a host, a service, an instance or a job.
pub(crate) fn resource(labels: &BTreeMap<String, String>) -> Option<String> {
    let label = |name: &str| {
        labels
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    };
    let workload = [
        "pod",
        "deployment",
        "statefulset",
        "daemonset",
        "job_name",
        "container",
    ]
    .into_iter()
    .find_map(label);
    let namespaced = [label("namespace"), workload]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("/");
    if !namespaced.is_empty() {
        return Some(namespaced);
    }
    ["host", "service", "instance", "job"]
        .into_iter()
        .find_map(label)
        .map(str::to_owned)
}

/// A Discord timestamp, shown in each reader's own time zone. `style` is one of
/// Discord's format letters, such as `f` (date and time), `t` (time) or `R`
/// (relative).
pub(crate) fn discord_time(millis: i64, style: char) -> String {
    format!("<t:{}:{style}>", millis.div_euclid(1000))
}

/// A compact duration rounded up to whole minutes, such as `1h 5m`.
pub(crate) fn duration(span: Duration) -> String {
    let minutes = u64::try_from(span.as_millis().div_ceil(60_000)).unwrap_or(u64::MAX);
    match (minutes / 1440, minutes / 60 % 24, minutes % 60) {
        (0, 0, minutes) => format!("{minutes}m"),
        (0, hours, 0) => format!("{hours}h"),
        (0, hours, minutes) => format!("{hours}h {minutes}m"),
        (days, 0, _) => format!("{days}d"),
        (days, hours, _) => format!("{days}d {hours}h"),
    }
}

fn rfc3339(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Escapes Markdown and mention syntax, within `limit` UTF-16 units.
pub(crate) fn escape(text: &str, limit: usize) -> String {
    bounded(text, limit, |character, out| match character {
        '\\' | '*' | '_' | '~' | '`' | '|' | '>' | '<' | '#' | '[' | ']' | '(' | ')' | ':' => {
            out.push('\\');
            out.push(character);
        }
        // A zero-width space prevents @everyone, @here and <@id> forms.
        '@' => out.push_str("@\u{200b}"),
        '\n' | '\r' | '\t' => out.push(' '),
        other if other.is_control() => {}
        other => out.push(other),
    })
}

/// Text for places Discord shows verbatim, such as footers: one line without
/// control characters, within `limit` UTF-16 units.
pub(crate) fn plain(text: &str, limit: usize) -> String {
    bounded(text, limit, |character, out| match character {
        '\n' | '\r' | '\t' => out.push(' '),
        other if other.is_control() => {}
        other => out.push(other),
    })
}

/// Writes each character of `text` through `write` until the output would
/// exceed `limit` units, then ends at the last point that leaves room for `...`.
fn bounded(text: &str, limit: usize, write: impl Fn(char, &mut String)) -> String {
    let mut out = String::with_capacity(text.len().min(limit * 2));
    let (mut used, mut fits) = (0, 0);
    for character in text.chars() {
        let start = out.len();
        write(character, &mut out);
        used += units(&out[start..]);
        if used > limit {
            out.truncate(fits);
            out.push_str("...");
            return out;
        }
        if used + 3 <= limit {
            fits = out.len();
        }
    }
    out
}

#[cfg(test)]
impl Part {
    /// What a reader sees, one element per line, for assertions.
    pub(crate) fn text(&self) -> String {
        match self {
            Self::Line(line) => line.clone(),
            Self::Card(card) => {
                let embed = &card.embed;
                let mut lines: Vec<&str> = Vec::new();
                lines.extend(embed.title.as_deref());
                lines.extend(embed.description.as_deref());
                for field in &embed.fields {
                    lines.push(&field.name);
                    lines.push(&field.value);
                }
                lines.extend(embed.footer.as_ref().map(|footer| footer.text.as_str()));
                lines.join("\n")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn subject<'a>(
        labels: &'a BTreeMap<String, String>,
        alertname: &'a str,
        severity: Option<&'a str>,
        summary: &'a str,
    ) -> Subject<'a> {
        Subject {
            source: "example",
            fingerprint: "0123456789abcdef",
            alertname,
            severity,
            summary,
            labels,
            started_at: DateTime::from_timestamp(1_791_871_200, 0).unwrap(),
        }
    }

    /// The Markdown text of a card with escaped characters removed, as Discord
    /// would interpret it. Footers are shown verbatim and checked separately.
    fn interpreted(card: &Card) -> String {
        let embed = &card.embed;
        let mut text = vec![card.headline.as_str()];
        text.extend(embed.title.as_deref());
        text.extend(embed.description.as_deref());
        let text = text.join("\n");
        let mut kept = String::with_capacity(text.len());
        let mut characters = text.chars();
        while let Some(character) = characters.next() {
            if character == '\\' {
                characters.next();
            } else {
                kept.push(character);
            }
        }
        kept
    }

    #[test]
    fn untrusted_text_cannot_mention_link_or_format_anywhere_in_a_card() {
        let labels = labels(&[("namespace", "@everyone"), ("pod", "<@123456789012345678>")]);
        let card = firing(
            &subject(
                &labels,
                "**Disk** <t:1:R>",
                Some("@here"),
                "@everyone <@&123456789012345678> [x](https://example.invalid) `code`\n# Heading",
            ),
            Action::Ping,
            &Icons::default(),
        );
        let text = interpreted(&card);
        assert!(
            !text.contains("@everyone")
                && !text.contains("@here")
                && !text.contains("<@")
                && !text.contains("](")
                && !text.contains("<t:")
                && !text.contains("`")
                && !text.lines().any(|line| line.starts_with('#'))
                && !card.embed.footer.unwrap().text.contains('\n'),
            "{text}"
        );
    }

    #[test]
    fn a_resolved_critical_alert_reads_as_a_recovery_not_another_red_alert() {
        let labels = labels(&[("host", "db-1")]);
        let subject = subject(&labels, "DiskFull", Some("critical"), "Disk almost full");
        let ended = subject.started_at + chrono::Duration::minutes(75);
        let card = resolved(&subject, Action::Ping, ended, &Icons::default());
        assert_eq!(
            (
                card.embed.color,
                card.embed.description.as_deref(),
                card.headline.as_str()
            ),
            (
                Tone::Recovered.color(),
                Some("\u{1F7E2} **Recovered** · critical · lasted 1h 15m · db-1"),
                "Recovered: Disk almost full"
            )
        );
    }

    #[test]
    fn firing_cards_name_their_tier_and_keep_an_unusual_severity_label() {
        let labels = BTreeMap::new();
        let tiers = [
            (
                Some("critical"),
                Action::Ping,
                "**Critical** · firing",
                Tone::Critical,
            ),
            (
                Some("warning"),
                Action::QuietPost,
                "**Warning** · firing",
                Tone::Warning,
            ),
            (
                Some("error"),
                Action::Ping,
                "**Critical (error)** · firing",
                Tone::Critical,
            ),
            (
                None,
                Action::QuietPost,
                "**Warning** · firing",
                Tone::Warning,
            ),
        ];
        for (severity, action, status, tone) in tiers {
            let card = firing(
                &subject(&labels, "Rule", severity, ""),
                action,
                &Icons::default(),
            );
            assert!(
                card.embed
                    .description
                    .as_deref()
                    .is_some_and(|text| text.contains(status))
                    && card.embed.color == tone.color()
                    && card.embed.title.as_deref() == Some("Rule"),
                "{severity:?}: {card:?}"
            );
        }
    }

    #[test]
    fn huge_and_astral_input_stays_within_every_discord_limit() {
        let emoji = "\u{1F525}".repeat(5000);
        let labels = labels(&[("namespace", &emoji), ("pod", &emoji)]);
        let card = firing(
            &subject(&labels, &emoji, Some(&emoji), &emoji),
            Action::Ping,
            &Icons::default(),
        );
        assert!(
            card.embed.within_limits() && units(&card.headline) <= HEADLINE_LIMIT,
            "embed {} units, headline {} units",
            card.embed.units(),
            units(&card.headline)
        );
    }

    #[test]
    fn units_count_astral_characters_twice() {
        assert_eq!(
            (units("abc"), units("\u{e9}"), units("\u{1F534}")),
            (3, 1, 2)
        );
    }

    #[test]
    fn configured_custom_emoji_replace_the_unicode_fallback() {
        let icons = Icons::new([(
            Tone::Critical,
            "<:criers_critical:123456789012345678>".to_owned(),
        )]);
        assert_eq!(
            (icons.get(Tone::Critical), icons.get(Tone::Warning)),
            ("<:criers_critical:123456789012345678>", "\u{1F7E0}")
        );
    }

    #[test]
    fn only_custom_emoji_markup_is_accepted_as_an_icon() {
        let accepted = [
            "<:criers_critical:123456789012345678>",
            "<a:spin:12345678901234567890>",
        ];
        let rejected = [
            "<@123456789012345678>",
            "<:x:123456789012345678>",
            "<:name:1234>",
            ":critical:",
            "\u{1F534}",
            "<:a b:123456789012345678>",
            "<:name:123456789012345678> extra",
        ];
        assert_eq!(
            (
                accepted.iter().all(|text| is_custom_emoji(text)),
                rejected.iter().any(|text| is_custom_emoji(text))
            ),
            (true, false)
        );
    }

    #[test]
    fn stored_cards_round_trip_and_earlier_plain_lines_stay_plain() {
        let labels = BTreeMap::new();
        let card = firing(
            &subject(&labels, "Rule", Some("critical"), "Down"),
            Action::Ping,
            &Icons::default(),
        );
        let legacy = "**FIRING** Rule [critical] - Down (example/0123456789abcdef)".to_owned();
        assert_eq!(
            (
                Part::parse(card.stored()),
                Part::parse(legacy.clone()),
                Part::parse("{not a card".to_owned())
            ),
            (
                Part::Card(card),
                Part::Line(legacy),
                Part::Line("{not a card".to_owned())
            )
        );
    }

    #[test]
    fn coverage_cards_say_what_is_unobserved() {
        let icons = Icons::default();
        let lost = coverage_stale("apollo", 1_791_871_200_000, 1_791_874_800_000, &icons);
        let restored = coverage_recovered("apollo", 1_791_874_800_000, &icons);
        assert_eq!(
            (
                lost.embed.color,
                lost.embed.description.as_deref(),
                restored.embed.color,
                restored.headline.as_str()
            ),
            (
                Tone::Coverage.color(),
                Some(
                    "\u{1F50C} **Coverage lost** · last heartbeat <t:1791871200:R>\n\
                     Alerts from this source may not be arriving."
                ),
                Tone::Recovered.color(),
                "Coverage restored: Heartbeats resumed from apollo"
            )
        );
    }

    #[test]
    fn durations_round_up_to_whole_minutes() {
        let render = |seconds| duration(Duration::from_secs(seconds));
        assert_eq!(
            (
                render(0),
                render(1),
                render(3600),
                render(7140),
                render(7141),
                render(90_000)
            ),
            (
                "0m".to_owned(),
                "1m".to_owned(),
                "1h".to_owned(),
                "1h 59m".to_owned(),
                "2h".to_owned(),
                "1d 1h".to_owned()
            )
        );
    }
}

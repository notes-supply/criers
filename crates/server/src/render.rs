//! Bounded, escaped notification lines. Alert text is untrusted.

use std::time::Duration;

/// Discord's message content limit in characters.
pub(crate) const DISCORD_CONTENT_CHARS: usize = 2000;

/// The subject of an occurrence notification.
pub(crate) struct Subject<'a> {
    pub source: &'a str,
    pub fingerprint: &'a str,
    pub alertname: &'a str,
    pub severity: Option<&'a str>,
    pub summary: &'a str,
}

/// A firing announcement.
pub(crate) fn firing(subject: &Subject<'_>) -> String {
    line("FIRING", subject)
}

/// A recovery announcement.
pub(crate) fn resolved(subject: &Subject<'_>) -> String {
    line("RESOLVED", subject)
}

fn line(state: &str, subject: &Subject<'_>) -> String {
    let mut text = format!(
        "**{state}** {} [{}]",
        escape(subject.alertname, 120),
        escape(subject.severity.unwrap_or("unknown"), 24)
    );
    if !subject.summary.is_empty() {
        text.push_str(" - ");
        text.push_str(&escape(subject.summary, 280));
    }
    text.push_str(&format!(" ({}/{})", subject.source, subject.fingerprint));
    text
}

/// Coverage transitions are generated from configuration, not alert text.
pub(crate) fn coverage_stale(source: &str, age: Duration) -> String {
    format!(
        "**COVERAGE STALE** source {source}: no heartbeat for {}; alert delivery from this \
         source may be broken",
        humantime::format_duration(Duration::from_secs(age.as_secs()))
    )
}

pub(crate) fn coverage_never_seen(source: &str, waited: Duration) -> String {
    format!(
        "**COVERAGE UNKNOWN** source {source}: no heartbeat received within {}",
        humantime::format_duration(Duration::from_secs(waited.as_secs()))
    )
}

pub(crate) fn coverage_recovered(source: &str) -> String {
    format!("**COVERAGE RECOVERED** source {source}: heartbeats resumed")
}

/// Escapes Markdown and mention syntax and truncates to `limit` characters.
fn escape(text: &str, limit: usize) -> String {
    let mut escaped = String::with_capacity(text.len().min(limit * 2));
    for (index, character) in text.chars().enumerate() {
        if index == limit {
            escaped.push_str("...");
            break;
        }
        match character {
            '\\' | '*' | '_' | '~' | '`' | '|' | '>' | '<' | '#' | '[' | ']' | '(' | ')' | ':' => {
                escaped.push('\\');
                escaped.push(character);
            }
            // A zero-width space prevents @everyone, @here and <@id> forms.
            '@' => escaped.push_str("@\u{200b}"),
            '\n' | '\r' | '\t' => escaped.push(' '),
            other if other.is_control() => {}
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untrusted_text_cannot_form_mentions_or_markdown_links() {
        let line = firing(&Subject {
            source: "example",
            fingerprint: "0123456789abcdef",
            alertname: "Disk",
            severity: Some("warning"),
            summary: "@everyone <@123456789012345678> [x](https://example.invalid)",
        });
        assert!(
            !line.contains("@everyone") && !line.contains("<@1") && !line.contains("[x]("),
            "{line}"
        );
    }

    #[test]
    fn long_summary_is_truncated_within_the_line_budget() {
        let summary = "x".repeat(10_000);
        let line = firing(&Subject {
            source: "example",
            fingerprint: "0123456789abcdef",
            alertname: &"a".repeat(1000),
            severity: Some(&"s".repeat(1000)),
            summary: &summary,
        });
        assert!(line.chars().count() < 600, "{}", line.chars().count());
    }
}

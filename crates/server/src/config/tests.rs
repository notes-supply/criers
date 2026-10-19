use std::path::{Path, PathBuf};

use super::{Config, Mode};

const TOKEN: &str = "synthetic-test-token-not-a-live-secret-0123456789";

fn write(directory: &Path, body: &str) -> PathBuf {
    std::fs::create_dir_all(directory.join("data")).unwrap();
    std::fs::write(directory.join("source.token"), TOKEN).unwrap();
    std::fs::write(
        directory.join("ops.url"),
        "https://discord.com/api/webhooks/123456789012345678/synthetic",
    )
    .unwrap();
    let path = directory.join("criers.toml");
    std::fs::write(&path, body).unwrap();
    path
}

const MINIMAL: &str = r#"
[sources.prod]
credential_files = ["source.token"]
default_destination = "ops"

[destinations.ops]
kind = "discord"
webhook_url_file = "ops.url"
"#;

fn error(body: &str) -> String {
    let directory = tempfile::tempdir().unwrap();
    let path = write(directory.path(), body);
    format!("{:#}", Config::load(&path).unwrap_err())
}

#[test]
fn minimal_configuration_defaults_to_shadow_mode() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config::load(&write(directory.path(), MINIMAL)).unwrap();
    assert_eq!(
        (
            config.mode,
            config.destinations["ops"].webhook_url.is_none()
        ),
        (Mode::Shadow, true)
    );
}

#[test]
fn unknown_fields_are_rejected_with_their_name() {
    let message = error(&format!(
        "{MINIMAL}\n[server]\nlisten_adress = \"0.0.0.0:1\"\n"
    ));
    assert!(
        message.contains("unknown field `listen_adress`"),
        "{message}"
    );
}

#[test]
fn malformed_durations_explain_the_expected_format() {
    let message = error(&format!(
        "{MINIMAL}\n[storage]\nretention = \"30 days-ish\"\n"
    ));
    assert!(message.contains("invalid duration"), "{message}");
}

#[test]
fn unknown_destination_references_list_the_known_destinations() {
    let message = error(&MINIMAL.replace(
        "default_destination = \"ops\"",
        "default_destination = \"pager\"",
    ));
    assert!(
        message.contains("\"pager\" is not a configured destination (known: ops)"),
        "{message}"
    );
}

#[test]
fn malformed_credential_file_names_the_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(directory.path(), MINIMAL);
    std::fs::write(directory.path().join("source.token"), "short").unwrap();
    let message = format!("{:#}", Config::load(&path).unwrap_err());
    assert!(
        message.contains("source.token") && message.contains("32 to 256"),
        "{message}"
    );
}

#[test]
fn sources_cannot_share_a_credential() {
    let body = format!(
        "{MINIMAL}\n[sources.copy]\ncredential_files = [\"source.token\"]\ndefault_destination = \"ops\"\n"
    );
    let message = error(&body);
    assert!(message.contains("share a credential"), "{message}");
}

#[test]
fn live_mode_rejects_plain_http_to_a_remote_host() {
    let directory = tempfile::tempdir().unwrap();
    let path = write(directory.path(), &format!("mode = \"live\"\n{MINIMAL}"));
    std::fs::write(
        directory.path().join("ops.url"),
        "http://discord.example/api/webhooks/123456789012345678/x",
    )
    .unwrap();
    let message = format!("{:#}", Config::load(&path).unwrap_err());
    assert!(message.contains("must use https"), "{message}");
}

#[test]
fn mention_ids_must_be_snowflakes() {
    let message = error(&format!("{MINIMAL}mention_users = [\"@everyone\"]\n"));
    assert!(message.contains("not a Discord snowflake"), "{message}");
}

#[test]
fn critical_floor_cannot_be_silent() {
    let message = error(&format!(
        "{MINIMAL}\n[policy]\ncritical_floor = [\"critical\"]\n[policy.severity]\ncritical = \"record\"\n"
    ));
    assert!(
        message.contains("must map to quiet_post or ping"),
        "{message}"
    );
}

#[test]
fn ops_and_ingestion_listeners_must_differ() {
    let message = error(&format!(
        "{MINIMAL}\n[server]\nlisten = \"127.0.0.1:9000\"\nops_listen = \"127.0.0.1:9000\"\n"
    ));
    assert!(message.contains("must differ"), "{message}");
}

#[test]
fn shipped_example_configuration_is_valid() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    for path in ["examples", "data", "secrets"] {
        std::fs::create_dir(root.join(path)).unwrap();
    }
    std::fs::write(root.join("secrets/alertmanager-token"), TOKEN).unwrap();
    let config = root.join("examples/criers.toml");
    std::fs::write(&config, include_str!("../../../../examples/criers.toml")).unwrap();
    let loaded = Config::load(&config);
    assert!(loaded.is_ok(), "{:#}", loaded.unwrap_err());
}

#[test]
fn kubernetes_configuration_is_valid_with_mounted_paths() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::create_dir(root.join("data")).unwrap();
    std::fs::create_dir(root.join("secrets")).unwrap();
    std::fs::write(root.join("secrets/alertmanager-token"), TOKEN).unwrap();
    let text = include_str!("../../../../deploy/kubernetes/criers.toml")
        .replace(
            "/run/secrets/criers/",
            &format!("{}/secrets/", root.display()),
        )
        .replace("/var/lib/criers/", &format!("{}/data/", root.display()));
    let config = root.join("criers.toml");
    std::fs::write(&config, text).unwrap();
    let loaded = Config::load(&config);
    assert!(loaded.is_ok(), "{:#}", loaded.unwrap_err());
}

#[test]
fn kubernetes_placeholder_secret_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("token");
    std::fs::write(&path, "replace with 32 to 256 random printable characters").unwrap();
    assert!(crate::credentials::read_token(&path).is_err());
}

#[test]
fn live_delivery_with_assessment_is_allowed_with_an_explicit_warning() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("clef.token"),
        "synthetic-clef-token-not-live",
    )
    .unwrap();
    let body = format!(
        "mode = \"live\"\n{MINIMAL}\n[assessment]\naccount_id = \"0123456789abcdef0123456789abcdef\"\n\
         api_token_file = \"clef.token\"\n"
    );
    let config = Config::load(&write(directory.path(), &body)).unwrap();
    assert!(
        config.assessment.is_some()
            && config
                .warnings
                .iter()
                .any(|warning| warning.contains("sent to Cloudflare")),
        "{:?}",
        config.warnings
    );
}

#[test]
fn wildcard_and_specific_listeners_on_one_port_are_rejected() {
    let message = error(&format!(
        "{MINIMAL}\n[server]\nlisten = \"0.0.0.0:8080\"\nops_listen = \"127.0.0.1:8080\"\n"
    ));
    assert!(message.contains("cannot both bind"), "{message}");
}

#[test]
fn ipv6_wildcard_and_specific_listeners_on_one_port_are_rejected() {
    let message = error(&format!(
        "{MINIMAL}\n[server]\nlisten = \"[::1]:8080\"\nops_listen = \"[::]:8080\"\n"
    ));
    assert!(message.contains("cannot both bind"), "{message}");
}

#[test]
fn ephemeral_port_zero_listeners_are_accepted() {
    let directory = tempfile::tempdir().unwrap();
    let body =
        format!("{MINIMAL}\n[server]\nlisten = \"127.0.0.1:0\"\nops_listen = \"127.0.0.1:0\"\n");
    let loaded = Config::load(&write(directory.path(), &body));
    assert!(loaded.is_ok(), "{:#}", loaded.unwrap_err());
}

#[test]
fn distinct_specific_addresses_may_share_a_port() {
    let directory = tempfile::tempdir().unwrap();
    let body = format!(
        "{MINIMAL}\n[server]\nlisten = \"127.0.0.1:8080\"\nops_listen = \"127.0.0.2:8080\"\n"
    );
    let loaded = Config::load(&write(directory.path(), &body));
    assert!(loaded.is_ok(), "{:#}", loaded.unwrap_err());
}

/// The `config: |` block of the Helm chart's default values, unindented.
fn helm_default_config() -> String {
    let values = include_str!("../../../../charts/criers/values.yaml");
    let block = values
        .split_once("\nconfig: |\n")
        .expect("values.yaml has a config block")
        .1;
    block
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("  "))
        .map(|line| line.strip_prefix("  ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn helm_default_configuration_is_valid_with_mounted_paths() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::create_dir(root.join("data")).unwrap();
    std::fs::create_dir(root.join("secrets")).unwrap();
    std::fs::write(root.join("secrets/alertmanager-token"), TOKEN).unwrap();
    let text = helm_default_config()
        .replace(
            "/run/secrets/criers/",
            &format!("{}/secrets/", root.display()),
        )
        .replace("/var/lib/criers/", &format!("{}/data/", root.display()));
    let config = root.join("criers.toml");
    std::fs::write(&config, text).unwrap();
    let loaded = Config::load(&config).unwrap();
    let heartbeat = loaded.sources["alertmanager"].heartbeat.as_ref().unwrap();
    assert_eq!(
        (loaded.mode, heartbeat.alertname.as_str()),
        (Mode::Shadow, "Watchdog")
    );
}

#[test]
fn digest_without_a_summary_is_rejected() {
    let message = error(&format!(
        "{MINIMAL}\n[policy.severity]\ncritical = \"ping\"\nwarning = \"digest\"\n"
    ));
    assert!(message.contains("needs a [summary] section"), "{message}");
}

#[test]
fn summary_defaults_to_six_in_the_morning_utc() {
    let directory = tempfile::tempdir().unwrap();
    let body = format!(
        "{MINIMAL}\n[summary]\ndestination = \"ops\"\n[policy.severity]\ncritical = \"ping\"\n\
         warning = \"digest\"\n"
    );
    let config = Config::load(&write(directory.path(), &body)).unwrap();
    let summary = config.summary.unwrap();
    assert_eq!(
        (summary.destination.as_str(), summary.at.as_secs()),
        ("ops", 6 * 3600)
    );
}

#[test]
fn summary_time_must_be_hours_and_minutes() {
    let message = error(&format!(
        "{MINIMAL}\n[summary]\ndestination = \"ops\"\nat = \"6am\"\n"
    ));
    assert!(message.contains("summary.at \"6am\""), "{message}");
}

#[test]
fn summary_destination_must_exist() {
    let message = error(&format!("{MINIMAL}\n[summary]\ndestination = \"daily\"\n"));
    assert!(
        message.contains("summary.destination \"daily\" is not a configured destination"),
        "{message}"
    );
}

#[test]
fn summary_needs_retention_covering_a_delayed_window() {
    let message = error(&format!(
        "{MINIMAL}\n[summary]\ndestination = \"ops\"\n[storage]\nretention = \"1d\"\n"
    ));
    assert!(message.contains("at least 2d"), "{message}");
}

#[test]
fn coverage_notices_cannot_wait_for_the_summary() {
    let message = error(&format!(
        "{MINIMAL}\n[summary]\ndestination = \"ops\"\n[sources.prod.heartbeat]\n\
         stale_after = \"10m\"\naction = \"digest\"\n"
    ));
    assert!(message.contains("must be quiet_post or ping"), "{message}");
}

#[test]
fn destination_icons_use_configured_custom_emoji_and_unicode_otherwise() {
    let directory = tempfile::tempdir().unwrap();
    let body = format!(
        "{MINIMAL}\n[destinations.ops.icons]\ncritical = \"<:criers_critical:123456789012345678>\"\n"
    );
    let config = Config::load(&write(directory.path(), &body)).unwrap();
    let icons = config.icons("ops");
    let removed = config.icons("removed");
    assert_eq!(
        (
            icons.get(crate::render::Tone::Critical),
            icons.get(crate::render::Tone::Warning),
            removed.get(crate::render::Tone::Critical)
        ),
        (
            "<:criers_critical:123456789012345678>",
            "\u{1F7E0}",
            "\u{1F534}"
        )
    );
}

#[test]
fn destination_icons_must_be_custom_emoji_markup() {
    let message = error(&format!(
        "{MINIMAL}\n[destinations.ops.icons]\nwarning = \"<@123456789012345678>\"\n"
    ));
    assert!(
        message.contains("destinations.ops.icons.warning must be custom emoji markup"),
        "{message}"
    );
}

#[test]
fn unknown_icon_names_are_rejected() {
    let message = error(&format!(
        "{MINIMAL}\n[destinations.ops.icons]\nfiring = \"<:fire:123456789012345678>\"\n"
    ));
    assert!(message.contains("unknown field `firing`"), "{message}");
}

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

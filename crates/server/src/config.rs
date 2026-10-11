//! One validated TOML configuration file plus separately mounted secret files.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use criers_core::{Action, Policy};
use reqwest::Url;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};

use crate::credentials;

const MAX_CONFIG_BYTES: u64 = 1_048_576;
const MAX_SECRET_BYTES: u64 = 4096;

/// Whether accepted intents can leave the process.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Mode {
    /// Record decisions and intents without contacting destinations.
    #[default]
    Shadow,
    /// Deliver live intents through the durable outbox.
    Live,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

/// A humantime duration such as `30s`, `5m` or `30d`.
#[derive(Clone, Copy, Debug)]
struct HumanDuration(Duration);

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        humantime::parse_duration(&text).map(Self).map_err(|error| {
            serde::de::Error::custom(format!(
                "invalid duration {text:?} ({error}); use forms like \"30s\", \"5m\", \"30d\""
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    mode: Mode,
    #[serde(default)]
    server: RawServer,
    #[serde(default)]
    storage: RawStorage,
    #[serde(default)]
    delivery: RawDelivery,
    #[serde(default)]
    policy: RawPolicy,
    sources: BTreeMap<String, RawSource>,
    #[serde(default)]
    destinations: BTreeMap<String, RawDestination>,
    assessment: Option<RawAssessment>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawServer {
    listen: SocketAddr,
    ops_listen: SocketAddr,
    max_body_bytes: usize,
    ingest_concurrency: usize,
    admission_wait: HumanDuration,
    request_timeout: HumanDuration,
    shutdown_grace: HumanDuration,
    credential_reload_interval: HumanDuration,
}

impl Default for RawServer {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            ops_listen: SocketAddr::from(([127, 0, 0, 1], 9090)),
            max_body_bytes: 1_048_576,
            ingest_concurrency: 8,
            admission_wait: HumanDuration(Duration::from_secs(1)),
            request_timeout: HumanDuration(Duration::from_secs(30)),
            shutdown_grace: HumanDuration(Duration::from_secs(10)),
            credential_reload_interval: HumanDuration(Duration::from_secs(15)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawStorage {
    path: PathBuf,
    max_size_mib: u32,
    retention: HumanDuration,
    retention_interval: HumanDuration,
    retention_batch: u32,
    health_interval: HumanDuration,
}

impl Default for RawStorage {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data/criers.sqlite"),
            max_size_mib: 1024,
            retention: HumanDuration(Duration::from_secs(30 * 86_400)),
            retention_interval: HumanDuration(Duration::from_secs(60)),
            retention_batch: 500,
            health_interval: HumanDuration(Duration::from_secs(10)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawDelivery {
    timeout: HumanDuration,
    max_attempts: u32,
    backoff_base: HumanDuration,
    backoff_max: HumanDuration,
    max_rate_limit_wait: HumanDuration,
    batch_max: u32,
}

impl Default for RawDelivery {
    fn default() -> Self {
        Self {
            timeout: HumanDuration(Duration::from_secs(10)),
            max_attempts: 12,
            backoff_base: HumanDuration(Duration::from_secs(1)),
            backoff_max: HumanDuration(Duration::from_secs(300)),
            max_rate_limit_wait: HumanDuration(Duration::from_secs(900)),
            batch_max: 10,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawPolicy {
    severity: BTreeMap<String, Action>,
    unknown_severity: Action,
    critical_floor: BTreeSet<String>,
}

impl Default for RawPolicy {
    fn default() -> Self {
        Self {
            severity: BTreeMap::from([
                ("critical".to_owned(), Action::Ping),
                ("warning".to_owned(), Action::QuietPost),
                ("info".to_owned(), Action::Record),
            ]),
            unknown_severity: Action::Ping,
            critical_floor: BTreeSet::from(["critical".to_owned()]),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    credential_files: Vec<PathBuf>,
    default_destination: String,
    #[serde(default)]
    receivers: BTreeMap<String, String>,
    #[serde(default = "default_true")]
    trust_severity: bool,
    heartbeat: Option<RawHeartbeat>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHeartbeat {
    #[serde(default = "default_heartbeat_alertname")]
    alertname: String,
    stale_after: HumanDuration,
    destination: Option<String>,
    #[serde(default = "default_coverage_action")]
    action: Action,
}

fn default_heartbeat_alertname() -> String {
    "Watchdog".to_owned()
}

fn default_coverage_action() -> Action {
    Action::QuietPost
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum RawDestination {
    Discord {
        webhook_url_file: PathBuf,
        #[serde(default)]
        mention_users: Vec<String>,
        #[serde(default)]
        mention_roles: Vec<String>,
        #[serde(default = "default_mention_cooldown")]
        mention_cooldown: HumanDuration,
        username: Option<String>,
    },
}

fn default_mention_cooldown() -> HumanDuration {
    HumanDuration(Duration::from_secs(60))
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum AssessmentMode {
    #[default]
    Shadow,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum ClefModel {
    Clef,
    ClefFlash,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAssessment {
    #[serde(default)]
    mode: AssessmentMode,
    account_id: String,
    api_token_file: PathBuf,
    #[serde(default = "default_clef_model")]
    model: ClefModel,
    #[serde(default = "default_clef_base_url")]
    base_url: String,
    #[serde(default = "default_assessment_timeout")]
    timeout: HumanDuration,
    #[serde(default = "default_assessment_concurrency")]
    concurrency: usize,
    #[serde(default = "default_assessment_pending")]
    max_pending: u32,
    #[serde(default = "default_assessment_attempts")]
    max_attempts: u32,
    #[serde(default = "default_assessment_labels")]
    labels: BTreeSet<String>,
    #[serde(default = "default_assessment_annotations")]
    annotations: BTreeSet<String>,
    #[serde(default = "default_assessment_value_chars")]
    max_value_chars: usize,
    #[serde(default = "default_assessment_response_bytes")]
    max_response_bytes: usize,
}

fn default_clef_model() -> ClefModel {
    ClefModel::ClefFlash
}
fn default_clef_base_url() -> String {
    "https://api.cloudflare.com/client/v4".to_owned()
}
fn default_assessment_timeout() -> HumanDuration {
    HumanDuration(Duration::from_secs(5))
}
fn default_assessment_concurrency() -> usize {
    2
}
fn default_assessment_pending() -> u32 {
    1000
}
fn default_assessment_attempts() -> u32 {
    2
}
fn default_assessment_labels() -> BTreeSet<String> {
    ["alertname", "job", "namespace", "service", "severity"]
        .map(str::to_owned)
        .into()
}
fn default_assessment_annotations() -> BTreeSet<String> {
    BTreeSet::from(["summary".to_owned()])
}
fn default_assessment_value_chars() -> usize {
    256
}
fn default_assessment_response_bytes() -> usize {
    65_536
}

/// Listener, admission and lifecycle settings.
#[derive(Debug)]
pub(crate) struct Server {
    pub listen: SocketAddr,
    pub ops_listen: SocketAddr,
    pub max_body_bytes: usize,
    pub ingest_concurrency: usize,
    pub admission_wait: Duration,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub credential_reload_interval: Duration,
}

/// SQLite location, size cap and retention.
#[derive(Debug)]
pub(crate) struct Storage {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub retention: Duration,
    pub retention_interval: Duration,
    pub retention_batch: u32,
    pub health_interval: Duration,
}

/// Outbox retry and batching settings.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Delivery {
    pub timeout: Duration,
    pub max_attempts: u32,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub max_rate_limit_wait: Duration,
    pub batch_max: u32,
}

/// Source freshness tracking through an always-firing heartbeat alert.
#[derive(Debug)]
pub(crate) struct Heartbeat {
    pub alertname: String,
    pub stale_after: Duration,
    pub destination: String,
    pub action: Action,
}

/// A named Alertmanager source identity.
#[derive(Debug)]
pub(crate) struct Source {
    pub credential_files: Vec<PathBuf>,
    pub default_destination: String,
    pub receivers: BTreeMap<String, String>,
    pub trust_severity: bool,
    pub heartbeat: Option<Heartbeat>,
}

impl Source {
    /// The destination for an Alertmanager receiver; labels never choose it.
    pub(crate) fn destination_for(&self, receiver: &str) -> &str {
        self.receivers
            .get(receiver)
            .unwrap_or(&self.default_destination)
    }
}

/// A Discord webhook destination with explicit mention recipients.
pub(crate) struct Destination {
    /// Absent in shadow mode, where the secret is never read.
    pub webhook_url: Option<SecretString>,
    pub mention_users: Vec<String>,
    pub mention_roles: Vec<String>,
    pub mention_cooldown: Duration,
    pub username: Option<String>,
}

impl fmt::Debug for Destination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Destination")
            .field(
                "webhook_url",
                &self.webhook_url.as_ref().map(|_| "[redacted]"),
            )
            .field("mention_users", &self.mention_users)
            .field("mention_roles", &self.mention_roles)
            .field("mention_cooldown", &self.mention_cooldown)
            .finish_non_exhaustive()
    }
}

/// Shadow-mode Cloudflare Clef assessment settings.
pub(crate) struct Assessment {
    pub url: Url,
    pub api_token: SecretString,
    pub model: &'static str,
    pub timeout: Duration,
    pub concurrency: usize,
    pub max_pending: u32,
    pub max_attempts: u32,
    pub labels: BTreeSet<String>,
    pub annotations: BTreeSet<String>,
    pub max_value_chars: usize,
    pub max_response_bytes: usize,
}

impl fmt::Debug for Assessment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Assessment")
            .field("url", &self.url.as_str())
            .field("model", &self.model)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

/// The complete validated configuration.
#[derive(Debug)]
pub(crate) struct Config {
    pub mode: Mode,
    pub server: Server,
    pub storage: Storage,
    pub delivery: Delivery,
    pub policy: Policy,
    pub policy_version: String,
    pub sources: BTreeMap<String, Source>,
    pub destinations: BTreeMap<String, Destination>,
    pub assessment: Option<Arc<Assessment>>,
    /// Non-fatal findings shown by `check` and logged at startup.
    pub warnings: Vec<String>,
}

impl Config {
    /// Reads and validates the configuration and every referenced secret.
    ///
    /// # Errors
    /// Returns an actionable message naming the offending key or file.
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = read_bounded(path, MAX_CONFIG_BYTES)
            .with_context(|| format!("cannot read configuration {}", path.display()))?;
        let raw: RawConfig = toml::from_str(&text)
            .with_context(|| format!("invalid configuration {}", path.display()))?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        validate(raw, base)
    }
}

fn validate(raw: RawConfig, base: &Path) -> Result<Config> {
    let mut warnings = Vec::new();
    let server = validate_server(raw.server)?;
    let storage = validate_storage(raw.storage, base)?;
    let delivery = validate_delivery(raw.delivery)?;
    let policy = Policy::new(
        raw.policy.severity,
        raw.policy.unknown_severity,
        raw.policy.critical_floor,
    )
    .context("[policy]")?;
    let policy_version = format!(
        "policy-{}",
        &hex_sha256(policy.canonical().as_bytes())[..12]
    );

    ensure!(
        !raw.destinations.is_empty(),
        "[destinations] must define at least one destination"
    );
    let mut destinations = BTreeMap::new();
    for (id, destination) in raw.destinations {
        validate_id("destinations", &id)?;
        let destination = validate_destination(&id, destination, raw.mode, base)?;
        if policy.can_ping()
            && destination.mention_users.is_empty()
            && destination.mention_roles.is_empty()
        {
            warnings.push(format!(
                "destinations.{id} has no mention_users or mention_roles; ping decisions post \
                 without mentioning anyone"
            ));
        }
        destinations.insert(id, destination);
    }

    ensure!(
        !raw.sources.is_empty(),
        "[sources] must define at least one source"
    );
    let mut sources = BTreeMap::new();
    let mut seen_tokens = BTreeMap::new();
    for (id, source) in raw.sources {
        validate_id("sources", &id)?;
        let source = validate_source(&id, source, &destinations, base, &mut seen_tokens)?;
        if !source.trust_severity {
            warnings.push(format!(
                "sources.{id} does not trust severity labels; every firing alert uses \
                 policy.unknown_severity"
            ));
        }
        sources.insert(id, source);
    }

    let assessment = raw
        .assessment
        .map(|assessment| validate_assessment(assessment, base).map(Arc::new))
        .transpose()?;
    if raw.mode == Mode::Live && assessment.is_some() {
        warnings.push(
            "assessment is enabled with live delivery: allowlisted fields of production alerts \
             are sent to Cloudflare; results are recorded and never change notifications"
                .to_owned(),
        );
    }
    Ok(Config {
        mode: raw.mode,
        server,
        storage,
        delivery,
        policy,
        policy_version,
        sources,
        destinations,
        assessment,
        warnings,
    })
}

fn validate_server(raw: RawServer) -> Result<Server> {
    ensure!(
        raw.listen != raw.ops_listen,
        "server.listen and server.ops_listen must differ so probes and metrics stay separate \
         from authenticated ingestion"
    );
    ensure!(
        (4096..=16_777_216).contains(&raw.max_body_bytes),
        "server.max_body_bytes must be between 4096 and 16777216"
    );
    ensure!(
        (1..=64).contains(&raw.ingest_concurrency),
        "server.ingest_concurrency must be between 1 and 64"
    );
    within("server.request_timeout", raw.request_timeout, 1, 300)?;
    within("server.admission_wait", raw.admission_wait, 0, 30)?;
    ensure!(
        raw.admission_wait.0 < raw.request_timeout.0,
        "server.admission_wait must be shorter than server.request_timeout"
    );
    within("server.shutdown_grace", raw.shutdown_grace, 1, 300)?;
    within(
        "server.credential_reload_interval",
        raw.credential_reload_interval,
        1,
        3600,
    )?;
    Ok(Server {
        listen: raw.listen,
        ops_listen: raw.ops_listen,
        max_body_bytes: raw.max_body_bytes,
        ingest_concurrency: raw.ingest_concurrency,
        admission_wait: raw.admission_wait.0,
        request_timeout: raw.request_timeout.0,
        shutdown_grace: raw.shutdown_grace.0,
        credential_reload_interval: raw.credential_reload_interval.0,
    })
}

fn validate_storage(raw: RawStorage, base: &Path) -> Result<Storage> {
    ensure!(
        (16..=1_048_576).contains(&raw.max_size_mib),
        "storage.max_size_mib must be between 16 and 1048576"
    );
    within("storage.retention", raw.retention, 60, 10 * 365 * 86_400)?;
    within(
        "storage.retention_interval",
        raw.retention_interval,
        1,
        86_400,
    )?;
    within("storage.health_interval", raw.health_interval, 1, 300)?;
    ensure!(
        (1..=100_000).contains(&raw.retention_batch),
        "storage.retention_batch must be between 1 and 100000"
    );
    let path = resolve(base, &raw.path);
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        ensure!(
            parent.is_dir(),
            "storage.path parent directory {} does not exist; create it (or mount the volume) \
             first",
            parent.display()
        );
    }
    Ok(Storage {
        path,
        max_bytes: u64::from(raw.max_size_mib) * 1_048_576,
        retention: raw.retention.0,
        retention_interval: raw.retention_interval.0,
        retention_batch: raw.retention_batch,
        health_interval: raw.health_interval.0,
    })
}

fn validate_delivery(raw: RawDelivery) -> Result<Delivery> {
    within("delivery.timeout", raw.timeout, 1, 120)?;
    ensure!(
        (1..=100).contains(&raw.max_attempts),
        "delivery.max_attempts must be between 1 and 100"
    );
    ensure!(
        raw.backoff_base.0 >= Duration::from_millis(100) && raw.backoff_base.0 <= raw.backoff_max.0,
        "delivery.backoff_base must be at least 100ms and no larger than delivery.backoff_max"
    );
    ensure!(
        raw.backoff_max.0 <= Duration::from_secs(3600),
        "delivery.backoff_max must be at most 1h"
    );
    within(
        "delivery.max_rate_limit_wait",
        raw.max_rate_limit_wait,
        1,
        86_400,
    )?;
    ensure!(
        (1..=25).contains(&raw.batch_max),
        "delivery.batch_max must be between 1 and 25"
    );
    Ok(Delivery {
        timeout: raw.timeout.0,
        max_attempts: raw.max_attempts,
        backoff_base: raw.backoff_base.0,
        backoff_max: raw.backoff_max.0,
        max_rate_limit_wait: raw.max_rate_limit_wait.0,
        batch_max: raw.batch_max,
    })
}

fn validate_destination(
    id: &str,
    raw: RawDestination,
    mode: Mode,
    base: &Path,
) -> Result<Destination> {
    let RawDestination::Discord {
        webhook_url_file,
        mention_users,
        mention_roles,
        mention_cooldown,
        username,
    } = raw;
    for (field, ids) in [
        ("mention_users", &mention_users),
        ("mention_roles", &mention_roles),
    ] {
        ensure!(
            ids.len() <= 10,
            "destinations.{id}.{field} supports at most 10 IDs"
        );
        if let Some(bad) = ids.iter().find(|value| !is_snowflake(value)) {
            bail!(
                "destinations.{id}.{field} entry {bad:?} is not a Discord snowflake ID (17 to 20 digits)"
            );
        }
        let unique: BTreeSet<&String> = ids.iter().collect();
        ensure!(
            unique.len() == ids.len(),
            "destinations.{id}.{field} contains duplicate IDs"
        );
    }
    within(
        &format!("destinations.{id}.mention_cooldown"),
        mention_cooldown,
        0,
        86_400,
    )?;
    if let Some(name) = &username {
        ensure!(
            (1..=80).contains(&name.chars().count())
                && !name.to_ascii_lowercase().contains("discord"),
            "destinations.{id}.username must be 1 to 80 characters and must not contain \"discord\""
        );
    }
    let webhook_url = match mode {
        Mode::Shadow => None,
        Mode::Live => {
            let path = resolve(base, &webhook_url_file);
            let url = read_secret(&path)
                .with_context(|| format!("destinations.{id}.webhook_url_file"))?;
            validate_webhook_url(&url).with_context(|| {
                format!("destinations.{id}.webhook_url_file {}", path.display())
            })?;
            Some(SecretString::from(url))
        }
    };
    Ok(Destination {
        webhook_url,
        mention_users,
        mention_roles,
        mention_cooldown: mention_cooldown.0,
        username,
    })
}

fn validate_webhook_url(text: &str) -> Result<()> {
    let url = Url::parse(text).context("webhook URL is not a valid URL")?;
    ensure_secure_endpoint(&url)?;
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "webhook URL must not contain a query or fragment; Criers adds wait=true itself"
    );
    let segments: Vec<&str> = url
        .path_segments()
        .map(Iterator::collect)
        .unwrap_or_default();
    ensure!(
        matches!(segments.as_slice(), ["api", "webhooks", id, token] if is_snowflake(id) && !token.is_empty())
            || matches!(segments.as_slice(), ["api", version, "webhooks", id, token]
                if version.starts_with('v') && is_snowflake(id) && !token.is_empty()),
        "webhook URL path must look like /api/webhooks/<id>/<token>"
    );
    Ok(())
}

fn validate_source(
    id: &str,
    raw: RawSource,
    destinations: &BTreeMap<String, Destination>,
    base: &Path,
    seen_tokens: &mut BTreeMap<String, String>,
) -> Result<Source> {
    ensure!(
        (1..=4).contains(&raw.credential_files.len()),
        "sources.{id}.credential_files must list 1 to 4 files (two overlap during rotation)"
    );
    let known = || destinations.keys().cloned().collect::<Vec<_>>().join(", ");
    ensure!(
        destinations.contains_key(&raw.default_destination),
        "sources.{id}.default_destination {:?} is not a configured destination (known: {})",
        raw.default_destination,
        known()
    );
    ensure!(
        raw.receivers.len() <= 64,
        "sources.{id}.receivers supports at most 64 mappings"
    );
    for (receiver, destination) in &raw.receivers {
        ensure!(
            !receiver.is_empty() && receiver.len() <= 256,
            "sources.{id}.receivers keys must be 1 to 256 bytes"
        );
        ensure!(
            destinations.contains_key(destination),
            "sources.{id}.receivers.{receiver:?} names unknown destination {destination:?} \
             (known: {})",
            known()
        );
    }
    let credential_files: Vec<PathBuf> = raw
        .credential_files
        .iter()
        .map(|path| resolve(base, path))
        .collect();
    let unique: BTreeSet<&PathBuf> = credential_files.iter().collect();
    ensure!(
        unique.len() == credential_files.len(),
        "sources.{id}.credential_files contains duplicate paths"
    );
    for path in &credential_files {
        let token = credentials::read_token(path)
            .with_context(|| format!("sources.{id}.credential_files entry {}", path.display()))?;
        let digest = hex_sha256(token.expose_secret().as_bytes());
        if let Some(other) = seen_tokens.insert(digest, id.to_owned())
            && other != id
        {
            bail!(
                "sources.{id} and sources.{other} share a credential; each source needs its own \
                 token so one source cannot impersonate another"
            );
        }
    }
    let heartbeat = raw
        .heartbeat
        .map(|heartbeat| -> Result<Heartbeat> {
            ensure!(
                !heartbeat.alertname.is_empty() && heartbeat.alertname.len() <= 256,
                "sources.{id}.heartbeat.alertname must be 1 to 256 bytes"
            );
            within(
                &format!("sources.{id}.heartbeat.stale_after"),
                heartbeat.stale_after,
                30,
                7 * 86_400,
            )?;
            let destination = heartbeat
                .destination
                .unwrap_or_else(|| raw.default_destination.clone());
            ensure!(
                destinations.contains_key(&destination),
                "sources.{id}.heartbeat.destination {destination:?} is not a configured \
                 destination (known: {})",
                known()
            );
            ensure!(
                heartbeat.action != Action::Record,
                "sources.{id}.heartbeat.action must be quiet_post or ping"
            );
            Ok(Heartbeat {
                alertname: heartbeat.alertname,
                stale_after: heartbeat.stale_after.0,
                destination,
                action: heartbeat.action,
            })
        })
        .transpose()?;
    Ok(Source {
        credential_files,
        default_destination: raw.default_destination,
        receivers: raw.receivers,
        trust_severity: raw.trust_severity,
        heartbeat,
    })
}

fn validate_assessment(raw: RawAssessment, base: &Path) -> Result<Assessment> {
    let AssessmentMode::Shadow = raw.mode;
    ensure!(
        raw.account_id.len() == 32 && raw.account_id.bytes().all(|b| b.is_ascii_hexdigit()),
        "assessment.account_id must be the 32-character hexadecimal Cloudflare account ID"
    );
    within("assessment.timeout", raw.timeout, 1, 60)?;
    ensure!(
        (1..=16).contains(&raw.concurrency),
        "assessment.concurrency must be between 1 and 16"
    );
    ensure!(
        (1..=100_000).contains(&raw.max_pending),
        "assessment.max_pending must be between 1 and 100000"
    );
    ensure!(
        (1..=5).contains(&raw.max_attempts),
        "assessment.max_attempts must be between 1 and 5"
    );
    ensure!(
        raw.labels.len() <= 32 && raw.annotations.len() <= 8,
        "assessment.labels supports at most 32 names and assessment.annotations at most 8"
    );
    ensure!(
        (16..=2048).contains(&raw.max_value_chars),
        "assessment.max_value_chars must be between 16 and 2048"
    );
    ensure!(
        (1024..=1_048_576).contains(&raw.max_response_bytes),
        "assessment.max_response_bytes must be between 1024 and 1048576"
    );
    let model = match raw.model {
        ClefModel::Clef => "clef",
        ClefModel::ClefFlash => "clef-flash",
    };
    let mut url = Url::parse(raw.base_url.trim_end_matches('/'))
        .context("assessment.base_url is not a valid URL")?;
    ensure_secure_endpoint(&url).context("assessment.base_url")?;
    url.path_segments_mut()
        .map_err(|()| anyhow::anyhow!("assessment.base_url cannot be a base URL"))?
        .extend([
            "accounts",
            &raw.account_id,
            "ai",
            "run",
            "@cf",
            "cloudflare",
            model,
        ]);
    let token_path = resolve(base, &raw.api_token_file);
    let api_token = read_secret(&token_path).context("assessment.api_token_file")?;
    ensure!(
        (16..=512).contains(&api_token.len()) && api_token.bytes().all(|b| b.is_ascii_graphic()),
        "assessment.api_token_file {} must contain one 16 to 512 character API token",
        token_path.display()
    );
    Ok(Assessment {
        url,
        api_token: SecretString::from(api_token),
        model,
        timeout: raw.timeout.0,
        concurrency: raw.concurrency,
        max_pending: raw.max_pending,
        max_attempts: raw.max_attempts,
        labels: raw.labels,
        annotations: raw.annotations,
        max_value_chars: raw.max_value_chars,
        max_response_bytes: raw.max_response_bytes,
    })
}

/// Plain HTTP is accepted only for loopback test fixtures.
fn ensure_secure_endpoint(url: &Url) -> Result<()> {
    let loopback = match url.host() {
        Some(url::Host::Domain(domain)) => domain == "localhost",
        Some(url::Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
        Some(url::Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
        None => false,
    };
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && loopback),
        "endpoint must use https (plain http is accepted only for loopback test fixtures)"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "endpoint must not embed user credentials"
    );
    Ok(())
}

fn within(name: &str, value: HumanDuration, min_secs: u64, max_secs: u64) -> Result<()> {
    ensure!(
        value.0 >= Duration::from_secs(min_secs) && value.0 <= Duration::from_secs(max_secs),
        "{name} must be between {} and {}",
        humantime::format_duration(Duration::from_secs(min_secs)),
        humantime::format_duration(Duration::from_secs(max_secs))
    );
    Ok(())
}

fn validate_id(section: &str, id: &str) -> Result<()> {
    let bytes = id.as_bytes();
    ensure!(
        !bytes.is_empty()
            && bytes.len() <= 64
            && bytes[0].is_ascii_alphanumeric()
            && bytes
                .iter()
                .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_'),
        "{section}.{id:?}: IDs must be 1 to 64 lowercase letters, digits, '-' or '_', starting \
         with a letter or digit"
    );
    Ok(())
}

fn is_snowflake(value: &str) -> bool {
    (17..=20).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit())
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            // Writing to a String cannot fail.
            let _ = write!(text, "{byte:02x}");
            text
        })
}

fn read_bounded(path: &Path, limit: u64) -> Result<String> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    let mut text = String::new();
    file.take(limit + 1).read_to_string(&mut text)?;
    ensure!(
        text.len() as u64 <= limit,
        "file is larger than {limit} bytes"
    );
    Ok(text)
}

/// Reads one secret value, trimming surrounding whitespace.
pub(crate) fn read_secret(path: &Path) -> Result<String> {
    let text = read_bounded(path, MAX_SECRET_BYTES)
        .with_context(|| format!("cannot read secret file {}", path.display()))?;
    let value = text.trim();
    ensure!(!value.is_empty(), "secret file {} is empty", path.display());
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests;

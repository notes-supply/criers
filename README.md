# Criers

A Rust alert router for Alertmanager. Criers authenticates webhooks per source,
records every decision in SQLite before acknowledging, and delivers Discord
notifications through a durable outbox. It can also record Cloudflare Clef
impact probabilities, which never change who gets notified.

Alertmanager keeps grouping, silences and inhibition. Criers decides between
recording, a quiet post and a ping, and keeps the delivery history.

## Quickstart

Install [Mise](https://mise.jdx.dev/), then run from the repository root:

```sh
mise trust
mise install --locked rust aqua:nextest-rs/nextest/cargo-nextest
mkdir -p data secrets
head -c 32 /dev/urandom | base64 > secrets/alertmanager-token
mise run dev
```

`mise run dev` starts Criers with [examples/criers.toml](examples/criers.toml)
in shadow mode: decisions are stored, nothing is sent. In another terminal:

```sh
curl -H "Authorization: Bearer $(cat secrets/alertmanager-token)" \
  --data @examples/critical.json \
  http://127.0.0.1:8080/v1/sources/local/alertmanager
curl http://127.0.0.1:9090/metrics
```

Validate a configuration without opening the database or sending anything:

```sh
cargo run --package criers -- check --config examples/criers.toml
```

## Configuration

One TOML file plus separately mounted secret files. The
[example](examples/criers.toml) documents every section: sources and their
credential files, severity policy, Discord destinations, delivery retries,
retention and optional assessment. Set `mode = "live"` to deliver.

[Operating Criers](docs/operations.md) covers the Alertmanager setup, HTTP and
delivery contracts, rotation, retention, metrics and Kubernetes.

## Development

`mise run ci` runs formatting, strict Clippy, nextest, doctests and a build.
`mise run manifests` validates the Kubernetes manifests and `mise run image`
builds the OCI image.

[Architecture](docs/adr/001-workspace.md) · [Delivery](docs/adr/002-durable-outbox.md) ·
[Glossary](GLOSSARY.md) · [MIT license](LICENSE)

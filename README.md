# Criers

A Rust alert router for Alertmanager. It stores alert history and decides
whether an alert needs a ping, a quiet post, or no notification.

Early development. Decisions are recorded locally in shadow mode; Discord
delivery and Cloudflare Clef integration are planned.

## Run

Install [Mise](https://mise.jdx.dev/) and create a private token file containing
32 to 256 random printable ASCII characters. Keep the token outside the checkout.

```sh
mise trust
mise install --locked
mkdir -p data

export CRIERS_SOURCE=local
export CRIERS_TOKEN_FILE=/path/to/private-token
mise run dev
```

Defaults: `127.0.0.1:8080` and `data/alerts.sqlite`. Override with `CRIERS_BIND`
and `CRIERS_DATABASE`.

Send webhooks to `POST /v1/alertmanager` with `Authorization: Bearer <token>`.
A [sample alert](examples/critical.json) is included. Status is available at
`GET /v1/status` with the same credential.

## Development

- `criers-core`: alert types, validation, and notification policy.
- `criers`: HTTP receiver and SQLite storage.

`mise run ci` runs formatting, Clippy, tests, doctests, and a build.

[Architecture](docs/adr/001-workspace.md) · [Glossary](GLOSSARY.md) · [MIT license](LICENSE)

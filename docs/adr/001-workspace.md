# 001: Pure core and a transactional runtime

Status: accepted.

## Decision

Use two Cargo crates with one-way dependencies. Share version, edition, license,
dependency versions, and lints in the workspace root.

```mermaid
flowchart LR
    Producer[Alertmanager] --> HTTP[HTTP admission]
    subgraph Runtime[criers runtime module]
        HTTP --> Store[Transactional store]
        Store --> DB[(SQLite)]
        Workers[Delivery and assessment workers] --> Store
    end
    Workers --> Discord[Discord webhooks]
    Workers --> Clef[Cloudflare Clef]
    HTTP --> Core[criers-core interface]
    Store --> Core
    Core --> Policy[Validation, occurrence keys, policy]
```

The `criers-core` module owns protocol types, validation, canonical occurrence
keys, and deterministic policy. Its interface exports domain types directly.
It has no HTTP, database, async runtime, credentials, or remote dependencies.
Chrono clock access is enabled only by the runtime crate.

The `criers` module owns configuration, authentication, request admission,
SQLite, delivery, assessment and runtime concurrency. Its store interface
exposes ingestion, outbox claims and outcomes, assessment jobs, coverage,
retention and a health probe. SQL stays private. Ingestion keeps the event,
occurrence changes, decisions, intents and assessment jobs in one transaction,
acknowledged only after commit. Occurrence transitions are pure functions in
`criers-core`; the store supplies prior state and applies the result.

## Depth and locality

The store is a deep module: a small interface hides lifecycle and transactional
failure behavior. Readiness SQL belongs there, not in the HTTP adapter. Fault
injection uses an internal test seam; assertions observe HTTP and store results.
This provides leverage without publishing arbitrary connection callbacks.

Keep the core seam concrete. No repository trait, model plugin trait, extra
lifecycle crate, or generic event framework is justified by the current scope.
Discord and Clef are concrete HTTP adapters tested against local mock servers;
add a trait only when a second real destination or provider exists.

## Consequences

- Core policy can be exercised independently without Tokio or SQLite.
- SQLite state and lifecycle changes remain local to one implementation.
- Workspace tests preserve restart deduplication, escalation, stale-event
  handling, authentication, admission, and transactional rollback behavior.
- The runtime is a single process with one SQLite writer. It is not an
  active-active deployment; see [002](002-durable-outbox.md) for delivery.

# Glossary

- **Source**: configured trusted producer identity. Labels cannot select it.
- **Observation**: one incoming firing or resolved alert event.
- **Occurrence**: source, normalized fingerprint, and canonical start timestamp.
  A later start creates a new occurrence, even with the same fingerprint.
- **Severity**: trusted detector label. It is not model-assessed impact.
- **Action**: notification policy proposal: record, quiet post, or ping.
- **Intent**: durable proposal to notify, not proof of remote delivery.
- **Shadow**: records decisions and intents without external sends. Existing
  shadow intents cannot later be promoted into live delivery.
- **Coverage gap**: known missing observations, such as `truncatedAlerts`.
  Absence of traffic is unknown coverage, not a confirmed outage.

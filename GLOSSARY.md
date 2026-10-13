# Glossary

Criers turns Alertmanager notifications into recorded decisions and, when
configured, Discord messages.

## Alerts

- **Source**: configured trusted producer identity, established by its URL and
  credential. Labels cannot select it. HA peers of one Alertmanager share a source.
- **Observation**: one incoming firing or resolved alert event.
- **Occurrence**: source, normalized fingerprint, and canonical start timestamp.
  A later start creates a new occurrence, even with the same fingerprint.
- **Superseded occurrence**: an occurrence replaced by a later start of the same
  fingerprint. Observations of it change nothing.
- **Severity**: trusted detector label. It is not model-assessed impact.
- **Heartbeat**: an always-firing alert whose arrival proves the source's
  pipeline still delivers. It is not stored as an occurrence.
- **Coverage**: whether recent heartbeats show a source is being heard:
  unknown, healthy, stale or never seen.
- **Coverage gap**: known missing observations, such as `truncatedAlerts`.
  Absence of traffic is unknown coverage, not a confirmed outage.

## Decisions

- **Action**: notification policy proposal: record, digest, quiet post, or ping.
- **Digest**: the action that defers an occurrence to the next summary instead
  of notifying now. A later, higher action still notifies at once.
- **Critical floor**: severities that always notify immediately and skip
  assessment.
- **Assessment**: model-estimated impact and urgency probabilities for an
  occurrence. It is recorded separately and never changes an action.
  _Avoid_: score, triage.
- **Intent**: durable proposal to notify, not proof of remote delivery.
- **Recovery**: a message that an occurrence resolved, sent only when its firing
  message was or may have been delivered.
- **Shadow**: records decisions and intents without external sends. Existing
  shadow intents cannot later be promoted into live delivery.

## Summaries

- **Summary**: a scheduled report of one reporting window, posted without
  mentions: what is open, what resolved, and coverage gaps.
- **Reporting window**: the period a summary covers, from the end of the
  previous summary to the latest scheduled time.

## Delivery

- **Destination**: a configured place to post messages, with its own mention
  recipients. Alerts reach it only through source and receiver configuration.
- **Delivery**: the attempts to turn one live intent into a remote message, and
  their outcome: sent, failed, ambiguous or superseded.
- **Ambiguous delivery**: an attempt whose remote effect is unknown; the message
  may exist, so a retry can duplicate it.
- **Receipt**: the remote message identifier returned for a sent delivery.

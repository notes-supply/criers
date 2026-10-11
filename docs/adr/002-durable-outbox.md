# 002: Single-owner SQLite outbox with truthful ambiguity

Status: accepted.

Delivery state lives in the same SQLite database as alert history, and one
worker per destination owns that destination's rows. A row is marked in flight
in its own committed transaction before the HTTP request, and the outcome is
committed afterwards. We chose this over an in-memory queue (lost on restart)
and over a broker or second database (more moving parts for one small daemon).
The cost is one writer: Criers runs as a single replica, fenced by a lock file
and, where the storage supports it, `ReadWriteOncePod`.

Delivery uses durable retries, not an exactly-once guarantee. A send whose
outcome is unknown (a timeout after
the request left, a gateway error, or a crash mid-flight) is recorded as
possibly delivered and retried, and ends as `ambiguous` rather than `failed`
if retries run out. Attempts are bounded, so a terminal failure or ambiguous
outcome is not a delivery guarantee. A duplicate Discord message is preferred
to a missed page.

Recovery messages depend on real delivery, not on a recorded intent. If an
alert resolves before its first send was attempted, that send is cancelled and
no recovery is posted; once a send may have reached someone, the recovery is
queued behind it so it cannot arrive first.

# Engineering with Effect — Relay

Relay is the single cumulative system built throughout the **Engineering with
Effect** course. It accepts events and delivers them to a snapshotted set of
trusted webhook destinations.

The repository intentionally starts without application code. Course chapters
introduce the system honestly: first a small Promise baseline, then typed
boundaries, explicit failures, services and lifetimes, bounded concurrency,
retry policy, flow control, persistence, crash recovery, multiple workers, and
operational reconciliation.

## Repository model

- `main` contains the latest accepted cumulative state.
- `chapter/<chapter-id>-<slug>` branches show how one chapter changes Relay.
- Immutable `course/v1/chapter/<chapter-id>/start` and
  `course/v1/chapter/<chapter-id>/complete` tags become learner checkpoints.
- Bun is used for installation, scripts, tests, and TypeScript commands.

This is a production-shaped teaching system, not a ready-to-deploy public
webhook service. Security, tenancy, SSRF protection, secret rotation, quotas,
retention, compliance, and multi-region operation require additional design.

Relay accompanies the learner-facing Engineering with Effect course.

## M0 checkpoint

The first checkpoint contains an honest Promise sender and a deliberately
small Effect-shaped equivalent. Both use the same pure delivery policy and an
injected native HTTP adapter; the comparison is about execution and failure
contracts, not a Promise strawman.

Run its deterministic evidence with:

```bash
bun install --frozen-lockfile
bun run validate
```

The short [Act 1 design note](docs/act-01-design-note.md) records which pressure
comes from Relay's domain, Promise semantics, or missing application
architecture.

## M3 checkpoint

Relay's local engine owns every dynamic delivery fiber and bounds active
outbound sends with configurable global and per-destination limits. Concurrency
metrics report active attempts without exposing destination credentials. Scope
shutdown interrupts both active sends and work waiting for permits, and waits
for their finalizers before disposal completes.

This remains an in-memory, single-process milestone. It has no durable intake,
retry policy, crash recovery, untrusted-destination defense, or cross-process
capacity guarantee.

## M4 checkpoint

Relay now gives every outbound attempt a timeout and classifies its evidence
before deciding whether to stop or retry. Successful responses and ordinary
client rejections are terminal. Timeouts, transport failures, 408, 425, 429,
and 5xx responses may enter a bounded retry schedule with capped exponential
backoff, full jitter, elapsed-time and attempt limits, and provider
`Retry-After` evidence.

Each logical delivery keeps one generated `DeliveryId` as its
`Idempotency-Key`, and its result carries ordered attempt history plus visible
exhaustion. Retry sleeps do not occupy outbound concurrency permits; each real
network attempt must acquire capacity again.

Timeout and transport ambiguity mean a retry can duplicate a remote effect.
The stable key prevents that duplicate only when the destination validates and
deduplicates it. Relay therefore does not claim exactly-once delivery. This is
still an in-memory, single-process checkpoint: retry state and attempt history
do not survive a crash, and capacity is not coordinated across processes.

## M5 checkpoint

Relay now admits delivery requests through a bounded local Queue consumed by a
scoped Stream. A separate admission permit bounds the complete accepted
population, including work waiting behind active-attempt limits. When no permit
is available, the caller receives a typed `DeliveryOverloaded` failure instead
of creating another waiting fiber.

The earlier global and per-destination attempt limits remain the socket
bulkheads. Load snapshots expose admitted deliveries, active owned deliveries,
queue depth and capacity, active attempts by destination, configured limits,
and cumulative rejections. The queue and permits are process-local: they do not
coordinate a fleet, provide durable intake, or reserve queue space per tenant.
Relay does not add a proactive time-based rate limit without a destination rate
contract.

The M5 act gate keeps one slow destination saturated while a producer repeatedly
offers excess work, releases one delivery, and submits one replacement. The
test proves admitted work never exceeds its configured capacity, active work
never exceeds the per-destination limit, every excess offer is rejected
visibly, and all accepted work can drain without sleeps or wall-clock guesses.

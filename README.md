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

## C05-06 checkpoint

Relay now classifies final HTTP statuses before policy acts: successful and
permanently rejected responses are terminal, while 408, 425, 429, and 5xx
responses remain eligible for a later bounded retry policy. The outbound
adapter disables redirects and sends one generated `DeliveryId` as the same
`Idempotency-Key` for every attempt of a logical delivery.

The key prevents duplicate remote effects only when the destination validates
and deduplicates it. Relay still has no retry loop at this checkpoint, and it
does not claim exactly-once delivery. The M4 incident checkpoint will connect
these outcomes to bounded retry and visible exhaustion.

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

The course repository is
[Zakariaaben/learning-effect](https://github.com/Zakariaaben/learning-effect).

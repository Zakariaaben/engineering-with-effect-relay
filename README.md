# Engineering with Effect: Relay

Relay is the cumulative application built throughout **Engineering with
Effect**. It accepts invoice events, commits delivery intent, and sends webhooks
to a trusted configured destination.

It begins in the course as a small, honest Promise sender. Requirements then
create the pressure for typed boundaries, explicit failures, owned resources,
bounded concurrency, finite retry policy, flow control, persistence, crash
recovery, multiple workers, and operational repair.

**Relay never claims exactly-once remote effects.** A receiver can accept a
request while Relay remains uncertain about the result. The delivery ID stays
stable across attempts so a cooperating receiver can deduplicate, but the
receiver owns that guarantee.

## What the current application guarantees

- Unknown intake is decoded before domain construction.
- Idempotent acceptance commits the event, route snapshot, and delivery intent
  atomically.
- Global and per-destination admission and attempt limits are explicit.
- Retry is classified, bounded by attempts and elapsed time, and driven by an
  injectable clock and random service.
- Claims use leases and monotonically increasing generations. A stale worker
  cannot mutate authoritative database state.
- Attempt history, dead letters, reconciliation, readiness, shutdown, logs,
  traces, and metrics have deterministic evidence in the test suite.

This is a **production-shaped teaching system**, not a ready-to-deploy public
webhook product. Tenancy, arbitrary destination security, SSRF and DNS
rebinding defense, secret rotation, quotas, retention, compliance, and
multi-region operation require additional design.

## Read the code by responsibility

```text
src/
  app/layer.ts                 application and HTTP composition roots
  adapters/
    memoryPersistence.ts      deterministic in-memory adapter
    postgres/                 SQL repository, intake transaction, migrations
  deliveryAdmission.ts        admission and concurrency ownership
  deliveryWorker.ts           one claimed delivery's lease and attempt lifecycle
  deliverySupervisor.ts       queue, fiber ownership, and public delivery facade
  deliveryEngine.ts           retry and attempt policy execution
  eventIntake.ts              validated, idempotent event acceptance
  httpServer.ts               HTTP contracts, handlers, and server policy
  runtime.ts                  Promise-facing embedding boundary
  main.node.ts                Node process entrypoint and signal-aware runtime
```

The dependency direction is deliberate: pure domain decisions feed application
services, adapters implement the external boundaries, `app/layer.ts` assembles
the graph, and process/runtime files stay at the edge.

The delivery state truth table lives in `src/deliveryPolicy.ts`. Memory and SQL
adapters consume the same decision, and `test/delivery-policy.test.ts` guards
their parity.

## Run the evidence

The repository pins Bun, TypeScript, Effect, and official Effect packages in
`package.json` and `bun.lock`.

```bash
bun install --frozen-lockfile
bun run validate
```

Validation type-checks the complete application and runs the deterministic
unit, integration, incident, lifecycle, concurrency, and persistence suites.

## Run the current Node application

The complete application requires PostgreSQL 17 and these secrets or boundary
values:

- `RELAY_DATABASE_URL`
- `RELAY_DESTINATION_URL`
- `RELAY_DESTINATION_AUTHORIZATION`
- `RELAY_INTAKE_AUTHORIZATION`
- `RELAY_OPERATIONS_AUTHORIZATION`

Optional configuration includes `RELAY_HOST`, `RELAY_PORT`, concurrency,
admission, retry, claim, and polling settings. Defaults are decoded and checked
through Effect `Config`; credentials remain redacted.

```bash
bun run start
```

`NodeRuntime.runMain` owns process signals and finalization. The separate
`startRelayApplication` function remains available when a test or another Node
application needs a Promise-facing embedding boundary.

## Operational repair

The authenticated teaching fixture can inspect delivery history, list dead
letters, retry with the accepted route, repair with the current trusted route,
terminate work, and request one bounded reconciliation pass.

The [recovery runbook](ops/recovery-runbook.md), [guarantee evidence](ops/guarantee-evidence.md),
and [dashboard fixture](ops/relay-m9-dashboard.json) connect operator decisions
to the exact authority and evidence behind each claim.

An optional AI specialist adapter can analyze a sanitized delivery summary. It
cannot see payloads, endpoints, credentials, worker identity, fencing tokens,
or trace identifiers, and it has no mutation capability. Model failure falls
back to deterministic runbook guidance.

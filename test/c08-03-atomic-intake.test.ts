import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Effect,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import { IngestionConflictError } from "../src/errors.ts"
import { makeRelayIntakeStoreSql } from "../src/intakeStoreSql.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  ConfigurationVersion,
  ClaimGeneration,
  Delivery,
  DeliveryClaim,
  DeliveryId,
  DeliveryRouteSnapshot,
  DeliveryState,
  EventAcceptance,
  EventId,
  IngestionKey,
  RequestFingerprint,
  WorkerId,
  type DeliveryResult,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  DeliveryRepository,
  IntakeDecision,
  type IntakeRecord,
  RelayIntakeStore,
} from "../src/services.ts"
import {
  destination,
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
  submission,
} from "./fixtures.ts"

interface StoredEvent {
  readonly eventId: string
  readonly eventType: string
  readonly invoiceId: string
  readonly amountCents: number
  readonly ingestionKey: string
  readonly requestFingerprint: string
  readonly acceptedAtMillis: number
}

interface StoredDelivery {
  readonly deliveryId: string
  readonly eventId: string
  readonly destinationId: string
  readonly destinationUrl: string
  readonly configurationVersion: number
}

interface DatabaseState {
  readonly events: Map<string, StoredEvent>
  readonly deliveries: Map<string, StoredDelivery>
}

const copyState = (state: DatabaseState): DatabaseState => ({
  events: new Map(state.events),
  deliveries: new Map(state.deliveries),
})

const makeDatabase = (failDeliveryInsert = false) =>
  Effect.gen(function* () {
    let committed: DatabaseState = {
      events: new Map(),
      deliveries: new Map(),
    }
    let staged: DatabaseState | undefined
    const commands: Array<string> = []

    const execute = (
      statement: string,
      params: ReadonlyArray<unknown>,
    ): Effect.Effect<ReadonlyArray<Record<string, unknown>>, SqlError.SqlError> =>
      Effect.suspend((): Effect.Effect<
        ReadonlyArray<Record<string, unknown>>,
        SqlError.SqlError
      > => {
        const normalized = statement.replace(/\s+/g, " ").trim()
        commands.push(normalized)

        if (normalized === "BEGIN") {
          staged = copyState(committed)
          return Effect.succeed([])
        }
        if (normalized === "COMMIT") {
          if (staged !== undefined) committed = staged
          staged = undefined
          return Effect.succeed([])
        }
        if (normalized === "ROLLBACK") {
          staged = undefined
          return Effect.succeed([])
        }

        const target = staged ?? committed
        if (
          normalized.startsWith("INSERT INTO relay_events") &&
          normalized.includes("ON CONFLICT")
        ) {
          const ingestionKey = String(params[4])
          if (target.events.has(ingestionKey)) {
            return Effect.succeed([])
          }
          const stored: StoredEvent = {
            eventId: String(params[0]),
            eventType: String(params[1]),
            invoiceId: String(params[2]),
            amountCents: Number(params[3]),
            ingestionKey,
            requestFingerprint: String(params[5]),
            acceptedAtMillis: Number(params[6]),
          }
          target.events.set(ingestionKey, stored)
          return Effect.succeed([{ event_id: stored.eventId }])
        }
        if (normalized.startsWith("INSERT INTO deliveries")) {
          if (failDeliveryInsert) {
            return Effect.fail(new SqlError.SqlError({
              reason: new SqlError.UnknownError({
                cause: new Error("forced route insert failure"),
                message: "forced route insert failure",
                operation: "insertDeliveryRoute",
              }),
            }))
          }
          const stored: StoredDelivery = {
            deliveryId: String(params[0]),
            eventId: String(params[1]),
            destinationId: String(params[2]),
            destinationUrl: String(params[5]),
            configurationVersion: Number(params[6]),
          }
          target.deliveries.set(stored.deliveryId, stored)
          return Effect.succeed([{
            claim_owner: String(params[7]),
            claim_generation: 1,
            lease_expires_at_ms: 30_000,
          }])
        }
        if (normalized.startsWith("SELECT relay_events.event_id")) {
          const acceptedEvent = target.events.get(String(params[0]))
          if (acceptedEvent === undefined) return Effect.succeed([])
          const acceptedDelivery = Array.from(
            target.deliveries.values(),
          ).find((delivery) =>
            delivery.eventId === acceptedEvent.eventId
          )
          if (acceptedDelivery === undefined) return Effect.succeed([])
          return Effect.succeed([{
            event_id: acceptedEvent.eventId,
            event_type: acceptedEvent.eventType,
            invoice_id: acceptedEvent.invoiceId,
            amount_cents: acceptedEvent.amountCents,
            request_fingerprint: acceptedEvent.requestFingerprint,
            accepted_at_ms: acceptedEvent.acceptedAtMillis,
            delivery_id: acceptedDelivery.deliveryId,
            destination_id: acceptedDelivery.destinationId,
            destination_url: acceptedDelivery.destinationUrl,
            configuration_version: acceptedDelivery.configurationVersion,
          }])
        }

        return Effect.succeed([])
      })

    const connection = {
      execute: (statement, params) => execute(statement, params),
      executeRaw: (statement, params) => execute(statement, params),
      executeStream: () => Stream.empty,
      executeValues: () => Effect.succeed([]),
      executeValuesUnprepared: () => Effect.succeed([]),
      executeUnprepared: (statement, params) => execute(statement, params),
    } satisfies SqlConnection.Connection

    const sql = yield* SqlClient.make({
      acquirer: Effect.succeed(connection),
      compiler: PgClient.makeCompiler(),
      spanAttributes: [],
      transactionAcquirer: Effect.succeed(connection),
    })

    return {
      sql,
      commands,
      state: () => committed,
    }
  }).pipe(Effect.provide(Reactivity.layer))

const record = (options: {
  readonly eventId: string
  readonly deliveryId: string
  readonly fingerprint?: string
}): IntakeRecord => ({
  ingestionKey: IngestionKey.make("invoice-import:42"),
  requestFingerprint: RequestFingerprint.make(
    options.fingerprint ?? "a".repeat(64),
  ),
  event: {
    ...event,
    id: EventId.make(options.eventId),
  },
  deliveryId: DeliveryId.make(options.deliveryId),
  route: DeliveryRouteSnapshot.make({
    destinationId: destination.id,
    endpoint: new URL("https://old.example.test/invoices"),
    configurationVersion: ConfigurationVersion.make(7),
  }),
  acceptedAtMillis: 1_234,
  claim: {
    ownerId: WorkerId.make("wrk-atomic-intake"),
    leaseDurationMillis: 30_000,
  },
})

const configuration = (endpoint: string) => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "atomic-secret",
  RELAY_DESTINATION_CONFIGURATION_VERSION: 8,
  RELAY_DESTINATION_ID: String(destination.id),
  RELAY_DESTINATION_URL: endpoint,
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
})

describe("C08-03 atomic acceptance and ingestion idempotency", () => {
  it("commits one acceptance, replays it, and rejects conflicting key reuse", async () => {
    const database = await Effect.runPromise(makeDatabase())
    const store = makeRelayIntakeStoreSql(database.sql)
    const firstRecord = record({
      eventId: "evt-atomic-first",
      deliveryId: "dlv-atomic-first",
    })

    const first = await Effect.runPromise(store.accept(firstRecord))
    const replay = await Effect.runPromise(store.accept(record({
      eventId: "evt-atomic-discarded",
      deliveryId: "dlv-atomic-discarded",
    })))
    const conflict = await Effect.runPromise(
      Effect.flip(store.accept(record({
        eventId: "evt-atomic-conflict",
        deliveryId: "dlv-atomic-conflict",
        fingerprint: "b".repeat(64),
      }))),
    )

    expect(IntakeDecision.$is("Accepted")(first)).toBe(true)
    expect(IntakeDecision.$is("Replay")(replay)).toBe(true)
    expect(replay.event.id).toBe(firstRecord.event.id)
    expect(replay.delivery.id).toBe(firstRecord.deliveryId)
    expect(replay.route.endpoint.href).toBe(
      "https://old.example.test/invoices",
    )
    expect(conflict).toBeInstanceOf(IngestionConflictError)
    expect(database.state().events.size).toBe(1)
    expect(database.state().deliveries.size).toBe(1)
  })

  it("rolls back the event when its snapshotted delivery cannot be inserted", async () => {
    const database = await Effect.runPromise(makeDatabase(true))
    const store = makeRelayIntakeStoreSql(database.sql)

    const failure = await Effect.runPromise(
      Effect.flip(store.accept(record({
        eventId: "evt-atomic-rollback",
        deliveryId: "dlv-atomic-rollback",
      }))),
    )

    expect(failure._tag).toBe("RelayIntakeStoreError")
    expect(database.state().events.size).toBe(0)
    expect(database.state().deliveries.size).toBe(0)
    expect(database.commands.at(-1)).toBe("ROLLBACK")
  })

  it("returns the original HTTP acceptance on replay and maps conflict to 409", async () => {
    const sent = makeGate<void>()
    let outboundCalls = 0
    const application = await startRelayApplication({
      configProvider: configuration(
        "https://current.example.test/invoices",
      ),
      httpClientLayer: makeHttpClientLayer((_request) =>
        Effect.sync(() => {
          outboundCalls += 1
          sent.resolve(undefined)
        }).pipe(Effect.andThen(Effect.never))
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })
    const post = (amountCents: number) =>
      fetch(`${application.httpAddress}/events`, {
        method: "POST",
        headers: {
          authorization: "Bearer intake-secret",
          "content-type": "application/json",
          "idempotency-key": "invoice-import:response-loss",
        },
        body: JSON.stringify({
          ...submission,
          payload: { ...submission.payload, amountCents },
        }),
      })

    try {
      const firstResponse = await post(submission.payload.amountCents)
      const first = await Effect.runPromise(
        Schema.decodeUnknownEffect(EventAcceptance)(
          await firstResponse.json(),
        ),
      )
      const replayResponse = await post(submission.payload.amountCents)
      const replay = await Effect.runPromise(
        Schema.decodeUnknownEffect(EventAcceptance)(
          await replayResponse.json(),
        ),
      )
      const conflictResponse = await post(
        submission.payload.amountCents + 1,
      )
      await sent.promise

      expect(firstResponse.status).toBe(202)
      expect(replayResponse.status).toBe(202)
      expect(replay).toEqual({ ...first, replayed: true })
      expect(conflictResponse.status).toBe(409)
      expect(await conflictResponse.json()).toEqual({
        error: "idempotency_conflict",
      })
      expect(outboundCalls).toBe(1)
    } finally {
      await application.shutdown()
    }
  })

  it("uses the committed route snapshot after configuration changes", async () => {
    const route = DeliveryRouteSnapshot.make({
      destinationId: destination.id,
      endpoint: new URL("https://accepted.example.test/invoices"),
      configurationVersion: ConfigurationVersion.make(7),
    })
    const pending = Delivery.make({
      id: DeliveryId.make("dlv-snapshotted-route"),
      eventId: event.id,
      destinationId: destination.id,
      state: DeliveryState.cases.Pending.make({}),
    })
    const claim = DeliveryClaim.make({
      ownerId: WorkerId.make("wrk-route-recovery"),
      generation: ClaimGeneration.make(1),
      leaseExpiresAtMillis: Number.MAX_SAFE_INTEGER,
    })
    let claimed = false
    let terminal: DeliveryResult | undefined
    const sentTo = makeGate<string>()
    const persistence = Layer.merge(
      Layer.succeed(
        DeliveryRepository,
        DeliveryRepository.of({
          save: () => Effect.void,
          findById: () => Effect.succeed(Option.some(pending)),
          findStatus: () => Effect.succeed(Option.none()),
          recordAttempt: () => Effect.void,
          listDeadLetters: () => Effect.succeed([]),
          retryDeadLetter: () => Effect.void,
          claimPending: () =>
            Effect.sync(() => {
              if (claimed || terminal !== undefined) return []
              claimed = true
              return [{
                claim,
                claimLagMillis: 0,
                delivery: pending,
                event,
                nextAttemptOrdinal: 1,
                route: Option.some(route),
              }]
            }),
          renewClaim: (_deliveryId, currentClaim) =>
            Effect.succeed(currentClaim),
          completeClaim: (_deliveryId, _claim, result) =>
            Effect.sync(() => {
              terminal = result
              claimed = false
            }),
          releaseClaim: () => Effect.sync(() => {
            claimed = false
          }),
        }),
      ),
      Layer.succeed(
        RelayIntakeStore,
        RelayIntakeStore.of({
          accept: () => Effect.die(new Error("not used by recovery")),
          savePending: () => Effect.die(new Error("not used by recovery")),
        }),
      ),
    )
    const application = await startRelayApplication({
      configProvider: configuration(
        "https://changed.example.test/invoices",
      ),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          sentTo.resolve(request.url)
          return makeHttpResponse(request)
        })
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: persistence,
      registerShutdownHook: () => () => {},
    })

    try {
      expect(await sentTo.promise).toBe(
        "https://accepted.example.test/invoices",
      )
    } finally {
      await application.shutdown()
    }
  })
})

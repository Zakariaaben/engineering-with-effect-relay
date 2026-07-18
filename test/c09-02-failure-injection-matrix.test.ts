import { NodeCrypto } from "@effect/platform-node"
import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "bun:test"
import {
  Cause,
  ConfigProvider,
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Random,
  Ref,
  Stream,
} from "effect"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDeliveryResilience,
  defaultDestinationConfigurationVersion,
  type DeliveryFlow,
  type DeliveryResilience,
} from "../src/configuration.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  makeDeliverySupervisorLive,
  type DeliverySupervisorHooks,
} from "../src/deliverySupervisor.ts"
import {
  DestinationClient,
  type DestinationClientService,
} from "../src/destinationClient.ts"
import {
  ClaimLostError,
  DeliveryOverloaded,
  InvalidEventError,
} from "../src/errors.ts"
import { makeRelayIntakeStoreSql } from "../src/intakeStoreSql.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  ClaimGeneration,
  ConfigurationVersion,
  DeliveryId,
  DeliveryResult,
  DeliveryRouteSnapshot,
  EventId,
  IngestionKey,
  RequestFingerprint,
  WorkerId,
  type Delivery,
  type DeliveryClaim,
  type DeliveryResult as DeliveryResultValue,
  type EventId as EventIdValue,
  type RelayEvent,
} from "../src/model.ts"
import { RelayReadiness } from "../src/readiness.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  DeliveryRepository,
  IntakeDecision,
  type IntakeRecord,
  RelayIntakeStore,
} from "../src/services.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import {
  delivery,
  destination,
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
  submission,
} from "./fixtures.ts"

type IntakeFault = "BeforeCommit" | "DeliveryInsert"

interface StoredIntakeState {
  readonly deliveries: Map<string, string>
  readonly events: Map<string, string>
}

const copyIntakeState = (
  state: StoredIntakeState,
): StoredIntakeState => ({
  deliveries: new Map(state.deliveries),
  events: new Map(state.events),
})

const sqlFailure = (operation: string) =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      cause: new Error(`injected ${operation} failure`),
      message: `injected ${operation} failure`,
      operation,
    }),
  })

const makeIntakeDatabase = (fault?: IntakeFault) =>
  Effect.gen(function* () {
    let committed: StoredIntakeState = {
      deliveries: new Map(),
      events: new Map(),
    }
    let staged: StoredIntakeState | undefined
    const commands: Array<string> = []

    const execute = (
      statement: string,
      params: ReadonlyArray<unknown>,
    ): Effect.Effect<
      ReadonlyArray<Record<string, unknown>>,
      SqlError.SqlError
    > =>
      Effect.suspend((): Effect.Effect<
        ReadonlyArray<Record<string, unknown>>,
        SqlError.SqlError
      > => {
        const normalized = statement.replace(/\s+/g, " ").trim()
        commands.push(normalized)

        if (normalized === "BEGIN") {
          staged = copyIntakeState(committed)
          return Effect.succeed([])
        }
        if (normalized === "COMMIT") {
          if (fault === "BeforeCommit") {
            staged = undefined
            return Effect.fail(sqlFailure("commit"))
          }
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
          target.events.set(ingestionKey, String(params[0]))
          return Effect.succeed([{ event_id: String(params[0]) }])
        }
        if (normalized.startsWith("INSERT INTO deliveries")) {
          if (fault === "DeliveryInsert") {
            return Effect.fail(sqlFailure("insertDeliveryRoute"))
          }
          const deliveryId = String(params[0])
          target.deliveries.set(deliveryId, String(params[1]))
          return Effect.succeed([{
            claim_owner: String(params[7]),
            claim_generation: 1,
            lease_expires_at_ms: 30_000,
          }])
        }
        if (normalized.startsWith("SELECT relay_events.event_id")) {
          const acceptedEventId = target.events.get(String(params[0]))
          if (acceptedEventId === undefined) return Effect.succeed([])
          const deliveryEntry = [...target.deliveries].find(
            ([, eventId]) => eventId === acceptedEventId,
          )
          if (deliveryEntry === undefined) return Effect.succeed([])
          return Effect.succeed([{
            accepted_at_ms: 1_234,
            amount_cents: event.amountCents,
            configuration_version: 7,
            delivery_id: deliveryEntry[0],
            destination_id: destination.id,
            destination_url: "https://accepted.example.test/invoices",
            event_id: acceptedEventId,
            event_type: "invoice.created",
            invoice_id: event.invoiceId,
            request_fingerprint: "a".repeat(64),
          }])
        }

        return Effect.succeed([])
      })

    const connection = {
      execute: (statement, params) => execute(statement, params),
      executeRaw: (statement, params) => execute(statement, params),
      executeStream: () => Stream.empty,
      executeUnprepared: (statement, params) => execute(statement, params),
      executeValues: () => Effect.succeed([]),
      executeValuesUnprepared: () => Effect.succeed([]),
    } satisfies SqlConnection.Connection

    const sql = yield* SqlClient.make({
      acquirer: Effect.succeed(connection),
      compiler: PgClient.makeCompiler(),
      spanAttributes: [],
      transactionAcquirer: Effect.succeed(connection),
    })

    return {
      commands,
      sql,
      state: () => committed,
    }
  }).pipe(Effect.provide(Reactivity.layer))

const intakeRecord = (suffix: string): IntakeRecord => ({
  acceptedAtMillis: 1_234,
  claim: {
    leaseDurationMillis: 30_000,
    ownerId: WorkerId.make("wrk-c09-02-intake"),
  },
  deliveryId: DeliveryId.make(`dlv-c09-02-${suffix}`),
  event: {
    ...event,
    id: EventId.make(`evt-c09-02-${suffix}`),
  },
  ingestionKey: IngestionKey.make("invoice-import:c09-02"),
  requestFingerprint: RequestFingerprint.make("a".repeat(64)),
  route: DeliveryRouteSnapshot.make({
    configurationVersion: ConfigurationVersion.make(7),
    destinationId: destination.id,
    endpoint: new URL("https://accepted.example.test/invoices"),
  }),
})

const workerA = WorkerId.make("wrk-c09-02-a")
const workerB = WorkerId.make("wrk-c09-02-b")
const leaseDurationMillis = 10_000

const persistenceWithTestClock = Layer.merge(
  RelayPersistenceMemory,
  TestClock.layer(),
)

const runPersistence = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DeliveryRepository | RelayIntakeStore | TestClock.TestClock
  >,
) => Effect.runPromise(program.pipe(Effect.provide(persistenceWithTestClock)))

const saveInitiallyClaimed = Effect.gen(function* () {
  const intake = yield* RelayIntakeStore
  return yield* intake.savePending(
    event,
    delivery.id,
    destination.id,
    {
      leaseDurationMillis,
      ownerId: workerA,
    },
  )
})

const terminalResult = (
  claim: DeliveryClaim,
): DeliveryResultValue => DeliveryResult.Delivered({
  attempts: [],
  deliveryId: delivery.id,
  destinationId: destination.id,
  status: claim.generation > 0 ? 202 : 500,
})

const makeSupervisorRuntime = (options: {
  readonly flow?: Partial<DeliveryFlow>
  readonly hooks?: DeliverySupervisorHooks
  readonly post: DestinationClientService["post"]
  readonly resilience?: Partial<DeliveryResilience>
}) => {
  const persistence = RelayPersistenceMemory
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      AppConfiguration,
      AppConfiguration.of({
        concurrency: { global: 2, perDestination: 1 },
        destination,
        destinationConfigurationVersion:
          defaultDestinationConfigurationVersion,
        flow: { ...defaultDeliveryFlow, ...options.flow },
        recovery: {
          ...defaultDeliveryRecovery,
          claimLeaseDuration: Duration.seconds(30),
          claimRenewInterval: Duration.seconds(10),
        },
        resilience: {
          ...defaultDeliveryResilience,
          attemptTimeout: Duration.seconds(1),
          baseDelay: Duration.seconds(1),
          maxAttempts: 2,
          maxDelay: Duration.seconds(1),
          maxElapsed: Duration.seconds(5),
          ...options.resilience,
        },
      }),
    ),
    Layer.succeed(
      DestinationClient,
      DestinationClient.of({ post: options.post }),
    ),
    NodeCrypto.layer,
    persistence,
    makeWorkerIdentityLayer(workerA),
  )
  const supervisor = makeDeliverySupervisorLive(options.hooks).pipe(
    Layer.provide(DeliveryEventsLive),
    Layer.provide(dependencies),
  )

  return ManagedRuntime.make(
    Layer.mergeAll(supervisor, persistence, TestClock.layer()),
  )
}

const awaitCondition = (
  predicate: () => boolean,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!predicate()) yield* Effect.yieldNow
  })

const runtimeConfiguration = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "matrix-secret",
  RELAY_DESTINATION_CONCURRENCY: 1,
  RELAY_DESTINATION_ID: "dst-matrix",
  RELAY_DESTINATION_URL: "https://hooks.example.test/matrix",
  RELAY_GLOBAL_CONCURRENCY: 1,
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
  RELAY_OPERATIONS_AUTHORIZATION: "operations-secret",
})

interface MatrixRow {
  readonly boundary: string
  readonly claim: string
  readonly inject: () => Promise<void>
}

const matrix: ReadonlyArray<MatrixRow> = [
  {
    boundary: "trust / unknown input",
    claim: "malformed input cannot reach the receiver",
    inject: async () => {
      let outboundCalls = 0
      const runtime = makeSupervisorRuntime({
        post: () => Effect.sync(() => {
          outboundCalls += 1
          return { status: 202 }
        }),
      })
      try {
        const failure = await runtime.runPromise(
          Effect.flatMap(DeliverySupervisor, (supervisor) =>
            supervisor.deliver({ topic: "invoice.created" }).pipe(
              Effect.flip,
            )
          ),
        )
        expect(failure).toBeInstanceOf(InvalidEventError)
        expect(outboundCalls).toBe(0)
      } finally {
        await runtime.dispose()
      }
    },
  },
  {
    boundary: "failure / defect",
    claim: "a defect stays visible as a defect while owned cleanup runs",
    inject: async () => {
      const releases = await Effect.runPromise(Ref.make(0))
      const exit = await Effect.runPromise(
        Effect.acquireUseRelease(
          Effect.void,
          () => Effect.die("injected defect"),
          () => Ref.update(releases, (count) => count + 1),
        ).pipe(Effect.exit),
      )
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.hasDies(exit.cause)).toBe(true)
      }
      expect(await Effect.runPromise(Ref.get(releases))).toBe(1)
    },
  },
  {
    boundary: "before intake transaction commit",
    claim: "a failed commit publishes neither event nor delivery",
    inject: async () => {
      const database = await Effect.runPromise(
        makeIntakeDatabase("BeforeCommit"),
      )
      const store = makeRelayIntakeStoreSql(database.sql)
      const exit = await Effect.runPromise(
        store.accept(intakeRecord("before-commit")).pipe(Effect.exit),
      )
      expect(Exit.isFailure(exit)).toBe(true)
      expect(database.state().events.size).toBe(0)
      expect(database.state().deliveries.size).toBe(0)
    },
  },
  {
    boundary: "after commit but before response",
    claim: "response loss cannot erase committed acceptance",
    inject: async () => {
      const database = await Effect.runPromise(makeIntakeDatabase())
      const store = makeRelayIntakeStoreSql(database.sql)
      const responseLost = await Effect.runPromise(
        store.accept(intakeRecord("committed")).pipe(
          Effect.andThen(Effect.fail("response lost" as const)),
          Effect.flip,
        ),
      )
      expect(responseLost).toBe("response lost")
      expect(database.state().events.size).toBe(1)
      expect(database.state().deliveries.size).toBe(1)
    },
  },
  {
    boundary: "response loss followed by repeated intake",
    claim: "the same key and fingerprint replay one acceptance",
    inject: async () => {
      const database = await Effect.runPromise(makeIntakeDatabase())
      const store = makeRelayIntakeStoreSql(database.sql)
      const first = await Effect.runPromise(
        store.accept(intakeRecord("original")),
      )
      const replay = await Effect.runPromise(
        store.accept(intakeRecord("discarded")),
      )
      expect(IntakeDecision.$is("Accepted")(first)).toBe(true)
      expect(IntakeDecision.$is("Replay")(replay)).toBe(true)
      expect(replay.event.id).toBe(first.event.id)
      expect(replay.delivery.id).toBe(first.delivery.id)
      expect(database.state().events.size).toBe(1)
      expect(database.state().deliveries.size).toBe(1)
    },
  },
  {
    boundary: "partial route write",
    claim: "a delivery insert failure rolls the event back",
    inject: async () => {
      const database = await Effect.runPromise(
        makeIntakeDatabase("DeliveryInsert"),
      )
      const store = makeRelayIntakeStoreSql(database.sql)
      await Effect.runPromise(
        store.accept(intakeRecord("partial-write")).pipe(Effect.flip),
      )
      expect(database.state().events.size).toBe(0)
      expect(database.state().deliveries.size).toBe(0)
      expect(database.commands.at(-1)).toBe("ROLLBACK")
    },
  },
  {
    boundary: "after commit but before local enqueue",
    claim: "committed intent remains discoverable after its lease expires",
    inject: async () => {
      const evidence = await runPersistence(Effect.gen(function* () {
        const repository = yield* DeliveryRepository
        const first = yield* saveInitiallyClaimed
        yield* TestClock.adjust("10 seconds")
        const recovered = yield* repository.claimPending(
          workerB,
          destination.id,
          1,
          leaseDurationMillis,
        )
        return { first, recovered }
      }))
      expect(evidence.first.delivery.state._tag).toBe("Pending")
      expect(evidence.recovered[0]?.delivery.id).toBe(delivery.id)
      expect(evidence.recovered[0]?.claim.generation).toBe(
        ClaimGeneration.make(2),
      )
    },
  },
  {
    boundary: "after claim but before outbound send",
    claim: "an unused claim changes ownership, not delivery truth",
    inject: async () => {
      const evidence = await runPersistence(Effect.gen(function* () {
        const repository = yield* DeliveryRepository
        const first = yield* saveInitiallyClaimed
        const status = yield* repository.findStatus(first.delivery.id)
        return { first, status }
      }))
      const status = Option.getOrThrow(evidence.status)
      expect(evidence.first.claim.generation).toBe(ClaimGeneration.make(1))
      expect(status.delivery.state._tag).toBe("Pending")
      expect(status.attempts).toEqual([])
    },
  },
  {
    boundary: "after remote acceptance but before local recording",
    claim: "local state remains pending in the uncertainty window",
    inject: async () => {
      const observed = makeGate<DeliveryId>()
      const runtime = makeSupervisorRuntime({
        hooks: {
          afterAttemptObserved: (deliveryId) =>
            Effect.sync(() => observed.resolve(deliveryId)).pipe(
              Effect.andThen(Effect.never),
            ),
        },
        post: () => Effect.succeed({ status: 202 }),
        resilience: { maxAttempts: 1 },
      })
      try {
        const context = await runtime.context()
        const supervisor = Context.get(context, DeliverySupervisor)
        const repository = Context.get(context, DeliveryRepository)
        const running = runtime.runPromise(supervisor.deliver(event)).then(
          () => "completed" as const,
          () => "interrupted" as const,
        )
        const deliveryId = await observed.promise
        const uncertain = Option.getOrThrow(
          await runtime.runPromise(repository.findStatus(deliveryId)),
        )
        expect(uncertain.delivery.state._tag).toBe("Pending")
        expect(uncertain.attempts).toEqual([])
        await runtime.dispose()
        expect(await running).toBe("interrupted")
      } finally {
        await runtime.dispose()
      }
    },
  },
  {
    boundary: "during local timeout ambiguity",
    claim: "a retry keeps identity but cannot prove one remote effect",
    inject: async () => {
      const firstStarted = makeGate<void>()
      const deliveryIds: Array<DeliveryId> = []
      let calls = 0
      const runtime = makeSupervisorRuntime({
        post: ({ deliveryId }) =>
          Effect.suspend(() => {
            calls += 1
            deliveryIds.push(deliveryId)
            if (calls === 1) {
              firstStarted.resolve(undefined)
              return Effect.never
            }
            return Effect.succeed({ status: 202 })
          }),
      })
      try {
        const result = await runtime.runPromise(Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const fiber = yield* supervisor.deliver(event).pipe(
            Effect.forkChild,
          )
          yield* Effect.promise(() => firstStarted.promise)
          yield* TestClock.adjust("2 seconds")
          return yield* Fiber.join(fiber)
        }).pipe(Random.withSeed("c09-02-timeout")))
        expect(result._tag).toBe("Delivered")
        expect(result.attempts.map(({ outcome }) => outcome._tag)).toEqual([
          "TimedOut",
          "Delivered",
        ])
        expect(deliveryIds[0]).toBe(deliveryIds[1])
      } finally {
        await runtime.dispose()
      }
    },
  },
  {
    boundary: "during retry delay and budget exhaustion",
    claim: "virtual time proves both no-early-retry and finite exhaustion",
    inject: async () => {
      let calls = 0
      const runtime = makeSupervisorRuntime({
        post: () => Effect.sync(() => {
          calls += 1
          return { retryAfter: "1", status: 429 }
        }),
      })
      try {
        const result = await runtime.runPromise(Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const fiber = yield* supervisor.deliver(event).pipe(
            Effect.forkChild,
          )
          yield* awaitCondition(() => calls === 1)
          yield* TestClock.adjust("999 millis")
          expect(calls).toBe(1)
          yield* TestClock.adjust("1 milli")
          yield* awaitCondition(() => calls === 2)
          return yield* Fiber.join(fiber)
        }).pipe(Random.withSeed("c09-02-exhaustion")))
        expect(result._tag).toBe("Exhausted")
        expect(result.attempts).toHaveLength(2)
        expect(calls).toBe(2)
      } finally {
        await runtime.dispose()
      }
    },
  },
  {
    boundary: "during lease expiry",
    claim: "work is not claimable early and gains a new generation at expiry",
    inject: async () => {
      const evidence = await runPersistence(Effect.gen(function* () {
        const repository = yield* DeliveryRepository
        yield* saveInitiallyClaimed
        const early = yield* repository.claimPending(
          workerB,
          destination.id,
          1,
          leaseDurationMillis,
        )
        yield* TestClock.adjust("10 seconds")
        const expired = yield* repository.claimPending(
          workerB,
          destination.id,
          1,
          leaseDurationMillis,
        )
        return { early, expired }
      }))
      expect(evidence.early).toEqual([])
      expect(evidence.expired[0]?.claim.generation).toBe(
        ClaimGeneration.make(2),
      )
    },
  },
  {
    boundary: "stale completion after reassignment",
    claim: "a stale generation cannot write terminal state",
    inject: async () => {
      const evidence = await runPersistence(Effect.gen(function* () {
        const repository = yield* DeliveryRepository
        const first = yield* saveInitiallyClaimed
        yield* TestClock.adjust("10 seconds")
        const [second] = yield* repository.claimPending(
          workerB,
          destination.id,
          1,
          leaseDurationMillis,
        )
        if (second === undefined) {
          return yield* Effect.die(new Error("expected reassignment"))
        }
        const stale = yield* repository.completeClaim(
          delivery.id,
          first.claim,
          terminalResult(first.claim),
        ).pipe(Effect.flip)
        const afterStale = yield* repository.findById(delivery.id)
        yield* repository.completeClaim(
          delivery.id,
          second.claim,
          terminalResult(second.claim),
        )
        const afterCurrent = yield* repository.findById(delivery.id)
        return { afterCurrent, afterStale, stale }
      }))
      expect(evidence.stale).toBeInstanceOf(ClaimLostError)
      expect(Option.getOrThrow(evidence.afterStale).state._tag).toBe(
        "Pending",
      )
      expect(Option.getOrThrow(evidence.afterCurrent).state._tag).toBe(
        "Delivered",
      )
    },
  },
  {
    boundary: "during slow-consumer saturation",
    claim: "admitted work stays bounded and excess work is rejected",
    inject: async () => {
      const started = makeGate<void>()
      const runtime = makeSupervisorRuntime({
        flow: {
          deliveryRequestsCapacity: 1,
          deliveryRequestsPerDestinationCapacity: 1,
        },
        post: () => Effect.sync(() => started.resolve(undefined)).pipe(
          Effect.andThen(Effect.never),
        ),
      })
      try {
        const observation = await runtime.runPromise(Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const first = yield* supervisor.deliver(event).pipe(
            Effect.forkChild({ startImmediately: true }),
          )
          yield* Effect.promise(() => started.promise)
          const excess = yield* supervisor.deliver(event).pipe(Effect.flip)
          const load = yield* supervisor.loadMetrics()
          yield* Fiber.interrupt(first)
          return { excess, load }
        }))
        expect(observation.excess).toBeInstanceOf(DeliveryOverloaded)
        expect(observation.load.admittedDeliveries).toBe(1)
        expect(observation.load.globalActive).toBe(1)
      } finally {
        await runtime.dispose()
      }
    },
  },
  {
    boundary: "during startup acquisition",
    claim: "readiness and shutdown registration wait for required acquisition",
    inject: async () => {
      const acquisitionStarted = makeGate<void>()
      const releaseAcquisition = makeGate<void>()
      let ready = false
      let shutdownRegistrations = 0
      const delayedPersistence = RelayPersistenceMemory.pipe(
        Layer.tap(() =>
          Effect.sync(() => acquisitionStarted.resolve(undefined)).pipe(
            Effect.andThen(
              Effect.promise(() => releaseAcquisition.promise),
            ),
          )
        ),
      )
      const readinessLayer = Layer.succeed(
        RelayReadiness,
        RelayReadiness.of({
          current: Effect.sync(() => ready),
          markNotReady: Effect.sync(() => {
            ready = false
          }),
          markReady: Effect.sync(() => {
            ready = true
          }),
        }),
      )
      const starting = startRelayApplication({
        configProvider: runtimeConfiguration(),
        httpClientLayer: makeHttpClientLayer((request) =>
          Effect.succeed(makeHttpResponse(request))
        ),
        httpServerLayer: makeTestHttpServerLayer(),
        persistenceLayer: delayedPersistence,
        readinessLayer,
        registerShutdownHook: () => {
          shutdownRegistrations += 1
          return () => {}
        },
      })
      await acquisitionStarted.promise
      expect(ready).toBe(false)
      expect(shutdownRegistrations).toBe(0)
      releaseAcquisition.resolve(undefined)
      const application = await starting
      try {
        expect(await application.isReady()).toBe(true)
        expect(shutdownRegistrations).toBe(1)
      } finally {
        await application.shutdown()
      }
    },
  },
  {
    boundary: "during graceful shutdown with active attempts",
    claim: "readiness closes before interruption and committed intent remains pending",
    inject: async () => {
      const markedNotReady = makeGate<void>()
      const releaseShutdown = makeGate<void>()
      const outboundStarted = makeGate<AbortSignal>()
      let ready = false
      const application = await startRelayApplication({
        configProvider: runtimeConfiguration(),
        httpClientLayer: makeHttpClientLayer(
          (_request, _endpoint, signal) =>
            Effect.sync(() => outboundStarted.resolve(signal)).pipe(
              Effect.andThen(Effect.never),
            ),
        ),
        httpServerLayer: makeTestHttpServerLayer(),
        persistenceLayer: RelayPersistenceMemory,
        readinessLayer: Layer.succeed(
          RelayReadiness,
          RelayReadiness.of({
            current: Effect.sync(() => ready),
            markNotReady: Effect.sync(() => {
              ready = false
              markedNotReady.resolve(undefined)
            }).pipe(
              Effect.andThen(
                Effect.promise(() => releaseShutdown.promise),
              ),
            ),
            markReady: Effect.sync(() => {
              ready = true
            }),
          }),
        ),
        registerShutdownHook: () => () => {},
      })
      const accepted = await application.accept(
        IngestionKey.make("c09-02:active-shutdown"),
        submission,
      )
      const signal = await outboundStarted.promise
      const beforeShutdown = await application.deliveryStatus(
        accepted.deliveryId,
      )
      expect(beforeShutdown?.delivery.state._tag).toBe("Pending")
      const stopping = application.shutdown()
      await markedNotReady.promise
      expect(await application.isReady()).toBe(false)
      expect(signal.aborted).toBe(false)
      releaseShutdown.resolve(undefined)
      await stopping
      expect(signal.aborted).toBe(true)
    },
  },
]

describe("C09-02 complete failure-injection matrix", () => {
  for (const row of matrix) {
    it(`${row.boundary}: ${row.claim}`, row.inject)
  }
})

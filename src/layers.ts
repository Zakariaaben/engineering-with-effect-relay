import { NodeCrypto } from "@effect/platform-node"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import {
  Clock,
  ConfigProvider,
  Effect,
  Layer,
  Option,
} from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import {
  DestinationClientLive,
} from "./destinationClient.ts"
import { DeliveryEventsLive } from "./deliveryEvents.ts"
import { DeliveryOperationsLive } from "./deliveryOperations.ts"
import {
  DeliveryRepositorySql,
  PostgresLive,
} from "./deliveryRepositorySql.ts"
import { DeliverySupervisorLive } from "./deliverySupervisor.ts"
import { EventIntakeLive } from "./eventIntake.ts"
import {
  ClaimLostError,
  DeadLetterRecoveryError,
  DeliveryRepositoryError,
  IngestionConflictError,
} from "./errors.ts"
import {
  DeliveryHttpRoutes,
  IntakeAuthorizationLive,
} from "./httpServer.ts"
import {
  Delivery,
  ClaimGeneration,
  DeliveryClaim,
  DeliveryRouteSnapshot,
  DeliveryResult,
  DeliveryState,
  DeliveryStatus,
  type DeliveryId,
  type DeliveryAttemptRecord as DeliveryAttemptRecordValue,
  type EventId,
  type IngestionKey,
  type RelayEvent,
  type RequestFingerprint,
  type WorkerId,
} from "./model.ts"
import { ReconcilerLive } from "./reconciler.ts"
import { RelayIntakeStoreSql } from "./intakeStoreSql.ts"
import { RelayMigrationsLive } from "./migrations.ts"
import {
  DeliveryRepository,
  IntakeDecision,
  type IntakeDecisionFields,
  type IntakeRecord,
  RelayIntakeStore,
} from "./services.ts"
import { AppConfigurationLive } from "./configuration.ts"
import {
  RelayReadiness,
  RelayReadinessLive,
} from "./readiness.ts"
import {
  WorkerIdentity,
  WorkerIdentityLive,
} from "./workerIdentity.ts"

const stateFromResult = (
  result: DeliveryResult,
): Delivery["state"] =>
  DeliveryResult.$match(result, {
    Delivered: ({ status }) =>
      DeliveryState.cases.Delivered.make({ status }),
    Rejected: ({ status }) =>
      DeliveryState.cases.Rejected.make({ status }),
    ProtocolFailure: () => DeliveryState.cases.DeadLettered.make({
      reason: "ProviderProtocolFailure",
    }),
    Exhausted: () => DeliveryState.cases.DeadLettered.make({
      reason: "RetryBudgetExhausted",
    }),
  })

const makeMemoryRepository = (
  events: Map<EventId, RelayEvent>,
  records: Map<DeliveryId, Delivery>,
  claims: Map<DeliveryId, DeliveryClaim>,
  generations: Map<DeliveryId, number>,
  routes: Map<DeliveryId, DeliveryRouteSnapshot> = new Map(),
  attempts: Map<DeliveryId, Array<DeliveryAttemptRecordValue>> = new Map(),
  nextEligibleAt: Map<DeliveryId, number> = new Map(),
) => {
  const lost = (
    operation: "renew" | "recordAttempt" | "complete" | "release",
    deliveryId: DeliveryId,
    claim: DeliveryClaim,
  ) => new ClaimLostError({
    operation,
    deliveryId,
    ownerId: claim.ownerId,
    generation: claim.generation,
  })
  const owns = (
    deliveryId: DeliveryId,
    expected: DeliveryClaim,
    nowMillis?: number,
  ) => {
    const current = claims.get(deliveryId)
    return current !== undefined &&
      current.ownerId === expected.ownerId &&
      current.generation === expected.generation &&
      (nowMillis === undefined || current.leaseExpiresAtMillis > nowMillis)
  }
  const save = Effect.fn("DeliveryRepository.save")(
    (delivery: Delivery) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        if (!records.has(delivery.id)) {
          records.set(delivery.id, delivery)
          generations.set(delivery.id, 0)
          nextEligibleAt.set(delivery.id, nowMillis)
        }
      }),
  )
  const findById = Effect.fn("DeliveryRepository.findById")(
    (id: DeliveryId) =>
      Effect.sync(() => Option.fromNullishOr(records.get(id))),
  )
  const statusFor = (delivery: Delivery) => DeliveryStatus.make({
    delivery,
    attempts: [...(attempts.get(delivery.id) ?? [])].sort(
      (left, right) => left.ordinal - right.ordinal,
    ),
  })
  const findStatus = Effect.fn("DeliveryRepository.findStatus")(
    (id: DeliveryId) =>
      Effect.sync(() => Option.fromNullishOr(records.get(id))).pipe(
        Effect.map(Option.map(statusFor)),
      ),
  )
  const recordAttempt = Effect.fn("DeliveryRepository.recordAttempt")(
    (attempt: DeliveryAttemptRecordValue) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        const claim = DeliveryClaim.make({
          ownerId: attempt.workerId,
          generation: attempt.claimGeneration,
          leaseExpiresAtMillis: 0,
        })
        const delivery = records.get(attempt.deliveryId)
        if (
          delivery === undefined ||
          delivery.state._tag !== "Pending" ||
          !owns(attempt.deliveryId, claim, nowMillis)
        ) {
          return yield* Effect.fail(
            lost("recordAttempt", attempt.deliveryId, claim),
          )
        }
        const history = attempts.get(attempt.deliveryId) ?? []
        if (history.some(({ ordinal }) => ordinal === attempt.ordinal)) {
          return yield* Effect.fail(new DeliveryRepositoryError({
            operation: "recordAttempt",
            cause: `attempt ordinal ${attempt.ordinal} already exists`,
          }))
        }
        if (attempt.decision === "RetryScheduled") {
          attempts.set(attempt.deliveryId, [...history, attempt])
          nextEligibleAt.set(
            attempt.deliveryId,
            attempt.completedAtMillis +
              (attempt.retryDelayMillis ?? 0),
          )
          return
        }

        const terminalState = attempt.decision === "Exhausted"
          ? DeliveryState.cases.DeadLettered.make({
            reason: "RetryBudgetExhausted",
          })
          : attempt.outcome === "Delivered" && attempt.status !== null
          ? DeliveryState.cases.Delivered.make({
            status: attempt.status,
          })
          : attempt.outcome === "Rejected" && attempt.status !== null
          ? DeliveryState.cases.Rejected.make({
            status: attempt.status,
          })
          : attempt.outcome === "ProtocolFailure"
          ? DeliveryState.cases.DeadLettered.make({
            reason: "ProviderProtocolFailure",
          })
          : undefined

        if (terminalState === undefined) {
          return yield* Effect.fail(new DeliveryRepositoryError({
            operation: "recordAttempt",
            cause: "terminal attempt has no terminal delivery state",
          }))
        }

        attempts.set(attempt.deliveryId, [...history, attempt])
        records.set(attempt.deliveryId, Delivery.make({
          ...delivery,
          state: terminalState,
        }))
        claims.delete(attempt.deliveryId)
      }),
  )
  const listDeadLetters = Effect.fn(
    "DeliveryRepository.listDeadLetters",
  )((limit: number) =>
    Effect.sync(() =>
      [...records.values()]
        .filter(({ state }) => state._tag === "DeadLettered")
        .sort((left, right) => left.id.localeCompare(right.id))
        .slice(0, limit)
        .map(statusFor)
    ))
  const retryDeadLetter = Effect.fn(
    "DeliveryRepository.retryDeadLetter",
  )((id: DeliveryId) =>
    Effect.gen(function* () {
      const nowMillis = yield* Clock.currentTimeMillis
      const current = records.get(id)
      if (current === undefined || current.state._tag !== "DeadLettered") {
        return yield* Effect.fail(new DeadLetterRecoveryError({
          deliveryId: id,
          reason: "NotDeadLettered",
        }))
      }
      records.set(id, Delivery.make({
        ...current,
        state: DeliveryState.cases.Pending.make({}),
      }))
      claims.delete(id)
      nextEligibleAt.set(id, nowMillis)
    }))
  const claimPending = Effect.fn("DeliveryRepository.claimPending")(
    (
      ownerId: WorkerId,
      destinationId: Delivery["destinationId"],
      limit: number,
      leaseDurationMillis: number,
    ) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        const claimed = []
        for (const delivery of records.values()) {
          const existingClaim = claims.get(delivery.id)
          if (
            claimed.length >= limit ||
            delivery.state._tag !== "Pending" ||
            delivery.destinationId !== destinationId ||
            (nextEligibleAt.get(delivery.id) ?? 0) > nowMillis ||
            (existingClaim !== undefined &&
              existingClaim.leaseExpiresAtMillis > nowMillis)
          ) {
            continue
          }
          const event = events.get(delivery.eventId)
          if (event === undefined) continue
          const generation = (generations.get(delivery.id) ?? 0) + 1
          const claim = DeliveryClaim.make({
            ownerId,
            generation: ClaimGeneration.make(generation),
            leaseExpiresAtMillis: nowMillis + leaseDurationMillis,
          })
          generations.set(delivery.id, generation)
          claims.set(delivery.id, claim)
          const eligibleAt = nextEligibleAt.get(delivery.id) ?? nowMillis
          claimed.push({
            claim,
            claimLagMillis: Math.max(0, nowMillis - eligibleAt),
            delivery,
            event,
            nextAttemptOrdinal: Math.max(
              0,
              ...(attempts.get(delivery.id) ?? []).map(
                ({ ordinal }) => ordinal,
              ),
            ) + 1,
            route: Option.fromNullishOr(routes.get(delivery.id)),
          })
        }
        return claimed
      }),
  )
  const renewClaim = Effect.fn("DeliveryRepository.renewClaim")(
    (
      deliveryId: DeliveryId,
      claim: DeliveryClaim,
      leaseDurationMillis: number,
    ) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        if (!owns(deliveryId, claim, nowMillis)) {
          return yield* Effect.fail(lost("renew", deliveryId, claim))
        }
        const renewed = DeliveryClaim.make({
          ...claim,
          leaseExpiresAtMillis: nowMillis + leaseDurationMillis,
        })
        claims.set(deliveryId, renewed)
        return renewed
      }),
  )
  const completeClaim = Effect.fn("DeliveryRepository.completeClaim")(
    (
      deliveryId: DeliveryId,
      claim: DeliveryClaim,
      result: DeliveryResult,
    ) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        const current = records.get(deliveryId)
        if (
          current === undefined ||
          current.state._tag !== "Pending" ||
          !owns(deliveryId, claim, nowMillis)
        ) {
          return yield* Effect.fail(lost("complete", deliveryId, claim))
        }
        records.set(
          deliveryId,
          Delivery.make({
            ...current,
            state: stateFromResult(result),
          }),
        )
        claims.delete(deliveryId)
      }),
  )
  const releaseClaim = Effect.fn("DeliveryRepository.releaseClaim")(
    (deliveryId: DeliveryId, claim: DeliveryClaim) =>
      Effect.gen(function* () {
        const current = records.get(deliveryId)
        if (
          current === undefined ||
          current.state._tag !== "Pending" ||
          !owns(deliveryId, claim)
        ) {
          return yield* Effect.fail(lost("release", deliveryId, claim))
        }
        claims.delete(deliveryId)
      }),
  )

  return DeliveryRepository.of({
    save,
    findById,
    findStatus,
    recordAttempt,
    listDeadLetters,
    retryDeadLetter,
    claimPending,
    renewClaim,
    completeClaim,
    releaseClaim,
  })
}

export const DeliveryRepositoryMemory = Layer.sync(
  DeliveryRepository,
  () => makeMemoryRepository(new Map(), new Map(), new Map(), new Map()),
)

const makeRelayPersistenceMemory = () => Layer.suspend(() => {
  const events = new Map<EventId, RelayEvent>()
  const deliveries = new Map<DeliveryId, Delivery>()
  const claims = new Map<DeliveryId, DeliveryClaim>()
  const generations = new Map<DeliveryId, number>()
  const routes = new Map<DeliveryId, DeliveryRouteSnapshot>()
  const attempts = new Map<DeliveryId, Array<DeliveryAttemptRecordValue>>()
  const nextEligibleAt = new Map<DeliveryId, number>()
  const intakes = new Map<
    IngestionKey,
    {
      readonly requestFingerprint: RequestFingerprint
      readonly decision: IntakeDecisionFields
    }
  >()
  const repository = makeMemoryRepository(
    events,
    deliveries,
    claims,
    generations,
    routes,
    attempts,
    nextEligibleAt,
  )

  const accept = Effect.fn("RelayIntakeStore.accept")(
    (record: IntakeRecord) =>
      Effect.gen(function* () {
        const existing = intakes.get(record.ingestionKey)
        if (existing !== undefined) {
          if (existing.requestFingerprint !== record.requestFingerprint) {
            return yield* Effect.fail(new IngestionConflictError({
              ingestionKey: record.ingestionKey,
              existingEventId: existing.decision.event.id,
            }))
          }
          return IntakeDecision.Replay(existing.decision)
        }

        const nowMillis = yield* Clock.currentTimeMillis
        const delivery = Delivery.make({
          id: record.deliveryId,
          eventId: record.event.id,
          destinationId: record.route.destinationId,
          state: DeliveryState.cases.Pending.make({}),
        })
        const decision = {
          event: record.event,
          delivery,
          route: record.route,
          acceptedAtMillis: record.acceptedAtMillis,
        }
        const claim = DeliveryClaim.make({
          ownerId: record.claim.ownerId,
          generation: ClaimGeneration.make(1),
          leaseExpiresAtMillis:
            nowMillis + record.claim.leaseDurationMillis,
        })
        events.set(record.event.id, record.event)
        deliveries.set(delivery.id, delivery)
        routes.set(delivery.id, record.route)
        generations.set(delivery.id, 1)
        claims.set(delivery.id, claim)
        nextEligibleAt.set(delivery.id, record.acceptedAtMillis)
        intakes.set(record.ingestionKey, {
          requestFingerprint: record.requestFingerprint,
          decision,
        })
        return IntakeDecision.Accepted({ ...decision, claim })
      }),
  )

  const savePending = Effect.fn("RelayIntakeStore.savePending")(
    (
      event: RelayEvent,
      id: DeliveryId,
      destinationId: Delivery["destinationId"],
      claimRequest,
    ) =>
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis
        const delivery = Delivery.make({
          id,
          eventId: event.id,
          destinationId,
          state: DeliveryState.cases.Pending.make({}),
        })
        events.set(event.id, event)
        deliveries.set(delivery.id, delivery)
        nextEligibleAt.set(delivery.id, nowMillis)
        const claim = DeliveryClaim.make({
          ownerId: claimRequest.ownerId,
          generation: ClaimGeneration.make(1),
          leaseExpiresAtMillis:
            nowMillis + claimRequest.leaseDurationMillis,
        })
        generations.set(delivery.id, 1)
        claims.set(delivery.id, claim)
        return {
          claim,
          claimLagMillis: 0,
          delivery,
          event,
          nextAttemptOrdinal: 1,
          route: Option.none(),
        }
      }),
  )

  return Layer.merge(
    Layer.succeed(DeliveryRepository, repository),
    Layer.succeed(
      RelayIntakeStore,
      RelayIntakeStore.of({ accept, savePending }),
    ),
  )
})

export const RelayIntakeStoreMemory = makeRelayPersistenceMemory()

export const RelayPersistenceMemory = makeRelayPersistenceMemory()

export const RelayPersistenceLive = Layer.mergeAll(
  DeliveryRepositorySql,
  RelayIntakeStoreSql,
  RelayMigrationsLive,
).pipe(
  Layer.provide(PostgresLive),
)

export type RelayPersistenceLayer = Layer.Layer<
  DeliveryRepository | RelayIntakeStore,
  unknown
>

export const makeRelayAdapterLayer = (
  httpClientLayer: Layer.Layer<HttpClient.HttpClient>,
  persistenceLayer: RelayPersistenceLayer = RelayPersistenceMemory,
) =>
  Layer.mergeAll(
    DestinationClientLive.pipe(
      Layer.provide(httpClientLayer),
    ),
    persistenceLayer,
  )

export const makeRelayApplicationLayer = (
  httpClientLayer: Layer.Layer<HttpClient.HttpClient>,
  configProvider: ConfigProvider.ConfigProvider,
  persistenceLayer: RelayPersistenceLayer = RelayPersistenceMemory,
  workerIdentityLayer: Layer.Layer<
    WorkerIdentity,
    unknown
  > = WorkerIdentityLive,
) => {
  const dependencies = Layer.mergeAll(
    makeRelayAdapterLayer(httpClientLayer, persistenceLayer),
    AppConfigurationLive,
    NodeCrypto.layer,
  ).pipe(
    Layer.provide(ConfigProvider.layer(configProvider)),
  )
  const distributedDependencies = Layer.merge(
    dependencies,
    workerIdentityLayer,
  )

  const supervisor = DeliverySupervisorLive.pipe(
    Layer.provideMerge(DeliveryEventsLive),
    Layer.provideMerge(distributedDependencies),
  )

  return Layer.mergeAll(
    ReconcilerLive,
    EventIntakeLive,
    DeliveryOperationsLive,
  ).pipe(
    Layer.provideMerge(supervisor),
  )
}

export type RelayHttpServerLayer = ReturnType<
  typeof NodeHttpServer.layer
>

export const makeRelayHttpApplicationLayer = (
  httpClientLayer: Layer.Layer<HttpClient.HttpClient>,
  httpServerLayer: RelayHttpServerLayer,
  configProvider: ConfigProvider.ConfigProvider,
  persistenceLayer: RelayPersistenceLayer = RelayPersistenceMemory,
  readinessLayer: Layer.Layer<RelayReadiness> = RelayReadinessLive,
  workerIdentityLayer: Layer.Layer<
    WorkerIdentity,
    unknown
  > = WorkerIdentityLive,
) => {
  const application = makeRelayApplicationLayer(
    httpClientLayer,
    configProvider,
    persistenceLayer,
    workerIdentityLayer,
  )
  const intakeAuthorization = IntakeAuthorizationLive.pipe(
    Layer.provide(ConfigProvider.layer(configProvider)),
  )

  return HttpRouter.serve(DeliveryHttpRoutes).pipe(
    Layer.provideMerge(application),
    Layer.provideMerge(intakeAuthorization),
    Layer.provideMerge(readinessLayer),
    Layer.provideMerge(httpServerLayer),
  )
}

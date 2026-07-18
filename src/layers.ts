import { NodeCrypto } from "@effect/platform-node"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import { ConfigProvider, Effect, Layer, Option } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import {
  DestinationClientLive,
} from "./destinationClient.ts"
import { DeliveryEventsLive } from "./deliveryEvents.ts"
import {
  DeliveryRepositorySql,
  PostgresLive,
} from "./deliveryRepositorySql.ts"
import { DeliverySupervisorLive } from "./deliverySupervisor.ts"
import {
  DeliveryHttpRoutes,
  IntakeAuthorizationLive,
} from "./httpServer.ts"
import {
  Delivery,
  DeliveryResult,
  DeliveryState,
  type DeliveryId,
  type EventId,
  type RelayEvent,
} from "./model.ts"
import { ReconcilerLive } from "./reconciler.ts"
import { RelayIntakeStoreSql } from "./intakeStoreSql.ts"
import { RelayMigrationsLive } from "./migrations.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "./services.ts"
import { AppConfigurationLive } from "./configuration.ts"
import {
  RelayReadiness,
  RelayReadinessLive,
} from "./readiness.ts"

const stateFromResult = (
  result: DeliveryResult,
): Delivery["state"] =>
  DeliveryResult.$match(result, {
    Delivered: ({ status }) =>
      DeliveryState.cases.Delivered.make({ status }),
    Rejected: ({ status }) =>
      DeliveryState.cases.Rejected.make({ status }),
    ProtocolFailure: () => DeliveryState.cases.Pending.make({}),
    Exhausted: () => DeliveryState.cases.Pending.make({}),
  })

const makeMemoryRepository = (
  events: Map<EventId, RelayEvent>,
  records: Map<DeliveryId, Delivery>,
  claims: Set<DeliveryId>,
) => {
  const save = Effect.fn("DeliveryRepository.save")(
    (delivery: Delivery) =>
      Effect.sync(() => {
        records.set(delivery.id, delivery)
        claims.delete(delivery.id)
      }),
  )
  const findById = Effect.fn("DeliveryRepository.findById")(
    (id: DeliveryId) =>
      Effect.sync(() => Option.fromNullishOr(records.get(id))),
  )
  const resetClaims = Effect.fn("DeliveryRepository.resetClaims")(
    () => Effect.sync(() => claims.clear()),
  )
  const claimPending = Effect.fn("DeliveryRepository.claimPending")(
    (destinationId: Delivery["destinationId"], limit: number) =>
      Effect.sync(() => {
        const claimed = []
        for (const delivery of records.values()) {
          if (
            claimed.length >= limit ||
            delivery.state._tag !== "Pending" ||
            delivery.destinationId !== destinationId ||
            claims.has(delivery.id)
          ) {
            continue
          }
          const event = events.get(delivery.eventId)
          if (event === undefined) continue
          claims.add(delivery.id)
          claimed.push({ delivery, event })
        }
        return claimed
      }),
  )
  const completeClaim = Effect.fn("DeliveryRepository.completeClaim")(
    (deliveryId: DeliveryId, result: DeliveryResult) =>
      Effect.sync(() => {
        const current = records.get(deliveryId)
        if (current !== undefined && claims.has(deliveryId)) {
          records.set(
            deliveryId,
            Delivery.make({
              ...current,
              state: stateFromResult(result),
            }),
          )
        }
        claims.delete(deliveryId)
      }),
  )
  const releaseClaim = Effect.fn("DeliveryRepository.releaseClaim")(
    (deliveryId: DeliveryId) =>
      Effect.sync(() => {
        claims.delete(deliveryId)
      }),
  )

  return DeliveryRepository.of({
    save,
    findById,
    resetClaims,
    claimPending,
    completeClaim,
    releaseClaim,
  })
}

export const DeliveryRepositoryMemory = Layer.sync(
  DeliveryRepository,
  () => makeMemoryRepository(new Map(), new Map(), new Set()),
)

const makeRelayPersistenceMemory = () => Layer.suspend(() => {
  const events = new Map<EventId, RelayEvent>()
  const deliveries = new Map<DeliveryId, Delivery>()
  const claims = new Set<DeliveryId>()
  const repository = makeMemoryRepository(events, deliveries, claims)

  const savePending = Effect.fn("RelayIntakeStore.savePending")(
    (
      event: RelayEvent,
      id: DeliveryId,
      destinationId: Delivery["destinationId"],
    ) =>
      Effect.sync(() => {
        const delivery = Delivery.make({
          id,
          eventId: event.id,
          destinationId,
          state: DeliveryState.cases.Pending.make({}),
        })
        events.set(event.id, event)
        deliveries.set(delivery.id, delivery)
        claims.add(delivery.id)
        return delivery
      }),
  )

  return Layer.merge(
    Layer.succeed(DeliveryRepository, repository),
    Layer.succeed(
      RelayIntakeStore,
      RelayIntakeStore.of({ savePending }),
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
) => {
  const dependencies = Layer.mergeAll(
    makeRelayAdapterLayer(httpClientLayer, persistenceLayer),
    AppConfigurationLive,
    NodeCrypto.layer,
  ).pipe(
    Layer.provide(ConfigProvider.layer(configProvider)),
  )

  const supervisor = DeliverySupervisorLive.pipe(
    Layer.provideMerge(DeliveryEventsLive),
    Layer.provideMerge(dependencies),
  )

  return ReconcilerLive.pipe(
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
) => {
  const application = makeRelayApplicationLayer(
    httpClientLayer,
    configProvider,
    persistenceLayer,
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

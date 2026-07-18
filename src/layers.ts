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
  DeliveryState,
  type DeliveryId,
  type EventId,
  type RelayEvent,
} from "./model.ts"
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

export const DeliveryRepositoryMemory = Layer.sync(
  DeliveryRepository,
  () => {
    const records = new Map<DeliveryId, Delivery>()

    const save = Effect.fn("DeliveryRepository.save")(
      (delivery: Delivery) =>
        Effect.sync(() => {
          records.set(delivery.id, delivery)
        }),
    )
    const findById = Effect.fn("DeliveryRepository.findById")(
      (id: DeliveryId) =>
        Effect.sync(() => Option.fromNullishOr(records.get(id))),
    )

    return DeliveryRepository.of({ save, findById })
  },
)

export const RelayIntakeStoreMemory = Layer.sync(
  RelayIntakeStore,
  () => {
    const events = new Map<EventId, RelayEvent>()
    const deliveries = new Map<DeliveryId, Delivery>()

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
          return delivery
        }),
    )

    return RelayIntakeStore.of({ savePending })
  },
)

export const RelayPersistenceMemory = Layer.merge(
  DeliveryRepositoryMemory,
  RelayIntakeStoreMemory,
)

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

  return DeliverySupervisorLive.pipe(
    Layer.provideMerge(DeliveryEventsLive),
    Layer.provide(dependencies),
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

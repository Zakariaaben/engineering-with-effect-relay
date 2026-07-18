import { NodeCrypto } from "@effect/platform-node"
import { ConfigProvider, Effect, Layer, Option } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import {
  DestinationClientLive,
} from "./destinationClient.ts"
import { DeliveryEventsLive } from "./deliveryEvents.ts"
import { DeliverySupervisorLive } from "./deliverySupervisor.ts"
import type {
  Delivery,
  DeliveryId,
} from "./model.ts"
import { DeliveryRepository } from "./services.ts"
import { AppConfigurationLive } from "./configuration.ts"

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

export const makeRelayAdapterLayer = (
  httpClientLayer: Layer.Layer<HttpClient.HttpClient>,
) =>
  Layer.mergeAll(
    DestinationClientLive.pipe(
      Layer.provide(httpClientLayer),
    ),
    DeliveryRepositoryMemory,
  )

export const makeRelayApplicationLayer = (
  httpClientLayer: Layer.Layer<HttpClient.HttpClient>,
  configProvider: ConfigProvider.ConfigProvider,
) => {
  const dependencies = Layer.mergeAll(
    makeRelayAdapterLayer(httpClientLayer),
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

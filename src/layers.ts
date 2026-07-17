import { Effect, Layer, Option } from "effect"
import {
  DestinationClient,
  makeFetchDestinationClient,
  type Fetch,
} from "./destinationClient.ts"
import type {
  Delivery,
  DeliveryId,
} from "./model.ts"
import { DeliveryRepository } from "./services.ts"

export const destinationClientFromFetch = (fetch: Fetch) =>
  Layer.succeed(
    DestinationClient,
    makeFetchDestinationClient(fetch),
  )

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

export const makeRelayAdapterLayer = (fetch: Fetch) =>
  Layer.mergeAll(
    destinationClientFromFetch(fetch),
    DeliveryRepositoryMemory,
  )

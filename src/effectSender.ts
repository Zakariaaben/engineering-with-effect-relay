import { Data, Effect } from "effect"
import type { DestinationClient } from "./destinationClient.ts"
import {
  classifyDeliveryStatus,
  makeDeliveryRequest,
  type Destination,
  type DestinationId,
  type RelayEvent,
} from "./model.ts"

export class DeliveryTransportError extends Data.TaggedError(
  "DeliveryTransportError",
)<{
  readonly destinationId: DestinationId
  readonly cause: unknown
}> {}

export const sendDelivery = Effect.fn("Relay.sendDelivery")(
  function* (
    event: RelayEvent,
    destination: Destination,
    client: DestinationClient,
  ) {
    const request = makeDeliveryRequest(event, destination)
    const status = yield* Effect.tryPromise({
      try: (signal) => client.post({ ...request, signal }),
      catch: (cause) =>
        new DeliveryTransportError({
          destinationId: destination.id,
          cause,
        }),
    })

    return classifyDeliveryStatus(destination.id, status)
  },
)

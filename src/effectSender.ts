import { Effect } from "effect"
import { DestinationClient } from "./destinationClient.ts"
import { DeliveryTransportError } from "./errors.ts"
import {
  classifyDeliveryStatus,
  makeDeliveryRequest,
  type Destination,
  type RelayEvent,
} from "./model.ts"

export const sendDelivery = Effect.fn("Relay.sendDelivery")(
  function* (
    event: RelayEvent,
    destination: Destination,
  ) {
    const client = yield* DestinationClient
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

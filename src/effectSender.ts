import { Clock, Effect } from "effect"
import { DestinationClient } from "./destinationClient.ts"
import { DeliveryTransportError } from "./errors.ts"
import {
  classifyDeliveryResponse,
  makeDeliveryRequest,
  type DeliveryId,
  type Destination,
  type RelayEvent,
} from "./model.ts"

export const sendDelivery = Effect.fn("Relay.sendDelivery")(
  function* (
    deliveryId: DeliveryId,
    event: RelayEvent,
    destination: Destination,
  ) {
    const client = yield* DestinationClient
    const request = makeDeliveryRequest(
      deliveryId,
      event,
      destination,
    )
    const evidence = yield* Effect.tryPromise({
      try: (signal) => client.post({ ...request, signal }),
      catch: (cause) =>
        new DeliveryTransportError({
          deliveryId,
          destinationId: destination.id,
          cause,
        }),
    })
    const response = typeof evidence === "number"
      ? { status: evidence }
      : evidence
    const nowMillis = yield* Clock.currentTimeMillis

    return classifyDeliveryResponse(
      destination.id,
      response,
      nowMillis,
    )
  },
)

import { Clock, Effect } from "effect"
import { DestinationClient } from "./destinationClient.ts"
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
    const response = yield* client.post(request)
    const nowMillis = yield* Clock.currentTimeMillis

    return classifyDeliveryResponse(
      destination.id,
      response,
      nowMillis,
    )
  },
)

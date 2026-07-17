import { Effect } from "effect"
import { sendDelivery } from "./effectSender.ts"
import { InvalidEventError } from "./errors.ts"
import {
  decodeRelayEvent,
  type DeliveryId,
  type Destination,
} from "./model.ts"

export const decodeIncomingEvent = Effect.fn("Relay.decodeIncomingEvent")(
  function* (candidate: unknown) {
    return yield* decodeRelayEvent(candidate).pipe(
      Effect.mapError((error) =>
        new InvalidEventError({ summary: error.message })
      ),
    )
  },
)

export const deliverCandidate = Effect.fn("Relay.deliverCandidate")(
  function* (
    deliveryId: DeliveryId,
    candidate: unknown,
    destination: Destination,
  ) {
    const event = yield* decodeIncomingEvent(candidate)
    return yield* sendDelivery(deliveryId, event, destination)
  },
)

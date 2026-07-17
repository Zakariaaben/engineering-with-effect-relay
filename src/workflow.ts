import { Effect } from "effect"
import type { DestinationClient } from "./destinationClient.ts"
import { sendDelivery } from "./effectSender.ts"
import { InvalidEventError } from "./errors.ts"
import {
  decodeRelayEvent,
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
    candidate: unknown,
    destination: Destination,
    client: DestinationClient,
  ) {
    const event = yield* decodeIncomingEvent(candidate)
    return yield* sendDelivery(event, destination, client)
  },
)

import { Crypto, Effect } from "effect"
import {
  DeliveryId,
  EventId,
  WorkerId,
} from "./model.ts"

export const generateEventId = Effect.fn("Relay.generateEventId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return EventId.make(`evt-${uuid}`)
  },
)

export const generateDeliveryId = Effect.fn("Relay.generateDeliveryId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return DeliveryId.make(`dlv-${uuid}`)
  },
)

export const generateWorkerId = Effect.fn("Relay.generateWorkerId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return WorkerId.make(`wrk-${uuid}`)
  },
)

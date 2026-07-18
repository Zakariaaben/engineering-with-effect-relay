import { describe, expect, it } from "bun:test"
import { Crypto, Effect, Option } from "effect"
import {
  generateDeliveryId,
  generateEventId,
} from "../src/identifiers.ts"
import { DeliveryRepository } from "../src/services.ts"
import { delivery } from "./fixtures.ts"

describe("C03-02 service boundaries", () => {
  it("substitutes a delivery repository without changing domain data", async () => {
    const records = new Map<typeof delivery.id, typeof delivery>()
    const repository = DeliveryRepository.of({
      save: (record) => Effect.sync(() => {
        records.set(record.id, record)
      }),
      findById: (id) =>
        Effect.succeed(Option.fromNullishOr(records.get(id))),
      resetClaims: () => Effect.void,
      claimPending: () => Effect.succeed([]),
      completeClaim: () => Effect.void,
      releaseClaim: () => Effect.void,
    })

    const roundTrip = Effect.gen(function* () {
      const deliveries = yield* DeliveryRepository
      yield* deliveries.save(delivery)
      return yield* deliveries.findById(delivery.id)
    }).pipe(Effect.provideService(DeliveryRepository, repository))

    expect(await Effect.runPromise(roundTrip)).toEqual(Option.some(delivery))
  })

  it("uses the existing Crypto capability behind domain ID operations", async () => {
    const crypto = Crypto.make({
      randomBytes: (size) => new Uint8Array(size),
      digest: (_algorithm, bytes) => Effect.succeed(bytes),
    })
    const ids = Effect.all({
      eventId: generateEventId(),
      deliveryId: generateDeliveryId(),
    }).pipe(Effect.provideService(Crypto.Crypto, crypto))

    const result = await Effect.runPromise(ids)
    expect(String(result.eventId)).toBe(
      "evt-00000000-0000-4000-8000-000000000000",
    )
    expect(String(result.deliveryId)).toBe(
      "dlv-00000000-0000-4000-8000-000000000000",
    )
  })
})

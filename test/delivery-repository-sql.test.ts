import { describe, expect, it } from "bun:test"
import { Effect, Option, Schema } from "effect"
import {
  deliveryToRow,
  makeDeliveryRepositorySql,
} from "../src/deliveryRepositorySql.ts"
import { delivery } from "./fixtures.ts"

describe("C07-04 SQL repository boundary", () => {
  it("decodes a valid row before returning domain state", async () => {
    const repository = makeDeliveryRepositorySql({
      save: () => Effect.void,
      findById: () => Effect.succeed([deliveryToRow(delivery)]),
    })

    const result = await Effect.runPromise(repository.findById(delivery.id))

    expect(result).toEqual(Option.some(delivery))
  })

  it("rejects a row whose state and status disagree", async () => {
    const repository = makeDeliveryRepositorySql({
      save: () => Effect.void,
      findById: () =>
        Effect.succeed([{
          delivery_id: String(delivery.id),
          event_id: String(delivery.eventId),
          destination_id: String(delivery.destinationId),
          state: "Pending",
          status: 202,
        }]),
    })

    const error = await Effect.runPromise(
      Effect.flip(repository.findById(delivery.id)),
    )

    expect(error.operation).toBe("findById")
    expect(Schema.isSchemaError(error.cause)).toBe(true)
  })
})

import { describe, expect, it } from "bun:test"
import { Effect, Option, Schema } from "effect"
import {
  deliveryToRow,
  makeDeliveryRepositorySql,
} from "../src/deliveryRepositorySql.ts"
import { delivery, event } from "./fixtures.ts"

const recoveryStatements = {
  resetClaims: () => Effect.void,
  claimPending: () => Effect.succeed([]),
  completeClaim: () => Effect.void,
  releaseClaim: () => Effect.void,
}

describe("C07-04 SQL repository boundary", () => {
  it("decodes a valid row before returning domain state", async () => {
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([deliveryToRow(delivery)]),
    })

    const result = await Effect.runPromise(repository.findById(delivery.id))

    expect(result).toEqual(Option.some(delivery))
  })

  it("rejects a row whose state and status disagree", async () => {
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
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

  it("decodes a bounded database claim with its durable event", async () => {
    let observedRequest: unknown
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([]),
      claimPending: (request) => {
        observedRequest = request
        return Effect.succeed([{
          delivery_id: String(delivery.id),
          event_id: String(event.id),
          destination_id: String(delivery.destinationId),
          event_type: event.type,
          invoice_id: String(event.invoiceId),
          amount_cents: event.amountCents,
          destination_url: null,
          configuration_version: null,
        }])
      },
    })

    const claimed = await Effect.runPromise(
      repository.claimPending(delivery.destinationId, 2),
    )

    expect(observedRequest).toEqual({
      destination_id: delivery.destinationId,
      limit: 2,
    })
    expect(claimed).toEqual([{
      delivery,
      event,
      route: Option.none(),
    }])
  })

  it("rejects an invalid claim batch before querying", async () => {
    let queried = false
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([]),
      claimPending: () => {
        queried = true
        return Effect.succeed([])
      },
    })

    const error = await Effect.runPromise(
      Effect.flip(repository.claimPending(delivery.destinationId, 0)),
    )

    expect(queried).toBe(false)
    expect(error.operation).toBe("claimPending")
    expect(Schema.isSchemaError(error.cause)).toBe(true)
  })
})

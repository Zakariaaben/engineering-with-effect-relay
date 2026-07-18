import { describe, expect, it } from "bun:test"
import { Effect, Option, Schema } from "effect"
import {
  deliveryToRow,
  makeDeliveryRepositorySql,
} from "../src/deliveryRepositorySql.ts"
import {
  ClaimLostError,
} from "../src/errors.ts"
import {
  ClaimGeneration,
  DeliveryClaim,
  DeliveryResult,
  WorkerId,
} from "../src/model.ts"
import { delivery, event } from "./fixtures.ts"

const recoveryStatements = {
  findAttempts: () => Effect.succeed([]),
  recordAttempt: () => Effect.succeed([]),
  listDeadLetters: () => Effect.succeed([]),
  retryDeadLetter: () => Effect.succeed([]),
  claimPending: () => Effect.succeed([]),
  renewClaim: () => Effect.succeed([]),
  completeClaim: () => Effect.succeed([]),
  releaseClaim: () => Effect.succeed([]),
}

const workerId = WorkerId.make("wrk-repository-test")
const claim = DeliveryClaim.make({
  ownerId: workerId,
  generation: ClaimGeneration.make(3),
  leaseExpiresAtMillis: 30_000,
})

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
          dead_letter_reason: null,
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
          claim_lag_ms: 0,
          next_attempt_ordinal: 1,
          claim_owner: String(workerId),
          claim_generation: Number(claim.generation),
          lease_expires_at_ms: claim.leaseExpiresAtMillis,
        }])
      },
    })

    const claimed = await Effect.runPromise(
      repository.claimPending(
        workerId,
        delivery.destinationId,
        2,
        30_000,
      ),
    )

    expect(observedRequest).toEqual({
      owner_id: workerId,
      destination_id: delivery.destinationId,
      limit: 2,
      lease_duration_ms: 30_000,
    })
    expect(claimed).toEqual([{
      claim,
      claimLagMillis: 0,
      delivery,
      event,
      nextAttemptOrdinal: 1,
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
      Effect.flip(repository.claimPending(
        workerId,
        delivery.destinationId,
        0,
        30_000,
      )),
    )

    expect(queried).toBe(false)
    expect(error.operation).toBe("claimPending")
    expect(Schema.isSchemaError(error.cause)).toBe(true)
  })

  it("turns an empty fenced mutation into a typed claim-loss failure", async () => {
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([]),
    })
    const result = DeliveryResult.Delivered({
      attempts: [],
      deliveryId: delivery.id,
      destinationId: delivery.destinationId,
      status: 202,
    })

    const [renew, complete, release] = await Effect.runPromise(
      Effect.all([
        Effect.flip(repository.renewClaim(delivery.id, claim, 30_000)),
        Effect.flip(repository.completeClaim(delivery.id, claim, result)),
        Effect.flip(repository.releaseClaim(delivery.id, claim)),
      ]),
    )

    expect(renew).toBeInstanceOf(ClaimLostError)
    expect(renew.operation).toBe("renew")
    expect(complete).toBeInstanceOf(ClaimLostError)
    expect(complete.operation).toBe("complete")
    expect(release).toBeInstanceOf(ClaimLostError)
    expect(release.operation).toBe("release")
    if (!(complete instanceof ClaimLostError)) {
      throw new Error("expected a fenced completion failure")
    }
    expect(complete.generation).toBe(claim.generation)
  })
})

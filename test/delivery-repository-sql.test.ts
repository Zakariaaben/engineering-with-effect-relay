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
  ConfigurationVersion,
  DeliveryClaim,
  DeliveryRouteSnapshot,
  DeliveryResult,
  DestinationId,
  WorkerId,
} from "../src/model.ts"
import { delivery, destination, event } from "./fixtures.ts"

const recoveryStatements = {
  findAttempts: () => Effect.succeed([]),
  recordAttempt: () => Effect.succeed([]),
  listDeadLetters: () => Effect.succeed([]),
  retryDeadLetter: () => Effect.succeed([]),
  repairDeadLetter: () => Effect.succeed([]),
  terminateDeadLetter: () => Effect.succeed([]),
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

  it("encodes the current trusted route for a dead-letter repair", async () => {
    let observedRequest: unknown
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([{
        delivery_id: String(delivery.id),
        event_id: String(delivery.eventId),
        destination_id: String(delivery.destinationId),
        state: "DeadLettered",
        status: null,
        dead_letter_reason: "RetryBudgetExhausted",
      }]),
      repairDeadLetter: (request) => {
        observedRequest = request
        return Effect.succeed([{ delivery_id: String(delivery.id) }])
      },
    })
    const route = DeliveryRouteSnapshot.make({
      destinationId: destination.id,
      endpoint: new URL("https://current.example.test/invoices"),
      configurationVersion: ConfigurationVersion.make(2),
    })

    await Effect.runPromise(repository.repairDeadLetter(delivery.id, route))

    expect(observedRequest).toEqual({
      delivery_id: delivery.id,
      destination_id: destination.id,
      destination_url: "https://current.example.test/invoices",
      configuration_version: 2,
    })
  })

  it("reports a concurrent dead-letter action as a typed state conflict", async () => {
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([{
        delivery_id: String(delivery.id),
        event_id: String(delivery.eventId),
        destination_id: String(delivery.destinationId),
        state: "DeadLettered",
        status: null,
        dead_letter_reason: "RetryBudgetExhausted",
      }]),
      terminateDeadLetter: () => Effect.succeed([]),
    })

    const error = await Effect.runPromise(
      repository.terminateDeadLetter(delivery.id).pipe(Effect.flip),
    )

    if (error._tag !== "DeadLetterRecoveryError") {
      throw new Error("expected a dead-letter state conflict")
    }
    expect(error.reason).toBe("NotDeadLettered")
  })

  it("rejects repair to a different destination before mutation", async () => {
    let mutated = false
    const repository = makeDeliveryRepositorySql({
      ...recoveryStatements,
      save: () => Effect.void,
      findById: () => Effect.succeed([{
        delivery_id: String(delivery.id),
        event_id: String(delivery.eventId),
        destination_id: String(delivery.destinationId),
        state: "DeadLettered",
        status: null,
        dead_letter_reason: "RetryBudgetExhausted",
      }]),
      repairDeadLetter: () => {
        mutated = true
        return Effect.succeed([{ delivery_id: String(delivery.id) }])
      },
    })
    const route = DeliveryRouteSnapshot.make({
      destinationId: DestinationId.make("dst-other"),
      endpoint: new URL("https://other.example.test/invoices"),
      configurationVersion: ConfigurationVersion.make(1),
    })

    const error = await Effect.runPromise(
      repository.repairDeadLetter(delivery.id, route).pipe(Effect.flip),
    )

    if (error._tag !== "DeadLetterDestinationMismatchError") {
      throw new Error("expected a destination mismatch")
    }
    expect(error.deliveryDestinationId).toBe(delivery.destinationId)
    expect(error.repairDestinationId).toBe(route.destinationId)
    expect(mutated).toBe(false)
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

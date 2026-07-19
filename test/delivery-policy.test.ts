import { describe, expect, it } from "bun:test"
import {
  deliveryStateFromResult,
  terminalStateFromAttempt,
} from "../src/deliveryPolicy.ts"
import { deliveryCompletionRow } from "../src/adapters/postgres/deliveryRepository.ts"
import {
  ClaimGeneration,
  DeliveryAttemptRecord,
  DeliveryClaim,
  DeliveryOutcome,
  DeliveryResult,
  WorkerId,
  type DeliveryResult as DeliveryResultValue,
} from "../src/model.ts"
import { delivery, destination } from "./fixtures.ts"

const base = {
  deliveryId: delivery.id,
  destinationId: destination.id,
  attempts: [],
}

const results: ReadonlyArray<DeliveryResultValue> = [
  DeliveryResult.Delivered({ ...base, status: 202 }),
  DeliveryResult.Rejected({ ...base, status: 422 }),
  DeliveryResult.ProtocolFailure({ ...base, status: 103 }),
  DeliveryResult.Exhausted({
    ...base,
    lastOutcome: DeliveryOutcome.TransportFailure({
      destinationId: destination.id,
    }),
  }),
]

const claim = DeliveryClaim.make({
  ownerId: WorkerId.make("wrk-policy-test"),
  generation: ClaimGeneration.make(1),
  leaseExpiresAtMillis: 30_000,
})

describe("delivery state policy", () => {
  it("gives memory and PostgreSQL adapters the same terminal truth table", () => {
    const domainStates = results.map((result) =>
      deliveryStateFromResult(result)
    )
    const postgresStates = results.map((result) =>
      deliveryCompletionRow(delivery.id, claim, result)
    )

    expect(domainStates.map((state) => state._tag)).toEqual([
      "Delivered",
      "Rejected",
      "DeadLettered",
      "DeadLettered",
    ])
    expect(JSON.stringify(postgresStates.map((row) => row.state))).toBe(
      JSON.stringify(domainStates.map((state) => state._tag)),
    )
    expect(postgresStates.map((row) => row.dead_letter_reason)).toEqual([
      null,
      null,
      "ProviderProtocolFailure",
      "RetryBudgetExhausted",
    ])
  })

  it("interprets recorded terminal attempts through the same domain policy", () => {
    const attempt = DeliveryAttemptRecord.make({
      deliveryId: delivery.id,
      ordinal: 1,
      workerId: claim.ownerId,
      claimGeneration: claim.generation,
      startedAtMillis: 0,
      completedAtMillis: 10,
      outcome: "Delivered",
      decision: "Terminal",
      status: 202,
      retryDelayMillis: null,
      traceId: null,
      spanId: null,
    })

    expect(terminalStateFromAttempt(attempt)).toEqual(
      deliveryStateFromResult(
        DeliveryResult.Delivered({ ...base, status: 202 }),
      ),
    )
  })
})

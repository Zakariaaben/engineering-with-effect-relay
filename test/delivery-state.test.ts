import { describe, expect, it } from "bun:test"
import { Effect, Schema } from "effect"
import {
  DeliveryOutcome,
  DeliveryState,
  DestinationId,
  transitionDeliveryState,
} from "../src/model.ts"

const destinationId = DestinationId.make("dst-3")
const delivered = DeliveryOutcome.Delivered({
  destinationId,
  status: 202,
})
const rejected = DeliveryOutcome.Rejected({
  destinationId,
  status: 503,
})

describe("C02-04 delivery state", () => {
  it("decodes only declared boundary variants", async () => {
    const pending = await Effect.runPromise(
      Schema.decodeUnknownEffect(DeliveryState)({ _tag: "Pending" }),
    )
    const payloadError = await Effect.runPromise(
      Schema.decodeUnknownEffect(DeliveryState)({
        _tag: "Delivered",
        status: "202",
      }).pipe(Effect.flip),
    )
    const tagError = await Effect.runPromise(
      Schema.decodeUnknownEffect(DeliveryState)({
        _tag: "Scheduled",
      }).pipe(Effect.flip),
    )

    expect(pending).toEqual({ _tag: "Pending" })
    expect(payloadError).toBeInstanceOf(Schema.SchemaError)
    expect(payloadError.message).toContain("status")
    expect(tagError).toBeInstanceOf(Schema.SchemaError)
  })

  it("implements the complete transition table", () => {
    const pending = DeliveryState.cases.Pending.make({})
    const deliveredState = DeliveryState.cases.Delivered.make({ status: 202 })
    const rejectedState = DeliveryState.cases.Rejected.make({ status: 503 })

    const cases = [
      [pending, delivered, { _tag: "Delivered", status: 202 }],
      [pending, rejected, { _tag: "Rejected", status: 503 }],
      [deliveredState, delivered, deliveredState],
      [deliveredState, rejected, deliveredState],
      [rejectedState, delivered, rejectedState],
      [rejectedState, rejected, rejectedState],
    ] as const

    for (const [current, outcome, expected] of cases) {
      expect(transitionDeliveryState(current, outcome)).toEqual(expected)
    }
  })
})

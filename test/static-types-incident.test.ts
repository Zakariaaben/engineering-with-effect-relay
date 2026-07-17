import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClient } from "../src/destinationClient.ts"
import { sendDelivery } from "../src/effectSender.ts"
import { destination } from "./fixtures.ts"
import { unsafeParseRelayEvent } from "./incidents/unsafeJsonBoundary.ts"

describe("C02-01 static-type incident", () => {
  it("lets malformed JSON reach the client when no decoder runs", async () => {
    const malformedJson = JSON.stringify({
      id: 17,
      type: "invoice.created",
      invoiceId: null,
      amountCents: "12500",
    })
    const event = unsafeParseRelayEvent(malformedJson)
    let observedBody: string | undefined
    const client: DestinationClient = {
      post: ({ body }) => {
        observedBody = body
        return Promise.resolve(202)
      },
    }

    const outcome = await Effect.runPromise(
      sendDelivery(event, destination, client),
    )

    expect(outcome._tag).toBe("Delivered")
    expect(observedBody).toBe(malformedJson)
  })
})

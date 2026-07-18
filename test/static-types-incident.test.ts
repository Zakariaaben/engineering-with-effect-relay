import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClientService } from "../src/destinationClient.ts"
import { sendDelivery } from "../src/effectSender.ts"
import {
  delivery,
  destination,
  provideDestinationClient,
} from "./fixtures.ts"
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
    const client: DestinationClientService = {
      post: ({ body }) => {
        observedBody = body
        return Effect.succeed({ status: 202 })
      },
    }

    const outcome = await Effect.runPromise(
      sendDelivery(delivery.id, event, destination).pipe(
        provideDestinationClient(client),
      ),
    )

    expect(outcome._tag).toBe("Delivered")
    expect(observedBody).toBe(malformedJson)
  })
})

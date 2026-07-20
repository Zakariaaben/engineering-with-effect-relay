import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClientService } from "../src/destination.ts"
import { DeliveryTransportError } from "../src/errors.ts"
import { deliverCandidate } from "../src/delivery.ts"
import {
  delivery,
  destination,
  event,
  provideDestinationClient,
} from "./fixtures.ts"
import { confuseDeliveryFailure } from "./incidents/confusedFailure.ts"

describe("C02-07 confused-failure incident", () => {
  it("makes four different stops look like one provider response", async () => {
    const acceptedClient: DestinationClientService = {
      post: () => Effect.succeed({ status: 202 }),
    }
    const failedClient: DestinationClientService = {
      post: () => Effect.fail(
        new DeliveryTransportError({
          deliveryId: delivery.id,
          destinationId: destination.id,
          cause: new Error("connection reset"),
        }),
      ),
    }
    const invalid = deliverCandidate(
      delivery.id,
      { ...event, amountCents: "2500" },
      destination,
    ).pipe(provideDestinationClient(acceptedClient))
    const transport = deliverCandidate(
      delivery.id,
      event,
      destination,
    ).pipe(
      provideDestinationClient(failedClient),
    )
    const defect = Effect.die(new Error("broken invariant"))
    const interrupted = Effect.interrupt

    const outcomes = await Promise.all([
      Effect.runPromise(confuseDeliveryFailure(invalid, destination.id)),
      Effect.runPromise(confuseDeliveryFailure(transport, destination.id)),
      Effect.runPromise(confuseDeliveryFailure(defect, destination.id)),
      Effect.runPromise(confuseDeliveryFailure(interrupted, destination.id)),
    ])

    for (const outcome of outcomes) {
      expect(outcome).toEqual({
        _tag: "Rejected",
        destinationId: destination.id,
        status: 500,
      })
    }
  })
})

import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClientService } from "../src/destinationClient.ts"
import {
  DeliveryTransportError,
  InvalidEventError,
} from "../src/errors.ts"
import { deliverCandidate } from "../src/workflow.ts"
import {
  destination,
  event,
  provideDestinationClient,
} from "./fixtures.ts"

describe("C02-05 expected failures", () => {
  it("translates malformed input before the delivery client runs", async () => {
    let clientCalls = 0
    const client: DestinationClientService = {
      post: () => {
        clientCalls += 1
        return Promise.resolve(202)
      },
    }

    const failure = await Effect.runPromise(
      deliverCandidate(
        { ...event, amountCents: "2500" },
        destination,
      ).pipe(
        provideDestinationClient(client),
        Effect.flip,
      ),
    )

    expect(failure).toBeInstanceOf(InvalidEventError)
    expect(failure._tag).toBe("InvalidEventError")
    if (failure._tag !== "InvalidEventError") {
      throw new Error(`Expected InvalidEventError, got ${failure._tag}`)
    }
    expect(failure.summary).toContain("amountCents")
    expect(clientCalls).toBe(0)
  })

  it("preserves a transport failure as its own expected variant", async () => {
    const cause = Symbol("connection reset")
    const client: DestinationClientService = {
      post: () => Promise.reject(cause),
    }

    const failure = await Effect.runPromise(
      deliverCandidate(event, destination).pipe(
        provideDestinationClient(client),
        Effect.flip,
      ),
    )

    expect(failure).toEqual(
      new DeliveryTransportError({
        destinationId: destination.id,
        cause,
      }),
    )
  })

  it("keeps an observed provider rejection in the success value", async () => {
    const client: DestinationClientService = {
      post: () => Promise.resolve(503),
    }

    const outcome = await Effect.runPromise(
      deliverCandidate(event, destination).pipe(
        provideDestinationClient(client),
      ),
    )

    expect(outcome).toEqual({
      _tag: "Rejected",
      destinationId: destination.id,
      status: 503,
    })
  })
})

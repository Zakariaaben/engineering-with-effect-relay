import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClientService } from "../src/destination.ts"
import { runDelivery } from "../src/delivery.ts"
import { DeliveryTransportError } from "../src/errors.ts"
import {
  delivery,
  destination,
  event,
  makeGate,
  provideDestinationClient,
} from "./fixtures.ts"

describe("Relay M0 Effect workflow", () => {
  it("stays lazy until its execution boundary", async () => {
    const response = makeGate<number>()
    let starts = 0
    const client: DestinationClientService = {
      post: () => {
        return Effect.sync(() => {
          starts += 1
        }).pipe(
          Effect.andThen(Effect.promise(() => response.promise)),
          Effect.map((status) => ({ status })),
        )
      },
    }

    const program = runDelivery(
      delivery.id,
      event,
      destination,
    ).pipe(
      provideDestinationClient(client),
    )
    expect(starts).toBe(0)

    const run = Effect.runPromise(program)
    expect(starts).toBe(1)

    response.resolve(202)
    expect((await run)._tag).toBe("Delivered")
  })

  it("preserves a typed transport failure", async () => {
    const cause = Symbol("connection reset")
    const transportFailure = new DeliveryTransportError({
      deliveryId: delivery.id,
      destinationId: destination.id,
      cause,
    })
    const client: DestinationClientService = {
      post: () => Effect.fail(transportFailure),
    }

    const failure = await Effect.runPromise(
      runDelivery(delivery.id, event, destination).pipe(
        provideDestinationClient(client),
        Effect.flip,
      ),
    )

    expect(failure).toEqual(
      transportFailure,
    )
  })

  it("preserves host cancellation as interruption", async () => {
    const ready = makeGate<void>()
    const client: DestinationClientService = {
      post: () =>
        Effect.sync(() => ready.resolve(undefined)).pipe(
          Effect.andThen(Effect.never),
        ),
    }
    const controller = new AbortController()

    const run = Effect.runPromise(
      runDelivery(delivery.id, event, destination).pipe(
        provideDestinationClient(client),
      ),
      { signal: controller.signal },
    )
    await ready.promise
    controller.abort("stop Effect run")

    await expect(run).rejects.toBeDefined()
  })
})

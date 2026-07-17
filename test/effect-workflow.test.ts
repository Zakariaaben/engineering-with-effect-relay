import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClient } from "../src/destinationClient.ts"
import { sendDelivery } from "../src/effectSender.ts"
import { DeliveryTransportError } from "../src/errors.ts"
import {
  destination,
  event,
  makeGate,
} from "./fixtures.ts"

describe("Relay M0 Effect workflow", () => {
  it("stays lazy until its execution boundary", async () => {
    const response = makeGate<number>()
    let starts = 0
    const client: DestinationClient = {
      post: () => {
        starts += 1
        return response.promise
      },
    }

    const program = sendDelivery(event, destination, client)
    expect(starts).toBe(0)

    const run = Effect.runPromise(program)
    expect(starts).toBe(1)

    response.resolve(202)
    expect((await run)._tag).toBe("Delivered")
  })

  it("maps an arbitrary rejection to a typed transport failure", async () => {
    const cause = Symbol("connection reset")
    const client: DestinationClient = {
      post: () => Promise.reject(cause),
    }

    const failure = await Effect.runPromise(
      sendDelivery(event, destination, client).pipe(Effect.flip),
    )

    expect(failure).toEqual(
      new DeliveryTransportError({
        destinationId: destination.id,
        cause,
      }),
    )
  })

  it("forwards host cancellation to the client signal", async () => {
    const ready = makeGate<AbortSignal>()
    const client: DestinationClient = {
      post: ({ signal }) => {
        ready.resolve(signal)
        return new Promise<number>((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => reject(signal.reason),
            { once: true },
          )
        })
      },
    }
    const controller = new AbortController()

    const run = Effect.runPromise(
      sendDelivery(event, destination, client),
      { signal: controller.signal },
    )
    const clientSignal = await ready.promise
    controller.abort("stop Effect run")

    await expect(run).rejects.toBeDefined()
    expect(clientSignal.aborted).toBe(true)
  })
})

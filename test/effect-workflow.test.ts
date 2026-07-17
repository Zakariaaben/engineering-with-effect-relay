import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import type { DestinationClientService } from "../src/destinationClient.ts"
import { sendDelivery } from "../src/effectSender.ts"
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
        starts += 1
        return response.promise
      },
    }

    const program = sendDelivery(
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

  it("maps an arbitrary rejection to a typed transport failure", async () => {
    const cause = Symbol("connection reset")
    const client: DestinationClientService = {
      post: () => Promise.reject(cause),
    }

    const failure = await Effect.runPromise(
      sendDelivery(delivery.id, event, destination).pipe(
        provideDestinationClient(client),
        Effect.flip,
      ),
    )

    expect(failure).toEqual(
      new DeliveryTransportError({
        deliveryId: delivery.id,
        destinationId: destination.id,
        cause,
      }),
    )
  })

  it("forwards host cancellation to the client signal", async () => {
    const ready = makeGate<AbortSignal>()
    const client: DestinationClientService = {
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
      sendDelivery(delivery.id, event, destination).pipe(
        provideDestinationClient(client),
      ),
      { signal: controller.signal },
    )
    const clientSignal = await ready.promise
    controller.abort("stop Effect run")

    await expect(run).rejects.toBeDefined()
    expect(clientSignal.aborted).toBe(true)
  })
})

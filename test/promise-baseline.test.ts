import { describe, expect, it } from "bun:test"
import {
  makePromiseFetchDestinationClient,
  sendDeliveryWithPromise,
  type PromiseDestinationClient,
} from "../src/promiseSender.ts"
import {
  delivery,
  destination,
  event,
  makeGate,
} from "./fixtures.ts"

describe("Relay M0 Promise baseline", () => {
  it("starts the client when the async function is called", async () => {
    const response = makeGate<number>()
    let starts = 0
    const client: PromiseDestinationClient = {
      post: () => {
        starts += 1
        return response.promise
      },
    }
    const controller = new AbortController()

    const run = sendDeliveryWithPromise(
      delivery.id,
      event,
      destination,
      client,
      controller.signal,
    )

    expect(starts).toBe(1)
    response.resolve(202)
    expect((await run)._tag).toBe("Delivered")
  })

  it("can reject with an arbitrary JavaScript value", async () => {
    const cause = Symbol("connection reset")
    const client: PromiseDestinationClient = {
      post: () => Promise.reject(cause),
    }
    const controller = new AbortController()

    await expect(
      sendDeliveryWithPromise(
        delivery.id,
        event,
        destination,
        client,
        controller.signal,
      ),
    ).rejects.toBe(cause)
  })

  it("forwards cancellation owned by its caller", async () => {
    const ready = makeGate<AbortSignal>()
    const client: PromiseDestinationClient = {
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

    const run = sendDeliveryWithPromise(
      delivery.id,
      event,
      destination,
      client,
      controller.signal,
    )
    const clientSignal = await ready.promise
    controller.abort("stop Promise run")

    await expect(run).rejects.toBe("stop Promise run")
    expect(clientSignal.aborted).toBe(true)
  })

  it("discards the response body without exposing it", async () => {
    let bodyDiscarded = false
    const client = makePromiseFetchDestinationClient(async () => ({
      status: 400,
      body: {
        cancel: async () => {
          bodyDiscarded = true
        },
      },
    }))
    const controller = new AbortController()

    const outcome = await sendDeliveryWithPromise(
      delivery.id,
      event,
      destination,
      client,
      controller.signal,
    )

    expect(outcome).toEqual({
      _tag: "Rejected",
      destinationId: destination.id,
      status: 400,
    })
    expect(bodyDiscarded).toBe(true)
  })
})

import { describe, expect, it } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import type { DestinationClientService } from "../src/destinationClient.ts"
import {
  DeliveryTransportError,
  InvalidEventError,
} from "../src/errors.ts"
import { deliverCandidate } from "../src/workflow.ts"
import {
  destination,
  event,
  makeGate,
  provideDestinationClient,
} from "./fixtures.ts"

const failureCause = <A, E>(exit: Exit.Exit<A, E>): Cause.Cause<E> => {
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected a failed Exit")
  }
  return exit.cause
}

describe("Relay M1 act gate", () => {
  it("rejects invalid input before outbound work", async () => {
    let clientCalls = 0
    const client: DestinationClientService = {
      post: () => {
        clientCalls += 1
        return Promise.resolve(202)
      },
    }

    const exit = await Effect.runPromiseExit(
      deliverCandidate(
        { ...event, amountCents: "2500" },
        destination,
      ).pipe(provideDestinationClient(client)),
    )
    const cause = failureCause(exit)
    const reason = cause.reasons.find(Cause.isFailReason)

    expect(reason?.error).toBeInstanceOf(InvalidEventError)
    expect(Cause.hasDies(cause)).toBe(false)
    expect(clientCalls).toBe(0)
  })

  it("keeps client rejection as a transport failure", async () => {
    const transportCause = Symbol("connection reset")
    const client: DestinationClientService = {
      post: () => Promise.reject(transportCause),
    }

    const exit = await Effect.runPromiseExit(
      deliverCandidate(event, destination).pipe(
        provideDestinationClient(client),
      ),
    )
    const cause = failureCause(exit)
    const reason = cause.reasons.find(Cause.isFailReason)

    expect(reason?.error).toEqual(
      new DeliveryTransportError({
        destinationId: destination.id,
        cause: transportCause,
      }),
    )
  })

  it("keeps an HTTP rejection as an observed outcome", async () => {
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

  it("keeps a downstream invariant violation as a defect", async () => {
    const defect = new Error("broken invariant")
    const client: DestinationClientService = {
      post: () => Promise.resolve(202),
    }
    const program = deliverCandidate(event, destination).pipe(
      provideDestinationClient(client),
      Effect.flatMap(() => Effect.die(defect)),
    )

    const exit = await Effect.runPromiseExit(program)
    const cause = failureCause(exit)
    const reason = cause.reasons.find(Cause.isDieReason)

    expect(reason?.defect).toBe(defect)
    expect(Cause.hasFails(cause)).toBe(false)
  })

  it("keeps owner cancellation as interruption", async () => {
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

    const run = Effect.runPromiseExit(
      deliverCandidate(event, destination).pipe(
        provideDestinationClient(client),
      ),
      { signal: controller.signal },
    )
    const clientSignal = await ready.promise
    controller.abort("stop M1")
    const exit = await run
    const cause = failureCause(exit)

    expect(Cause.hasInterrupts(cause)).toBe(true)
    expect(Cause.hasFails(cause)).toBe(false)
    expect(clientSignal.aborted).toBe(true)
  })
})

import { describe, expect, it } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import type { DestinationClientService } from "../src/destinationClient.ts"
import { InvalidEventError } from "../src/errors.ts"
import { deliverCandidate } from "../src/workflow.ts"
import {
  delivery,
  destination,
  event,
  provideDestinationClient,
} from "./fixtures.ts"

const failureCause = <A, E>(exit: Exit.Exit<A, E>): Cause.Cause<E> => {
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected a failed Exit")
  }
  return exit.cause
}

describe("C02-06 Cause and Exit", () => {
  it("keeps typed failure, defect, and interruption distinguishable", async () => {
    const client: DestinationClientService = {
      post: () => Effect.succeed({ status: 202 }),
    }
    const expectedExit = await Effect.runPromiseExit(
      deliverCandidate(
        delivery.id,
        { ...event, amountCents: "2500" },
        destination,
      ).pipe(provideDestinationClient(client)),
    )
    const defectExit = await Effect.runPromiseExit(
      Effect.sync(() => {
        throw new Error("broken invariant")
      }),
    )
    const interruptExit = await Effect.runPromiseExit(Effect.interrupt)

    const expectedCause = failureCause(expectedExit)
    const defectCause = failureCause(defectExit)
    const interruptCause = failureCause(interruptExit)

    expect(Cause.hasFails(expectedCause)).toBe(true)
    expect(Cause.hasDies(expectedCause)).toBe(false)
    expect(Cause.hasDies(defectCause)).toBe(true)
    expect(Cause.hasInterrupts(interruptCause)).toBe(true)
    expect(expectedCause.reasons.map((reason) => reason._tag)).toEqual(["Fail"])
    expect(defectCause.reasons.map((reason) => reason._tag)).toEqual(["Die"])
    expect(interruptCause.reasons.map((reason) => reason._tag)).toEqual(["Interrupt"])
  })

  it("shows what Promise rejection loses when a Cause is squashed", async () => {
    const invalid = new InvalidEventError({ summary: "invalid event" })
    const defect = new Error("broken invariant")
    const combined = Cause.combine(
      Cause.fail(invalid),
      Cause.die(defect),
    )
    const program = Effect.failCause(combined)

    const exit = await Effect.runPromiseExit(program)
    const cause = failureCause(exit)

    expect(cause.reasons.map((reason) => reason._tag)).toEqual([
      "Fail",
      "Die",
    ])
    expect(Cause.hasFails(cause)).toBe(true)
    expect(Cause.hasDies(cause)).toBe(true)
    await expect(Effect.runPromise(program)).rejects.toBe(invalid)
  })
})

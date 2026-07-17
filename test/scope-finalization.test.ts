import { describe, expect, it } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { makeGate } from "./fixtures.ts"

type FinalizerExit = "success" | "failure" | "interruption"

const classifyFinalizerExit = (
  exit: Exit.Exit<unknown, unknown>,
): FinalizerExit => {
  if (Exit.isSuccess(exit)) {
    return "success"
  }
  return Cause.hasInterrupts(exit.cause) ? "interruption" : "failure"
}

const observedConnection = (events: Array<string>) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      events.push("acquire")
      return { name: "relay-db" }
    }),
    (_connection, exit) =>
      Effect.sync(() => {
        events.push(`release:${classifyFinalizerExit(exit)}`)
      }),
  )

describe("C03-04 scoped finalization", () => {
  it("releases after successful use", async () => {
    const events: Array<string> = []
    const program = Effect.scoped(
      Effect.gen(function* () {
        yield* observedConnection(events)
        events.push("use")
        return "delivered"
      }),
    )

    expect(await Effect.runPromise(program)).toBe("delivered")
    expect(events).toEqual(["acquire", "use", "release:success"])
  })

  it("releases after typed failure without replacing the failure", async () => {
    const events: Array<string> = []
    const program = Effect.scoped(
      Effect.gen(function* () {
        yield* observedConnection(events)
        events.push("use")
        return yield* Effect.fail("DeliveryFailed" as const)
      }),
    )

    const exit = await Effect.runPromiseExit(program)

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      throw new Error("Expected the delivery to fail")
    }
    expect(Cause.hasFails(exit.cause)).toBe(true)
    expect(events).toEqual(["acquire", "use", "release:failure"])
  })

  it("releases after owner interruption", async () => {
    const events: Array<string> = []
    const useStarted = makeGate<void>()
    const controller = new AbortController()
    const program = Effect.scoped(
      Effect.gen(function* () {
        yield* observedConnection(events)
        yield* Effect.sync(() => {
          events.push("use")
          useStarted.resolve()
        })
        return yield* Effect.never
      }),
    )

    const run = Effect.runPromiseExit(program, { signal: controller.signal })
    await useStarted.promise
    controller.abort("stop Relay")
    const exit = await run

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      throw new Error("Expected the delivery to be interrupted")
    }
    expect(Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(events).toEqual(["acquire", "use", "release:interruption"])
  })
})

import { describe, expect, it } from "bun:test"
import { Cause, Context, Effect, Exit, Layer } from "effect"
import { reproducePartialAcquisitionLeak } from "./incidents/partialAcquisitionLeak.ts"

class ConnectionPool extends Context.Service<ConnectionPool, {
  readonly name: string
}>()("Relay/M2/ConnectionPool") {}

class DestinationSession extends Context.Service<DestinationSession, {
  readonly name: string
}>()("Relay/M2/DestinationSession") {}

describe("Relay M2 act gate", () => {
  it("reproduces a leak when the second manual acquisition fails", async () => {
    expect(await reproducePartialAcquisitionLeak()).toEqual({
      events: [
        "pool:acquire",
        "session:acquire",
        "startup:failure",
      ],
      poolOpen: true,
    })
  })

  it("releases an acquired Layer dependency when its consumer fails to build", async () => {
    const events: Array<string> = []
    const ConnectionPoolLive = Layer.effect(
      ConnectionPool,
      Effect.acquireRelease(
        Effect.sync(() => {
          events.push("pool:acquire")
          return ConnectionPool.of({ name: "destination-pool" })
        }),
        () => Effect.sync(() => {
          events.push("pool:release")
        }),
      ),
    )
    const DestinationSessionLive = Layer.effect(
      DestinationSession,
      Effect.gen(function* () {
        yield* ConnectionPool
        yield* Effect.sync(() => {
          events.push("session:acquire")
        })
        return yield* Effect.fail("session unavailable" as const)
      }),
    )
    const graph = DestinationSessionLive.pipe(
      Layer.provide(ConnectionPoolLive),
    )

    const exit = await Effect.runPromiseExit(
      Effect.scoped(Layer.build(graph)),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      throw new Error("Expected Layer construction to fail")
    }
    expect(Cause.hasFails(exit.cause)).toBe(true)
    expect(events).toEqual([
      "pool:acquire",
      "session:acquire",
      "pool:release",
    ])
  })
})

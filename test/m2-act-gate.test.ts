import { describe, expect, it } from "bun:test"
import { Cause, Context, Effect, Exit, Layer } from "effect"
import { reproducePartialAcquisitionLeak } from "./incidents/partialAcquisitionLeak.ts"

class RepositoryConnection extends Context.Service<RepositoryConnection, {
  readonly name: string
}>()("C03-09/RepositoryConnection") {}

class DestinationConnection extends Context.Service<DestinationConnection, {
  readonly name: string
}>()("C03-09/DestinationConnection") {}

describe("Relay M2 act gate", () => {
  it("reproduces a leak when the second manual acquisition fails", async () => {
    expect(await reproducePartialAcquisitionLeak()).toEqual({
      events: [
        "repository:acquire",
        "destination:acquire",
        "startup:failure",
      ],
      repositoryOpen: true,
    })
  })

  it("releases an acquired Layer dependency when its consumer fails to build", async () => {
    const events: Array<string> = []
    const RepositoryLive = Layer.effect(
      RepositoryConnection,
      Effect.acquireRelease(
        Effect.sync(() => {
          events.push("repository:acquire")
          return RepositoryConnection.of({ name: "relay-repository" })
        }),
        () => Effect.sync(() => {
          events.push("repository:release")
        }),
      ),
    )
    const DestinationLive = Layer.effect(
      DestinationConnection,
      Effect.gen(function* () {
        yield* RepositoryConnection
        yield* Effect.sync(() => {
          events.push("destination:acquire")
        })
        return yield* Effect.fail("destination unavailable" as const)
      }),
    )
    const graph = DestinationLive.pipe(
      Layer.provideMerge(RepositoryLive),
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
      "repository:acquire",
      "destination:acquire",
      "repository:release",
    ])
  })
})

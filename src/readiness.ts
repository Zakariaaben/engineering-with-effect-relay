import { Context, Effect, Layer, Ref } from "effect"

export class RelayReadiness extends Context.Service<RelayReadiness, {
  readonly current: Effect.Effect<boolean>
  readonly markReady: Effect.Effect<void>
  readonly markNotReady: Effect.Effect<void>
}>()("Relay/Readiness") {}

export const RelayReadinessLive = Layer.effect(
  RelayReadiness,
  Effect.gen(function* () {
    const ready = yield* Ref.make(false)
    return RelayReadiness.of({
      current: Ref.get(ready),
      markReady: Ref.set(ready, true),
      markNotReady: Ref.set(ready, false),
    })
  }),
)

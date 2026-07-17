import {
  Context,
  Effect,
  Layer,
  PubSub,
  Stream,
} from "effect"
import type { DeliveryResult } from "./model.ts"

export class DeliveryEvents extends Context.Service<DeliveryEvents, {
  readonly publish: (
    result: DeliveryResult,
  ) => Effect.Effect<void>
  readonly results: Stream.Stream<DeliveryResult>
}>()("Relay/DeliveryEvents") {}

export const DeliveryEventsLive = Layer.effect(
  DeliveryEvents,
  Effect.gen(function* () {
    const pubsub = yield* Effect.acquireRelease(
      PubSub.unbounded<DeliveryResult>(),
      PubSub.shutdown,
    )
    const publish = Effect.fn("DeliveryEvents.publish")(
      (result: DeliveryResult) =>
        PubSub.publish(pubsub, result).pipe(Effect.asVoid),
    )

    return DeliveryEvents.of({
      publish,
      results: Stream.fromPubSub(pubsub),
    })
  }),
)

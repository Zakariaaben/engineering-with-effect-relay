import {
  Context,
  Effect,
  Layer,
  PubSub,
  Stream,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import type { DeliveryResult } from "./delivery.ts"

export class DeliveryEvents extends Context.Service<DeliveryEvents, {
  readonly publish: (
    result: DeliveryResult,
  ) => Effect.Effect<void>
  readonly results: Stream.Stream<DeliveryResult>
}>()("Relay/DeliveryEvents") {}

export const DeliveryEventsLive = Layer.effect(
  DeliveryEvents,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const pubsub = yield* Effect.acquireRelease(
      PubSub.bounded<DeliveryResult>(
        configuration.flow.deliveryEventsCapacity,
      ),
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

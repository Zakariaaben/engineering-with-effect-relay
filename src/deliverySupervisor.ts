import {
  Context,
  Effect,
  Fiber,
  FiberSet,
  Layer,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import { DestinationClient } from "./destinationClient.ts"
import type {
  DeliveryTransportError,
  InvalidEventError,
} from "./errors.ts"
import type { DeliveryOutcome } from "./model.ts"
import { deliverCandidate } from "./workflow.ts"

type DeliveryFailure = InvalidEventError | DeliveryTransportError

export class DeliverySupervisor extends Context.Service<DeliverySupervisor, {
  readonly deliver: (
    candidate: unknown,
  ) => Effect.Effect<DeliveryOutcome, DeliveryFailure>
  readonly activeCount: () => Effect.Effect<number>
}>()("Relay/DeliverySupervisor") {}

export const DeliverySupervisorLive = Layer.effect(
  DeliverySupervisor,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const destinationClient = yield* DestinationClient
    const deliveries = yield* FiberSet.make<
      DeliveryOutcome,
      DeliveryFailure
    >()

    const deliver = Effect.fn("DeliverySupervisor.deliver")(
      function* (candidate: unknown) {
        const task = deliverCandidate(
          candidate,
          configuration.destination,
        ).pipe(
          Effect.provideService(
            DestinationClient,
            destinationClient,
          ),
        )
        const fiber = yield* FiberSet.run(deliveries, task)

        return yield* Fiber.join(fiber).pipe(
          Effect.onInterrupt(() =>
            Fiber.interrupt(fiber).pipe(Effect.asVoid)
          ),
        )
      },
    )
    const activeCount = Effect.fn(
      "DeliverySupervisor.activeCount",
    )(function* () {
      return yield* FiberSet.size(deliveries)
    })

    return DeliverySupervisor.of({ activeCount, deliver })
  }),
)

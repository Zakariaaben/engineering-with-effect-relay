import {
  Context,
  Crypto,
  Duration,
  Effect,
  Fiber,
  FiberSet,
  Layer,
  Ref,
  Semaphore,
  SynchronizedRef,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import {
  observeDeliveryAttempt,
  runDeliveryWithRetry,
} from "./deliveryEngine.ts"
import { DestinationClient } from "./destinationClient.ts"
import {
  DeliveryIdentityError,
  type InvalidEventError,
} from "./errors.ts"
import { sendDelivery } from "./effectSender.ts"
import { generateDeliveryId } from "./identifiers.ts"
import type {
  DeliveryResult,
  Destination,
  DestinationId,
} from "./model.ts"
import { decodeIncomingEvent } from "./workflow.ts"

type DeliveryFailure =
  | InvalidEventError
  | DeliveryIdentityError

export interface DeliveryConcurrencyMetrics {
  readonly globalActive: number
  readonly activeByDestination: ReadonlyMap<DestinationId, number>
}

export class DeliverySupervisor extends Context.Service<DeliverySupervisor, {
  readonly deliver: (
    candidate: unknown,
  ) => Effect.Effect<DeliveryResult, DeliveryFailure>
  readonly deliverTo: (
    candidate: unknown,
    destination: Destination,
  ) => Effect.Effect<DeliveryResult, DeliveryFailure>
  readonly activeCount: () => Effect.Effect<number>
  readonly concurrencyMetrics: () => Effect.Effect<DeliveryConcurrencyMetrics>
}>()("Relay/DeliverySupervisor") {}

export const DeliverySupervisorLive = Layer.effect(
  DeliverySupervisor,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const crypto = yield* Crypto.Crypto
    const destinationClient = yield* DestinationClient
    const deliveries = yield* FiberSet.make<
      DeliveryResult,
      DeliveryFailure
    >()
    const globalCapacity = yield* Semaphore.make(
      configuration.concurrency.global,
    )
    const destinationCapacities = yield* SynchronizedRef.make(
      new Map<DestinationId, Semaphore.Semaphore>(),
    )
    const active = yield* Ref.make<DeliveryConcurrencyMetrics>({
      globalActive: 0,
      activeByDestination: new Map(),
    })

    const capacityFor = Effect.fn("DeliverySupervisor.capacityFor")(
      function* (destinationId: DestinationId) {
        return yield* SynchronizedRef.modifyEffect(
          destinationCapacities,
          (capacities) => {
            const existing = capacities.get(destinationId)
            if (existing !== undefined) {
              return Effect.succeed([existing, capacities] as const)
            }

            return Semaphore.make(
              configuration.concurrency.perDestination,
            ).pipe(
              Effect.map((created) => {
                const updated = new Map(capacities)
                updated.set(destinationId, created)
                return [created, updated] as const
              }),
            )
          },
        )
      },
    )

    const adjustActive = Effect.fn("DeliverySupervisor.adjustActive")(
      (destinationId: DestinationId, adjustment: 1 | -1) =>
        Ref.update(active, (current) => {
          const activeByDestination = new Map(
            current.activeByDestination,
          )
          const destinationActive =
            (activeByDestination.get(destinationId) ?? 0) + adjustment

          if (destinationActive === 0) {
            activeByDestination.delete(destinationId)
          } else {
            activeByDestination.set(destinationId, destinationActive)
          }

          return {
            globalActive: current.globalActive + adjustment,
            activeByDestination,
          }
        }),
    )

    const trackActive = <A, E, R>(
      destinationId: DestinationId,
      task: Effect.Effect<A, E, R>,
    ) =>
      Effect.acquireUseRelease(
        adjustActive(destinationId, 1),
        () => task,
        () => adjustActive(destinationId, -1),
      )

    const withCapacity = Effect.fn("DeliverySupervisor.withCapacity")(
      function* <A, E, R>(
        destinationId: DestinationId,
        task: Effect.Effect<A, E, R>,
      ) {
        const destinationCapacity = yield* capacityFor(destinationId)

        return yield* destinationCapacity.withPermit(
          globalCapacity.withPermit(
            trackActive(destinationId, task),
          ),
        )
      },
    )

    const deliverTo = Effect.fn("DeliverySupervisor.deliverTo")(
      function* (candidate: unknown, destination: Destination) {
        const event = yield* decodeIncomingEvent(candidate)
        const deliveryId = yield* generateDeliveryId().pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            new DeliveryIdentityError({
              destinationId: destination.id,
              cause,
            })
          ),
        )
        const task = runDeliveryWithRetry(
          deliveryId,
          destination.id,
          configuration.resilience,
          (ordinal, remaining) =>
            withCapacity(
              destination.id,
              observeDeliveryAttempt(
                ordinal,
                destination.id,
                Duration.min(
                  configuration.resilience.attemptTimeout,
                  remaining,
                ),
                sendDelivery(
                  deliveryId,
                  event,
                  destination,
                ).pipe(
                  Effect.provideService(
                    DestinationClient,
                    destinationClient,
                  ),
                ),
              ),
            ),
        )
        const fiber = yield* FiberSet.run(
          deliveries,
          task,
        )

        return yield* Fiber.join(fiber).pipe(
          Effect.onInterrupt(() =>
            Fiber.interrupt(fiber).pipe(Effect.asVoid)
          ),
        )
      },
    )
    const deliver = Effect.fn("DeliverySupervisor.deliver")(
      (candidate: unknown) =>
        deliverTo(candidate, configuration.destination),
    )
    const activeCount = Effect.fn(
      "DeliverySupervisor.activeCount",
    )(function* () {
      return yield* FiberSet.size(deliveries)
    })
    const concurrencyMetrics = Effect.fn(
      "DeliverySupervisor.concurrencyMetrics",
    )(function* () {
      return yield* Ref.get(active)
    })

    return DeliverySupervisor.of({
      activeCount,
      concurrencyMetrics,
      deliver,
      deliverTo,
    })
  }),
)

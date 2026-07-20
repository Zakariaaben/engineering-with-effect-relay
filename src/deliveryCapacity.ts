import {
  Context,
  Effect,
  Ref,
  Semaphore,
  SynchronizedRef,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import { makeDeliveryMetrics } from "./deliveryMetrics.ts"
import type { DestinationId } from "./identifiers.ts"

export interface DeliveryConcurrencyMetrics {
  readonly globalActive: number
  readonly activeByDestination: ReadonlyMap<DestinationId, number>
}

interface DeliveryCapacityOptions {
  readonly configuration: Context.Service.Shape<typeof AppConfiguration>
  readonly metrics: ReturnType<typeof makeDeliveryMetrics>
}

export const makeDeliveryCapacity = Effect.fn("DeliveryCapacity.make")(
  function* ({ configuration, metrics }: DeliveryCapacityOptions) {
    const global = yield* Semaphore.make(configuration.concurrency.global)
    const destinations = yield* SynchronizedRef.make(
      new Map<DestinationId, Semaphore.Semaphore>(),
    )
    const active = yield* Ref.make<DeliveryConcurrencyMetrics>({
      globalActive: 0,
      activeByDestination: new Map(),
    })

    const capacityFor = Effect.fn("DeliveryCapacity.capacityFor")(
      function* (destinationId: DestinationId) {
        return yield* SynchronizedRef.modifyEffect(
          destinations,
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

    const adjustActive = (
      destinationId: DestinationId,
      adjustment: 1 | -1,
    ) =>
      Ref.updateAndGet(active, (current) => {
        const activeByDestination = new Map(current.activeByDestination)
        const count =
          (activeByDestination.get(destinationId) ?? 0) + adjustment
        if (count === 0) activeByDestination.delete(destinationId)
        else activeByDestination.set(destinationId, count)
        return {
          globalActive: current.globalActive + adjustment,
          activeByDestination,
        }
      }).pipe(
        Effect.tap((current) => metrics.setActiveAttempts(current.globalActive)),
      )

    const withAttempt = Effect.fn("DeliveryCapacity.withAttempt")(
      function* <A, E, R>(
        destinationId: DestinationId,
        attempt: Effect.Effect<A, E, R>,
      ) {
        const destination = yield* capacityFor(destinationId)
        return yield* destination.withPermit(
          global.withPermit(
            Effect.acquireUseRelease(
              adjustActive(destinationId, 1),
              () => attempt,
              () => adjustActive(destinationId, -1),
            ),
          ),
        )
      },
    )

    return {
      metrics: Ref.get(active),
      withAttempt,
    }
  },
)

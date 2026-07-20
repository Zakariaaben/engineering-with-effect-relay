import {
  Context,
  Effect,
  Option,
  Ref,
  Semaphore,
  SynchronizedRef,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import type { DeliveryConcurrencyMetrics } from "./deliveryCapacity.ts"
import { makeDeliveryMetrics } from "./deliveryMetrics.ts"
import { DeliveryOverloaded } from "./errors.ts"
import type { DestinationId } from "./identifiers.ts"

export interface DeliveryLoadMetrics extends DeliveryConcurrencyMetrics {
  readonly activeDeliveries: number
  readonly admittedByDestination: ReadonlyMap<DestinationId, number>
  readonly admittedDeliveries: number
  readonly globalConcurrencyLimit: number
  readonly perDestinationAdmissionCapacity: number
  readonly perDestinationConcurrencyLimit: number
  readonly rejected: number
  readonly requestQueueCapacity: number
  readonly requestQueueDepth: number
}

interface DeliveryAdmissionOptions {
  readonly configuration: Context.Service.Shape<typeof AppConfiguration>
  readonly concurrencyMetrics: Effect.Effect<DeliveryConcurrencyMetrics>
  readonly metrics: ReturnType<typeof makeDeliveryMetrics>
}

interface DeliveryLoadObservation {
  readonly activeDeliveries: number
  readonly requestQueueDepth: number
}

export const makeDeliveryAdmission = Effect.fn("DeliveryAdmission.make")(
  function* ({
    configuration,
    concurrencyMetrics,
    metrics,
  }: DeliveryAdmissionOptions) {
    const globalAdmission = yield* Semaphore.make(
      configuration.flow.deliveryRequestsCapacity,
    )
    const destinationAdmissions = yield* SynchronizedRef.make(
      new Map<DestinationId, Semaphore.Semaphore>(),
    )
    const admitted = yield* Ref.make({
      total: 0,
      byDestination: new Map<DestinationId, number>(),
    })
    const rejected = yield* Ref.make(0)

    const semaphoreFor = Effect.fn("DeliveryAdmission.semaphoreFor")(
      function* (
        registry: SynchronizedRef.SynchronizedRef<
          Map<DestinationId, Semaphore.Semaphore>
        >,
        destinationId: DestinationId,
        permits: number,
      ) {
        return yield* SynchronizedRef.modifyEffect(
          registry,
          (semaphores) => {
            const existing = semaphores.get(destinationId)
            if (existing !== undefined) {
              return Effect.succeed([existing, semaphores] as const)
            }
            return Semaphore.make(permits).pipe(
              Effect.map((created) => {
                const updated = new Map(semaphores)
                updated.set(destinationId, created)
                return [created, updated] as const
              }),
            )
          },
        )
      },
    )

    const overload = (
      destinationId: DestinationId,
      limit: "GlobalAdmission" | "DestinationAdmission",
    ) =>
      Effect.all([
        Ref.update(rejected, (count) => count + 1),
        metrics.recordAdmissionRejection,
      ], { discard: true }).pipe(
        Effect.andThen(
          Effect.fail(new DeliveryOverloaded({
            admissionCapacity: limit === "GlobalAdmission"
              ? configuration.flow.deliveryRequestsCapacity
              : configuration.flow.deliveryRequestsPerDestinationCapacity,
            destinationId,
            limit,
          })),
        ),
      )

    const adjustAdmitted = (
      destinationId: DestinationId,
      adjustment: 1 | -1,
    ) =>
      Ref.updateAndGet(admitted, (current) => {
        const byDestination = new Map(current.byDestination)
        const count = (byDestination.get(destinationId) ?? 0) + adjustment
        if (count === 0) byDestination.delete(destinationId)
        else byDestination.set(destinationId, count)
        return { total: current.total + adjustment, byDestination }
      }).pipe(
        Effect.tap((current) => metrics.setAdmittedDeliveries(current.total)),
      )

    const admit = Effect.fn("DeliveryAdmission.admit")(
      function* <A, E, R>(
        destinationId: DestinationId,
        task: Effect.Effect<A, E, R>,
      ) {
        const destinationAdmission = yield* semaphoreFor(
          destinationAdmissions,
          destinationId,
          configuration.flow.deliveryRequestsPerDestinationCapacity,
        )
        const acceptedByDestination = yield*
          destinationAdmission.withPermitsIfAvailable(1)(
            globalAdmission.withPermitsIfAvailable(1)(
              Effect.acquireUseRelease(
                adjustAdmitted(destinationId, 1),
                () => task,
                () => adjustAdmitted(destinationId, -1),
              ),
            ),
          )

        if (Option.isNone(acceptedByDestination)) {
          return yield* overload(destinationId, "DestinationAdmission")
        }
        return Option.isSome(acceptedByDestination.value)
          ? acceptedByDestination.value.value
          : yield* overload(destinationId, "GlobalAdmission")
      },
    )

    const loadMetrics = Effect.fn("DeliveryAdmission.loadMetrics")(
      function* (observation: DeliveryLoadObservation) {
        const concurrency = yield* concurrencyMetrics
        const admission = yield* Ref.get(admitted)
        return {
          ...concurrency,
          ...observation,
          admittedByDestination: admission.byDestination,
          admittedDeliveries: admission.total,
          globalConcurrencyLimit: configuration.concurrency.global,
          perDestinationAdmissionCapacity:
            configuration.flow.deliveryRequestsPerDestinationCapacity,
          perDestinationConcurrencyLimit:
            configuration.concurrency.perDestination,
          rejected: yield* Ref.get(rejected),
          requestQueueCapacity: configuration.flow.deliveryRequestsCapacity,
        } satisfies DeliveryLoadMetrics
      },
    )

    return {
      admit,
      loadMetrics,
      reject: overload,
    }
  },
)

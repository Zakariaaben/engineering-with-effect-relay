import {
  Clock,
  Context,
  Crypto,
  Deferred,
  Duration,
  Effect,
  FiberSet,
  Layer,
  Option,
  Queue,
  Random,
  Ref,
  Semaphore,
  Stream,
  SynchronizedRef,
  Tracer,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import {
  observeDeliveryAttempt,
  runDeliveryWithRetry,
} from "./deliveryEngine.ts"
import { DeliveryEvents } from "./deliveryEvents.ts"
import { DestinationClient } from "./destinationClient.ts"
import {
  DeliveryIdentityError,
  DeliveryOverloaded,
  type InvalidEventError,
  type RelayIntakeStoreError,
} from "./errors.ts"
import { sendDelivery } from "./effectSender.ts"
import { generateDeliveryId } from "./identifiers.ts"
import type {
  Delivery,
  DeliveryId,
  DeliveryResult,
  Destination,
  DestinationId,
  RelayEvent,
} from "./model.ts"
import { RelayIntakeStore } from "./services.ts"
import { decodeIncomingEvent } from "./workflow.ts"

type DeliveryExecutionFailure =
  | InvalidEventError
  | DeliveryIdentityError

type DeliveryFailure =
  | DeliveryExecutionFailure
  | DeliveryOverloaded
  | RelayIntakeStoreError

interface DeliveryJob {
  readonly cancelled: Deferred.Deferred<void>
  readonly clock: Clock.Clock
  readonly deliveryId: DeliveryId
  readonly destination: Destination
  readonly event: RelayEvent
  readonly parentSpan: Option.Option<Tracer.AnySpan>
  readonly random: Context.Service.Shape<typeof Random.Random>
  readonly result: Deferred.Deferred<DeliveryResult>
}

export interface DeliverySupervisorHooks {
  readonly afterIntakeCommit?: (
    delivery: Delivery,
  ) => Effect.Effect<void>
}

export interface DeliveryConcurrencyMetrics {
  readonly globalActive: number
  readonly activeByDestination: ReadonlyMap<DestinationId, number>
}

export interface DeliveryLoadMetrics extends DeliveryConcurrencyMetrics {
  readonly activeDeliveries: number
  readonly admittedDeliveries: number
  readonly globalConcurrencyLimit: number
  readonly perDestinationConcurrencyLimit: number
  readonly rejected: number
  readonly requestQueueCapacity: number
  readonly requestQueueDepth: number
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
  readonly loadMetrics: () => Effect.Effect<DeliveryLoadMetrics>
}>()("Relay/DeliverySupervisor") {}

export const makeDeliverySupervisorLive = (
  hooks: DeliverySupervisorHooks = {},
) => Layer.effect(
  DeliverySupervisor,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const crypto = yield* Crypto.Crypto
    const destinationClient = yield* DestinationClient
    const deliveryEvents = yield* DeliveryEvents
    const intakeStore = yield* RelayIntakeStore
    const deliveries = yield* FiberSet.make<void>()
    const requests = yield* Effect.acquireRelease(
      Queue.dropping<DeliveryJob>(
        configuration.flow.deliveryRequestsCapacity,
      ),
      Queue.shutdown,
    )
    const admission = yield* Semaphore.make(
      configuration.flow.deliveryRequestsCapacity,
    )
    const admitted = yield* Ref.make(0)
    const rejected = yield* Ref.make(0)
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

    const executeDelivery = Effect.fn("DeliverySupervisor.executeDelivery")(
      function* (
        deliveryId: DeliveryId,
        event: RelayEvent,
        destination: Destination,
      ) {
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
        return yield* task.pipe(
          Effect.annotateLogs({
            "relay.event_id": event.id,
            "relay.delivery_id": deliveryId,
            "relay.destination_id": destination.id,
          }),
          Effect.tap(deliveryEvents.publish),
        )
      },
    )

    const processJob = Effect.fn("DeliverySupervisor.processJob")(
      function* (job: DeliveryJob) {
        yield* Effect.annotateCurrentSpan({
          "relay.event_id": job.event.id,
          "relay.delivery_id": job.deliveryId,
          "relay.destination_id": job.destination.id,
        })
        const wasCancelled = yield* Deferred.isDone(job.cancelled)
        const exit = yield* (
          wasCancelled
            ? Effect.interrupt
            : Effect.raceFirst(
                executeDelivery(
                  job.deliveryId,
                  job.event,
                  job.destination,
                ).pipe(
                  Effect.provideService(Clock.Clock, job.clock),
                  Effect.provideService(Random.Random, job.random),
                ),
                Deferred.await(job.cancelled).pipe(
                  Effect.andThen(Effect.interrupt),
                ),
              )
        ).pipe(Effect.exit)

        yield* Deferred.done(job.result, exit)
      },
    )

    const dispatchJob = Effect.fn("DeliverySupervisor.dispatchJob")(
      function* (job: DeliveryJob) {
        const task = Option.match(job.parentSpan, {
          onNone: () => processJob(job),
          onSome: (parentSpan) =>
            processJob(job).pipe(
              Effect.withParentSpan(parentSpan),
            ),
        })
        yield* FiberSet.run(
          deliveries,
          task,
        )
      },
    )

    yield* Stream.fromQueue(requests).pipe(
      Stream.runForEach(dispatchJob),
      Effect.forkScoped,
    )

    const overload = Effect.fn("DeliverySupervisor.overload")(
      (destinationId: DestinationId) =>
        Ref.update(rejected, (count) => count + 1).pipe(
          Effect.andThen(
            Effect.fail(
              new DeliveryOverloaded({
                admissionCapacity:
                  configuration.flow.deliveryRequestsCapacity,
                destinationId,
              }),
            ),
          ),
        ),
    )

    const trackAdmitted = <A, E, R>(task: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        Ref.update(admitted, (count) => count + 1),
        () => task,
        () => Ref.update(admitted, (count) => count - 1),
      )

    const deliverTo = Effect.fn("DeliverySupervisor.deliverTo")(
      function* (candidate: unknown, destination: Destination) {
        const accepted = yield* admission.withPermitsIfAvailable(1)(
          trackAdmitted(
            Effect.gen(function* () {
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
              yield* Effect.annotateCurrentSpan({
                "relay.event_id": event.id,
                "relay.delivery_id": deliveryId,
                "relay.destination_id": destination.id,
              })
              const delivery = yield* intakeStore.savePending(
                event,
                deliveryId,
                destination.id,
              )
              if (hooks.afterIntakeCommit !== undefined) {
                yield* hooks.afterIntakeCommit(delivery)
              }
              yield* Effect.logInfo("delivery.intent.persisted").pipe(
                Effect.annotateLogs({
                  "relay.event_id": event.id,
                  "relay.delivery_id": deliveryId,
                  "relay.destination_id": destination.id,
                }),
              )

              const result = yield* Deferred.make<DeliveryResult>()
              const cancelled = yield* Deferred.make<void>()
              const clock = yield* Clock.Clock
              const parentSpan = yield* Effect.option(
                Effect.currentSpan,
              )
              const random = yield* Random.Random
              const offered = yield* Queue.offer(requests, {
                cancelled,
                clock,
                deliveryId,
                destination,
                event,
                parentSpan,
                random,
                result,
              })

              if (!offered) {
                return yield* overload(destination.id)
              }

              return yield* Deferred.await(result).pipe(
                Effect.onInterrupt(() =>
                  Deferred.succeed(cancelled, undefined).pipe(
                    Effect.asVoid,
                  )
                ),
              )
            }),
          ),
        )

        return Option.isSome(accepted)
          ? accepted.value
          : yield* overload(destination.id)
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
    const loadMetrics = Effect.fn(
      "DeliverySupervisor.loadMetrics",
    )(function* () {
      const concurrency = yield* Ref.get(active)
      return {
        ...concurrency,
        activeDeliveries: yield* FiberSet.size(deliveries),
        admittedDeliveries: yield* Ref.get(admitted),
        globalConcurrencyLimit: configuration.concurrency.global,
        perDestinationConcurrencyLimit:
          configuration.concurrency.perDestination,
        rejected: yield* Ref.get(rejected),
        requestQueueCapacity:
          configuration.flow.deliveryRequestsCapacity,
        requestQueueDepth: yield* Queue.size(requests),
      }
    })

    return DeliverySupervisor.of({
      activeCount,
      concurrencyMetrics,
      deliver,
      deliverTo,
      loadMetrics,
    })
  }),
)

export const DeliverySupervisorLive = makeDeliverySupervisorLive()

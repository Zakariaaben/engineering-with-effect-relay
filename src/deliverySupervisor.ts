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
import { makeDeliveryMetrics } from "./deliveryMetrics.ts"
import { DeliveryEvents } from "./deliveryEvents.ts"
import { DestinationClient } from "./destinationClient.ts"
import {
  DeliveryIdentityError,
  DeliveryOverloaded,
  type DeliveryRepositoryError,
  type InvalidEventError,
  type RelayIntakeStoreError,
} from "./errors.ts"
import { sendDelivery } from "./effectSender.ts"
import { generateDeliveryId } from "./identifiers.ts"
import { Destination } from "./model.ts"
import type {
  Delivery,
  DeliveryId,
  DeliveryResult,
  DestinationId,
  RelayEvent,
} from "./model.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
  type ClaimedDelivery,
} from "./services.ts"
import { decodeIncomingEvent } from "./workflow.ts"

type DeliveryExecutionFailure =
  | InvalidEventError
  | DeliveryIdentityError

type DeliveryFailure =
  | DeliveryExecutionFailure
  | DeliveryOverloaded
  | DeliveryRepositoryError
  | RelayIntakeStoreError

interface DeliveryJob {
  readonly cancelled: Deferred.Deferred<void>
  readonly clock: Clock.Clock
  readonly deliveryId: DeliveryId
  readonly destination: Destination
  readonly event: RelayEvent
  readonly parentSpan: Option.Option<Tracer.AnySpan>
  readonly random: Context.Service.Shape<typeof Random.Random>
  readonly result: Deferred.Deferred<
    DeliveryResult,
    DeliveryRepositoryError
  >
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
  readonly resumeClaimed: (
    claimed: ClaimedDelivery,
  ) => Effect.Effect<
    DeliveryResult,
    DeliveryOverloaded | DeliveryRepositoryError
  >
  readonly enqueueClaimed: (
    claimed: ClaimedDelivery,
  ) => Effect.Effect<void, DeliveryRepositoryError>
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
    const repository = yield* DeliveryRepository
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
    const metrics = makeDeliveryMetrics()
    yield* metrics.initialize({
      activeAttempts: 0,
      activeAttemptLimit: configuration.concurrency.global,
      admittedDeliveries: 0,
      admissionCapacity: configuration.flow.deliveryRequestsCapacity,
      requestQueueDepth: 0,
      requestQueueCapacity: configuration.flow.deliveryRequestsCapacity,
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
        Ref.updateAndGet(active, (current) => {
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
        }).pipe(
          Effect.tap((current) =>
            metrics.setActiveAttempts(current.globalActive)
          ),
        ),
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
          metrics.recordAttempt,
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
        const execution = (
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
        ).pipe(
          Effect.tap((result) =>
            repository.completeClaim(job.deliveryId, result)
          ),
          Effect.catchCause((cause) =>
            repository.releaseClaim(job.deliveryId).pipe(
              Effect.ignore,
              Effect.andThen(Effect.failCause(cause)),
            )
          ),
        )
        const exit = yield* Effect.exit(execution)

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
        Effect.all([
          Ref.update(rejected, (count) => count + 1),
          metrics.recordAdmissionRejection,
        ], { discard: true }).pipe(
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
        Ref.updateAndGet(admitted, (count) => count + 1).pipe(
          Effect.tap(metrics.setAdmittedDeliveries),
        ),
        () => task,
        () =>
          Ref.updateAndGet(admitted, (count) => count - 1).pipe(
            Effect.tap(metrics.setAdmittedDeliveries),
          ),
      )

    const offerClaimed = Effect.fn("DeliverySupervisor.offerClaimed")(
      function* (
        deliveryId: DeliveryId,
        event: RelayEvent,
        destination: Destination,
      ) {
        const result = yield* Deferred.make<
          DeliveryResult,
          DeliveryRepositoryError
        >()
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
          yield* repository.releaseClaim(deliveryId)
          return yield* overload(destination.id)
        }

        return { cancelled, result }
      },
    )

    const submitClaimed = Effect.fn("DeliverySupervisor.submitClaimed")(
      function* (
        deliveryId: DeliveryId,
        event: RelayEvent,
        destination: Destination,
      ) {
        const offered = yield* offerClaimed(
          deliveryId,
          event,
          destination,
        )
        return yield* Deferred.await(offered.result).pipe(
          Effect.onInterrupt(() =>
            Deferred.succeed(offered.cancelled, undefined).pipe(
              Effect.asVoid,
            )
          ),
        )
      },
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

              return yield* submitClaimed(
                deliveryId,
                event,
                destination,
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
    const resumeClaimed = Effect.fn(
      "DeliverySupervisor.resumeClaimed",
    )((claimed: ClaimedDelivery) => {
      const destination = Option.match(claimed.route, {
        onNone: () => configuration.destination,
        onSome: (route) => Destination.make({
          id: route.destinationId,
          endpoint: route.endpoint,
          authorization: configuration.destination.authorization,
        }),
      })
      return submitClaimed(
        claimed.delivery.id,
        claimed.event,
        destination,
      )
    })
    const enqueueClaimed = Effect.fn(
      "DeliverySupervisor.enqueueClaimed",
    )((claimed: ClaimedDelivery) => {
      const destination = Option.match(claimed.route, {
        onNone: () => configuration.destination,
        onSome: (route) => Destination.make({
          id: route.destinationId,
          endpoint: route.endpoint,
          authorization: configuration.destination.authorization,
        }),
      })
      return offerClaimed(
        claimed.delivery.id,
        claimed.event,
        destination,
      ).pipe(
        Effect.asVoid,
        Effect.catchTag("DeliveryOverloaded", () => Effect.void),
      )
    })
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
      const snapshot = {
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
      yield* metrics.setSaturation({
        activeAttempts: snapshot.globalActive,
        activeAttemptLimit: snapshot.globalConcurrencyLimit,
        admittedDeliveries: snapshot.admittedDeliveries,
        admissionCapacity: snapshot.requestQueueCapacity,
        requestQueueDepth: snapshot.requestQueueDepth,
        requestQueueCapacity: snapshot.requestQueueCapacity,
      })
      return snapshot
    })

    return DeliverySupervisor.of({
      activeCount,
      concurrencyMetrics,
      deliver,
      deliverTo,
      enqueueClaimed,
      loadMetrics,
      resumeClaimed,
    })
  }),
)

export const DeliverySupervisorLive = makeDeliverySupervisorLive()

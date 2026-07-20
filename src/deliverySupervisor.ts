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
  Semaphore,
  Stream,
  Tracer,
} from "effect"
import {
  decodeIncomingEvent,
  type RelayEvent,
} from "./command.ts"
import { AppConfiguration } from "./configuration.ts"
import {
  type DeliveryLoadMetrics,
  makeDeliveryAdmission,
} from "./deliveryAdmission.ts"
import {
  type DeliveryConcurrencyMetrics,
  makeDeliveryCapacity,
} from "./deliveryCapacity.ts"
import type { AttemptObservation } from "./deliveryEngine.ts"
import { makeDeliveryMetrics } from "./deliveryMetrics.ts"
import { DeliveryEvents } from "./deliveryEvents.ts"
import {
  Destination,
  DestinationClient,
} from "./destination.ts"
import {
  type DeliveryJob,
  makeDeliveryWorker,
} from "./deliveryWorker.ts"
import {
  type ClaimLostError,
  DeliveryIdentityError,
  type DeliveryOverloaded,
  type DeliveryRepositoryError,
  type InvalidEventError,
  type RelayIntakeStoreError,
} from "./errors.ts"
import { generateDeliveryId } from "./identifiers.ts"
import type {
  Delivery,
  DeliveryAttempt,
  DeliveryResult,
} from "./delivery.ts"
import type { DeliveryId } from "./identifiers.ts"
import {
  DeliveryRepository,
  type ClaimedDelivery,
} from "./deliveryRepository.ts"
import { RelayIntakeStore } from "./intakeStore.ts"
import { WorkerIdentity } from "./workerIdentity.ts"

type DeliveryExecutionFailure =
  | InvalidEventError
  | DeliveryIdentityError

type DeliveryClaimFailure = ClaimLostError | DeliveryRepositoryError

type DeliveryFailure =
  | DeliveryExecutionFailure
  | DeliveryOverloaded
  | DeliveryClaimFailure
  | RelayIntakeStoreError

export interface DeliverySupervisorHooks {
  readonly afterIntakeCommit?: (
    delivery: Delivery,
  ) => Effect.Effect<void>
  readonly afterClaimQueued?: (
    deliveryId: DeliveryId,
  ) => Effect.Effect<void>
  readonly afterAttemptObserved?: (
    deliveryId: DeliveryId,
    attempt: AttemptObservation,
  ) => Effect.Effect<void>
  readonly afterAttemptRecorded?: (
    deliveryId: DeliveryId,
    attempt: DeliveryAttempt,
  ) => Effect.Effect<void>
}

export interface DeliverySupervisorService {
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
    DeliveryOverloaded | DeliveryClaimFailure
  >
  readonly enqueueClaimed: (
    claimed: ClaimedDelivery,
  ) => Effect.Effect<void, DeliveryClaimFailure>
  readonly activeCount: () => Effect.Effect<number>
  readonly concurrencyMetrics: () => Effect.Effect<DeliveryConcurrencyMetrics>
  readonly loadMetrics: () => Effect.Effect<DeliveryLoadMetrics>
}

export class DeliverySupervisor extends Context.Service<
  DeliverySupervisor,
  DeliverySupervisorService
>()("Relay/DeliverySupervisor") {}

const destinationFor = (
  claimed: ClaimedDelivery,
  fallback: Destination,
): Destination =>
  Option.match(claimed.route, {
    onNone: () => fallback,
    onSome: (route) => Destination.make({
      id: route.destinationId,
      endpoint: route.endpoint,
      authorization: fallback.authorization,
    }),
  })

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
    const worker = yield* WorkerIdentity
    const deliveries = yield* FiberSet.make<void>()
    const requests = yield* Effect.acquireRelease(
      Queue.dropping<DeliveryJob>(
        configuration.flow.deliveryRequestsCapacity,
      ),
      Queue.shutdown,
    )
    const dispatchPermits = yield* Semaphore.make(
      configuration.flow.deliveryRequestsCapacity,
    )
    const metrics = makeDeliveryMetrics()
    yield* metrics.initialize({
      activeAttempts: 0,
      activeAttemptLimit: configuration.concurrency.global,
      admittedDeliveries: 0,
      admissionCapacity: configuration.flow.deliveryRequestsCapacity,
      requestQueueDepth: 0,
      requestQueueCapacity: configuration.flow.deliveryRequestsCapacity,
    })
    const capacity = yield* makeDeliveryCapacity({
      configuration,
      metrics,
    })
    const admission = yield* makeDeliveryAdmission({
      configuration,
      concurrencyMetrics: capacity.metrics,
      metrics,
    })

    const processJob = makeDeliveryWorker({
      configuration,
      destinationClient,
      events: deliveryEvents,
      hooks,
      metrics,
      repository,
      withAttempt: capacity.withAttempt,
    })

    const dispatchJob = Effect.fn("DeliverySupervisor.dispatchJob")(
      (job: DeliveryJob) => Effect.uninterruptibleMask((restore) =>
        restore(Semaphore.take(dispatchPermits, 1)).pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const task = Option.match(job.parentSpan, {
                onNone: () => processJob(job),
                onSome: (parentSpan) =>
                  processJob(job).pipe(
                    Effect.withParentSpan(parentSpan),
                  ),
              })
              yield* FiberSet.run(
                deliveries,
                task.pipe(
                  Effect.ensuring(Semaphore.release(dispatchPermits, 1)),
                ),
              )
            }),
          ),
        )
      ),
    )

    yield* Stream.fromQueue(requests).pipe(
      Stream.runForEach(dispatchJob),
      Effect.forkScoped,
    )

    const offerClaimed = Effect.fn("DeliverySupervisor.offerClaimed")(
      function* (
        claimed: ClaimedDelivery,
        destination: Destination,
      ) {
        const result = yield* Deferred.make<
          DeliveryResult,
          DeliveryClaimFailure
        >()
        const cancelled = yield* Deferred.make<void>()
        const clock = yield* Clock.Clock
        const parentSpan = yield* Effect.option(
          Effect.currentSpan,
        )
        const random = yield* Random.Random
        const offered = yield* Queue.offer(requests, {
          cancelled,
          claim: claimed.claim,
          clock,
          deliveryId: claimed.delivery.id,
          destination,
          event: claimed.event,
          nextAttemptOrdinal: claimed.nextAttemptOrdinal,
          parentSpan,
          random,
          result,
        })

        if (!offered) {
          yield* repository.releaseClaim(
            claimed.delivery.id,
            claimed.claim,
          )
          return yield* admission.reject(
            destination.id,
            "GlobalAdmission",
          )
        }

        if (hooks.afterClaimQueued !== undefined) {
          yield* hooks.afterClaimQueued(claimed.delivery.id)
        }

        return { cancelled, result }
      },
    )

    const submitClaimed = Effect.fn("DeliverySupervisor.submitClaimed")(
      function* (
        claimed: ClaimedDelivery,
        destination: Destination,
      ) {
        const offered = yield* offerClaimed(
          claimed,
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
        return yield* admission.admit(
          destination.id,
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
            const claimed = yield* intakeStore.savePending(
              event,
              deliveryId,
              destination.id,
              {
                ownerId: worker.id,
                leaseDurationMillis: Duration.toMillis(
                  configuration.recovery.claimLeaseDuration,
                ),
              },
            )
            if (hooks.afterIntakeCommit !== undefined) {
              yield* hooks.afterIntakeCommit(claimed.delivery)
            }
            yield* Effect.logInfo("delivery.intent.persisted").pipe(
              Effect.annotateLogs({
                "relay.event_id": event.id,
                "relay.delivery_id": deliveryId,
                "relay.destination_id": destination.id,
              }),
            )

            return yield* submitClaimed(claimed, destination)
          }),
        )
      },
    )
    const deliver = Effect.fn("DeliverySupervisor.deliver")(
      (candidate: unknown) =>
        deliverTo(candidate, configuration.destination),
    )
    const resumeClaimed = Effect.fn(
      "DeliverySupervisor.resumeClaimed",
    )((claimed: ClaimedDelivery) => {
      const destination = destinationFor(claimed, configuration.destination)
      return submitClaimed(
        claimed,
        destination,
      )
    })
    const enqueueClaimed = Effect.fn(
      "DeliverySupervisor.enqueueClaimed",
    )((claimed: ClaimedDelivery) => {
      const destination = destinationFor(claimed, configuration.destination)
      return offerClaimed(
        claimed,
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
    )(() => capacity.metrics)
    const loadMetrics = Effect.fn(
      "DeliverySupervisor.loadMetrics",
    )(function* () {
      const snapshot = yield* admission.loadMetrics({
        activeDeliveries: yield* FiberSet.size(deliveries),
        requestQueueDepth: yield* Queue.size(requests),
      })
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

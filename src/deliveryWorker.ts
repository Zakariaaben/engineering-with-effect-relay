import {
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Option,
  Random,
  Ref,
  Tracer,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import {
  type AttemptObservation,
  observeDeliveryAttempt,
  runDeliveryWithRetry,
} from "./deliveryEngine.ts"
import type { RelayEvent } from "./command.ts"
import { DeliveryEvents } from "./deliveryEvents.ts"
import { makeDeliveryMetrics } from "./deliveryMetrics.ts"
import {
  DestinationClient,
  type Destination,
} from "./destination.ts"
import type { ClaimLostError, DeliveryRepositoryError } from "./errors.ts"
import {
  makeDeliveryAttemptRecord,
  runDelivery,
  type AttemptTraceCorrelation,
  type DeliveryAttempt,
  type DeliveryClaim,
  type DeliveryResult,
} from "./delivery.ts"
import type {
  DeliveryId,
  DestinationId,
} from "./identifiers.ts"
import { DeliveryRepository } from "./deliveryRepository.ts"

type DeliveryClaimFailure = ClaimLostError | DeliveryRepositoryError

export interface DeliveryJob {
  readonly cancelled: Deferred.Deferred<void>
  readonly claim: DeliveryClaim
  readonly clock: Clock.Clock
  readonly deliveryId: DeliveryId
  readonly destination: Destination
  readonly event: RelayEvent
  readonly nextAttemptOrdinal: number
  readonly parentSpan: Option.Option<Tracer.AnySpan>
  readonly random: Context.Service.Shape<typeof Random.Random>
  readonly result: Deferred.Deferred<DeliveryResult, DeliveryClaimFailure>
}

export interface DeliveryWorkerHooks {
  readonly afterAttemptObserved?: (
    deliveryId: DeliveryId,
    attempt: AttemptObservation,
  ) => Effect.Effect<void>
  readonly afterAttemptRecorded?: (
    deliveryId: DeliveryId,
    attempt: DeliveryAttempt,
  ) => Effect.Effect<void>
}

export type WithAttemptCapacity = <A, E, R>(
  destinationId: DestinationId,
  task: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R>

export interface DeliveryWorkerOptions {
  readonly configuration: Context.Service.Shape<typeof AppConfiguration>
  readonly destinationClient: Context.Service.Shape<typeof DestinationClient>
  readonly events: Context.Service.Shape<typeof DeliveryEvents>
  readonly hooks: DeliveryWorkerHooks
  readonly metrics: ReturnType<typeof makeDeliveryMetrics>
  readonly repository: Context.Service.Shape<typeof DeliveryRepository>
  readonly withAttempt: WithAttemptCapacity
}

export const makeDeliveryWorker = (options: DeliveryWorkerOptions) => {
  const execute = Effect.fn("DeliveryWorker.execute")(
    function* (
      deliveryId: DeliveryId,
      event: RelayEvent,
      destination: Destination,
      claim: Ref.Ref<DeliveryClaim>,
      firstAttemptOrdinal: number,
      trace: AttemptTraceCorrelation,
    ) {
      const recordAttempt = (attempt: DeliveryAttempt) =>
        Ref.get(claim).pipe(
          Effect.map((current) =>
            makeDeliveryAttemptRecord(
              deliveryId,
              current,
              attempt,
              trace,
            )
          ),
          Effect.flatMap(options.repository.recordAttempt),
          Effect.tap(() =>
            options.hooks.afterAttemptRecorded?.(deliveryId, attempt) ??
              Effect.void
          ),
          Effect.andThen(options.metrics.recordAttempt(attempt)),
        )

      return yield* runDeliveryWithRetry(
        deliveryId,
        destination.id,
        options.configuration.resilience,
        (ordinal, remaining) =>
          options.withAttempt(
            destination.id,
            observeDeliveryAttempt(
              ordinal,
              destination.id,
              Duration.min(
                options.configuration.resilience.attemptTimeout,
                remaining,
              ),
              runDelivery(deliveryId, event, destination).pipe(
                Effect.provideService(
                  DestinationClient,
                  options.destinationClient,
                ),
              ),
            ).pipe(
              Effect.tap((attempt) =>
                options.hooks.afterAttemptObserved?.(
                  deliveryId,
                  attempt,
                ) ?? Effect.void
              ),
            ),
          ),
        recordAttempt,
        firstAttemptOrdinal,
      ).pipe(
        Effect.annotateLogs({
          "relay.event_id": event.id,
          "relay.delivery_id": deliveryId,
          "relay.destination_id": destination.id,
        }),
        Effect.tap(options.events.publish),
      )
    },
  )

  const renewLease = Effect.fn("DeliveryWorker.renewLease")(
    function* (job: DeliveryJob, claim: Ref.Ref<DeliveryClaim>) {
      const current = yield* Ref.get(claim)
      const renewed = yield* options.repository.renewClaim(
        job.deliveryId,
        current,
        Duration.toMillis(
          options.configuration.recovery.claimLeaseDuration,
        ),
      )
      yield* Ref.set(claim, renewed)
    },
  )

  return Effect.fn("DeliveryWorker.process")(
    function* (job: DeliveryJob) {
      yield* Effect.annotateCurrentSpan({
        "relay.event_id": job.event.id,
        "relay.delivery_id": job.deliveryId,
        "relay.destination_id": job.destination.id,
        "relay.claim_owner": job.claim.ownerId,
        "relay.claim_generation": job.claim.generation,
      })
      const span = yield* Effect.currentSpan
      const trace: AttemptTraceCorrelation =
        /^(?!0{32}$)[0-9a-f]{32}$/.test(span.traceId) &&
          /^(?!0{16}$)[0-9a-f]{16}$/.test(span.spanId)
          ? { traceId: span.traceId, spanId: span.spanId }
          : { traceId: null, spanId: null }
      const wasCancelled = yield* Deferred.isDone(job.cancelled)
      const claim = yield* Ref.make(job.claim)
      const execution = wasCancelled
        ? Effect.interrupt
        : Effect.raceFirst(
            execute(
              job.deliveryId,
              job.event,
              job.destination,
              claim,
              job.nextAttemptOrdinal,
              trace,
            ).pipe(
              Effect.provideService(Clock.Clock, job.clock),
              Effect.provideService(Random.Random, job.random),
            ),
            Deferred.await(job.cancelled).pipe(
              Effect.andThen(Effect.interrupt),
            ),
          )
      const renewalFailed = yield* Deferred.make<
        never,
        DeliveryClaimFailure
      >()
      const renew = renewLease(job, claim)
      const withLease = Effect.scoped(
        Effect.gen(function* () {
          yield* renew
          yield* Effect.sleep(
            options.configuration.recovery.claimRenewInterval,
          ).pipe(
            Effect.andThen(renew),
            Effect.forever,
            Effect.catchCause((cause) =>
              Deferred.failCause(renewalFailed, cause).pipe(Effect.asVoid)
            ),
            Effect.provideService(Clock.Clock, job.clock),
            Effect.forkScoped,
          )

          return yield* Effect.raceFirst(
            execution,
            Deferred.await(renewalFailed),
          )
        }),
      ).pipe(
        Effect.tap((result) =>
          result._tag === "ProtocolFailure" || result._tag === "Exhausted"
            ? Effect.all([
                options.metrics.recordDeadLetter(
                  result._tag === "ProtocolFailure"
                    ? "ProviderProtocolFailure"
                    : "RetryBudgetExhausted",
                ),
                Effect.logWarning("delivery.dead_lettered").pipe(
                  Effect.annotateLogs({
                    "relay.dead_letter_reason":
                      result._tag === "ProtocolFailure"
                        ? "ProviderProtocolFailure"
                        : "RetryBudgetExhausted",
                    "relay.attempt_count": result.attempts.length,
                  }),
                ),
              ], { discard: true })
            : Effect.void
        ),
        Effect.tapErrorTag(
          "ClaimLostError",
          (error) => options.metrics.recordFencingRejection(error.operation),
        ),
        Effect.catchCause((cause) =>
          Ref.get(claim).pipe(
            Effect.flatMap((current) =>
              options.repository.releaseClaim(job.deliveryId, current)
            ),
            Effect.ignore,
            Effect.andThen(Effect.failCause(cause)),
          )
        ),
        Effect.provideService(Clock.Clock, job.clock),
        Effect.annotateLogs({
          "relay.claim_owner": job.claim.ownerId,
          "relay.claim_generation": job.claim.generation,
        }),
      )
      const exit = yield* Effect.exit(withLease)

      yield* Deferred.done(job.result, exit)
    },
  )
}

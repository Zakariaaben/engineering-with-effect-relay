import {
  Cause,
  Clock,
  Data,
  Duration,
  Effect,
  Random,
  Ref,
  Schedule,
} from "effect"
import type { DeliveryResilience } from "./configuration.ts"
import type { DeliveryTransportError } from "./errors.ts"
import {
  DeliveryAttemptDecision,
  DeliveryOutcome,
  DeliveryResult,
  type DeliveryAttempt,
  type DeliveryId,
  type DeliveryOutcome as DeliveryOutcomeType,
  type DeliveryResult as DeliveryResultType,
  type DestinationId,
} from "./model.ts"

interface AttemptObservation {
  readonly ordinal: number
  readonly startedAtMillis: number
  readonly completedAtMillis: number
  readonly outcome: DeliveryOutcomeType
}

class RetryableAttempt extends Data.TaggedError("RetryableAttempt")<{
  readonly attempt: AttemptObservation
}> {}

const withDecision = (
  attempt: AttemptObservation,
  decision: DeliveryAttempt["decision"],
): DeliveryAttempt => ({ ...attempt, decision })

const isRetryable = (outcome: DeliveryOutcomeType): boolean =>
  DeliveryOutcome.$match(outcome, {
    Delivered: () => false,
    Rejected: () => false,
    ProtocolFailure: () => false,
    Retryable: () => true,
    TransportFailure: () => true,
    TimedOut: () => true,
  })

const terminalResult = (
  deliveryId: DeliveryId,
  destinationId: DestinationId,
  outcome: DeliveryOutcomeType,
  attempts: ReadonlyArray<DeliveryAttempt>,
): DeliveryResultType => {
  const fields = { deliveryId, destinationId, attempts }
  return DeliveryOutcome.$match(outcome, {
    Delivered: ({ status }) =>
      DeliveryResult.Delivered({ ...fields, status }),
    Rejected: ({ status }) =>
      DeliveryResult.Rejected({ ...fields, status }),
    ProtocolFailure: ({ status }) =>
      DeliveryResult.ProtocolFailure({ ...fields, status }),
    Retryable: () =>
      DeliveryResult.Exhausted({
        ...fields,
        lastOutcome: outcome,
      }),
    TransportFailure: () =>
      DeliveryResult.Exhausted({
        ...fields,
        lastOutcome: outcome,
      }),
    TimedOut: () =>
      DeliveryResult.Exhausted({
        ...fields,
        lastOutcome: outcome,
      }),
  })
}

export const observeDeliveryAttempt = Effect.fn(
  "DeliveryEngine.observeAttempt",
)(function* (
  ordinal: number,
  destinationId: DestinationId,
  attemptTimeout: Duration.Duration,
  send: Effect.Effect<
    DeliveryOutcomeType,
    DeliveryTransportError
  >,
) {
  const startedAtMillis = yield* Clock.currentTimeMillis
  const outcome = yield* send.pipe(
    Effect.timeout(attemptTimeout),
    Effect.match({
      onFailure: (failure) =>
        failure instanceof Cause.TimeoutError
          ? DeliveryOutcome.TimedOut({ destinationId })
          : DeliveryOutcome.TransportFailure({ destinationId }),
      onSuccess: (result) => result,
    }),
  )
  const completedAtMillis = yield* Clock.currentTimeMillis
  return {
    ordinal,
    startedAtMillis,
    completedAtMillis,
    outcome,
  } satisfies AttemptObservation
})

const makeRetrySchedule = (
  resilience: DeliveryResilience,
  deliveryStartedAtMillis: number,
  onRetry: (
    attempt: AttemptObservation,
    delayMillis: number,
  ) => Effect.Effect<void>,
) => {
  const maxElapsedMillis = Duration.toMillis(resilience.maxElapsed)
  const deadline = deliveryStartedAtMillis + maxElapsedMillis
  const cappedBackoff = Schedule.min([
    Schedule.exponential(resilience.baseDelay),
    Schedule.spaced(resilience.maxDelay),
  ]).pipe(Schedule.setInputType<RetryableAttempt>())

  return cappedBackoff.pipe(
    Schedule.modifyDelay(({ duration }) =>
      Random.next.pipe(
        Effect.map((fraction) =>
          Duration.millis(
            Math.floor(
              Duration.toMillis(duration) * fraction,
            ),
          ),
        ),
      ),
    ),
    Schedule.modifyDelay(({ duration, input }) => {
      const retryAfterMillis =
        input.attempt.outcome._tag === "Retryable"
          ? input.attempt.outcome.retryAfterMillis ?? 0
          : 0
      return Effect.succeed(
        Duration.max(
          duration,
          Duration.millis(retryAfterMillis),
        ),
      )
    }),
    Schedule.modifyDelay(({ duration }) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) =>
          Duration.min(
            duration,
            Duration.millis(Math.max(0, deadline - now)),
          ),
        ),
      ),
    ),
    Schedule.upTo({ times: resilience.maxAttempts - 1 }),
    Schedule.while(({ duration, now }) =>
      now + Duration.toMillis(duration) < deadline,
    ),
    Schedule.tap(({ duration, input }) =>
      onRetry(input.attempt, Duration.toMillis(duration)),
    ),
  )
}

export const runDeliveryWithRetry = Effect.fn(
  "DeliveryEngine.runDeliveryWithRetry",
)(function* (
  deliveryId: DeliveryId,
  destinationId: DestinationId,
  resilience: DeliveryResilience,
  executeAttempt: (
    ordinal: number,
    remaining: Duration.Duration,
  ) => Effect.Effect<AttemptObservation>,
) {
  const deliveryStartedAtMillis = yield* Clock.currentTimeMillis
  const deadline = deliveryStartedAtMillis +
    Duration.toMillis(resilience.maxElapsed)
  const nextOrdinal = yield* Ref.make(0)
  const history = yield* Ref.make<ReadonlyArray<DeliveryAttempt>>([])
  const append = (attempt: DeliveryAttempt) =>
    Ref.update(history, (current) => [...current, attempt])

  const execute = Effect.gen(function* () {
    const ordinal = yield* Ref.updateAndGet(
      nextOrdinal,
      (current) => current + 1,
    )
    const attemptStartedAtMillis = yield* Clock.currentTimeMillis
    const attempt = yield* executeAttempt(
      ordinal,
      Duration.millis(
        Math.max(0, deadline - attemptStartedAtMillis),
      ),
    )
    if (isRetryable(attempt.outcome)) {
      return yield* Effect.fail(new RetryableAttempt({ attempt }))
    }

    yield* append(
      withDecision(attempt, DeliveryAttemptDecision.Terminal()),
    )
    const attempts = yield* Ref.get(history)
    return terminalResult(
      deliveryId,
      destinationId,
      attempt.outcome,
      attempts,
    )
  })

  const schedule = makeRetrySchedule(
    resilience,
    deliveryStartedAtMillis,
    (attempt, delayMillis) =>
      append(
        withDecision(
          attempt,
          DeliveryAttemptDecision.RetryScheduled({
            delayMillis,
          }),
        ),
      ),
  )

  return yield* execute.pipe(
    Effect.retryOrElse(
      schedule,
      (lastFailure) =>
        Effect.gen(function* () {
          const exhausted = withDecision(
            lastFailure.attempt,
            DeliveryAttemptDecision.Exhausted(),
          )
          yield* append(exhausted)
          const attempts = yield* Ref.get(history)
          return DeliveryResult.Exhausted({
            deliveryId,
            destinationId,
            attempts,
            lastOutcome: exhausted.outcome,
          })
        }),
    ),
  )
})

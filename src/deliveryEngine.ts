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

const logDeliveryAttempt = (attempt: DeliveryAttempt) => {
  const annotations: Record<string, unknown> = {
    "relay.attempt_number": attempt.ordinal,
    "relay.attempt_duration_ms":
      attempt.completedAtMillis - attempt.startedAtMillis,
    "relay.attempt_outcome": attempt.outcome._tag,
    "relay.attempt_decision": attempt.decision._tag,
  }

  if ("status" in attempt.outcome) {
    annotations["relay.http_status"] = attempt.outcome.status
  }
  if (attempt.outcome._tag === "Retryable") {
    annotations["relay.retry_reason"] = attempt.outcome.reason
  }
  if (attempt.decision._tag === "RetryScheduled") {
    annotations["relay.retry_delay_ms"] =
      attempt.decision.delayMillis
  }

  const log = attempt.decision._tag === "Exhausted"
    ? Effect.logWarning
    : Effect.logInfo
  return log("delivery.attempt.finished").pipe(
    Effect.annotateLogs(annotations),
  )
}

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
  deliveryStartedAtNanos: bigint,
  onRetry: (
    attempt: AttemptObservation,
    delayMillis: number,
  ) => Effect.Effect<void>,
) => {
  const deadline = deliveryStartedAtNanos +
    Duration.toNanosUnsafe(resilience.maxElapsed)
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
      Clock.currentTimeNanos.pipe(
        Effect.map((now) =>
          Duration.min(
            duration,
            Duration.nanos(
              now < deadline ? deadline - now : 0n,
            ),
          ),
        ),
      ),
    ),
    Schedule.upTo({ times: resilience.maxAttempts - 1 }),
    Schedule.while(({ duration }) =>
      Clock.currentTimeNanos.pipe(
        Effect.map((now) =>
          now + Duration.toNanosUnsafe(duration) < deadline,
        ),
      ),
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
  onAttempt: (
    attempt: DeliveryAttempt,
  ) => Effect.Effect<void> = () => Effect.void,
) {
  const deliveryStartedAtNanos = yield* Clock.currentTimeNanos
  const deadline = deliveryStartedAtNanos +
    Duration.toNanosUnsafe(resilience.maxElapsed)
  const nextOrdinal = yield* Ref.make(0)
  const history = yield* Ref.make<ReadonlyArray<DeliveryAttempt>>([])
  const append = (attempt: DeliveryAttempt) =>
    Ref.update(history, (current) => [...current, attempt]).pipe(
      Effect.andThen(logDeliveryAttempt(attempt)),
      Effect.andThen(onAttempt(attempt)),
    )

  const execute = Effect.gen(function* () {
    const ordinal = yield* Ref.updateAndGet(
      nextOrdinal,
      (current) => current + 1,
    )
    const attemptStartedAtNanos = yield* Clock.currentTimeNanos
    const attempt = yield* executeAttempt(
      ordinal,
      Duration.nanos(
        attemptStartedAtNanos < deadline
          ? deadline - attemptStartedAtNanos
          : 0n,
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
    deliveryStartedAtNanos,
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

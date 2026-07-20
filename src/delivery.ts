import { Clock, Data, Effect, Schema } from "effect"
import {
  decodeIncomingEvent,
  type RelayEvent,
} from "./command.ts"
import {
  DestinationClient,
  type DeliveryRequest,
  type DeliveryResponseEvidence,
  type Destination,
} from "./destination.ts"
import {
  ClaimGeneration,
  DeliveryId,
  DestinationId,
  EventId,
  WorkerId,
} from "./identifiers.ts"

export interface DeliveryClaim extends
  Schema.Schema.Type<typeof DeliveryClaim> {}

export const DeliveryClaim = Schema.Struct({
  ownerId: WorkerId,
  generation: ClaimGeneration,
  leaseExpiresAtMillis: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
  ),
})

export const DeliveryState = Schema.TaggedUnion({
  Pending: {},
  Delivered: {
    status: Schema.Int,
  },
  Rejected: {
    status: Schema.Int,
  },
  DeadLettered: {
    reason: Schema.Literals([
      "ProviderProtocolFailure",
      "RetryBudgetExhausted",
    ]),
  },
  Terminated: {
    reason: Schema.Literal("OperatorTerminated"),
  },
})
export type DeliveryState = Schema.Schema.Type<typeof DeliveryState>

export const DeadLetterReason = Schema.Literals([
  "ProviderProtocolFailure",
  "RetryBudgetExhausted",
])
export type DeadLetterReason = Schema.Schema.Type<typeof DeadLetterReason>

export interface Delivery extends Schema.Schema.Type<typeof Delivery> {}

export const Delivery = Schema.Struct({
  id: DeliveryId,
  eventId: EventId,
  destinationId: DestinationId,
  state: DeliveryState,
})

export type DeliveryOutcome = Data.TaggedEnum<{
  Delivered: {
    readonly destinationId: DestinationId
    readonly status: number
  }
  Rejected: {
    readonly destinationId: DestinationId
    readonly status: number
  }
  Retryable: {
    readonly destinationId: DestinationId
    readonly status: number
    readonly reason:
      | "AmbiguousResponse"
      | "RateLimited"
      | "ProviderFailure"
    readonly retryAfterMillis?: number
  }
  ProtocolFailure: {
    readonly destinationId: DestinationId
    readonly status: number
  }
  TransportFailure: {
    readonly destinationId: DestinationId
  }
  TimedOut: {
    readonly destinationId: DestinationId
  }
}>

export const DeliveryOutcome = Data.taggedEnum<DeliveryOutcome>()

export const parseRetryAfterMillis = (
  value: string | undefined,
  nowMillis: number,
): number | undefined => {
  if (value === undefined) return undefined
  const trimmed = value.trim()
  if (/^\d+$/.test(trimmed)) {
    const seconds = Number(trimmed)
    return Number.isSafeInteger(seconds) &&
        seconds <= Number.MAX_SAFE_INTEGER / 1_000
      ? seconds * 1_000
      : undefined
  }
  const timestamp = Date.parse(trimmed)
  return Number.isFinite(timestamp)
    ? Math.max(0, timestamp - nowMillis)
    : undefined
}

export const classifyDeliveryResponse = (
  destinationId: DestinationId,
  response: DeliveryResponseEvidence,
  nowMillis: number,
): DeliveryOutcome => {
  const { status } = response
  if (status >= 200 && status < 300) {
    return DeliveryOutcome.Delivered({ destinationId, status })
  }
  if (status === 408 || status === 425) {
    return DeliveryOutcome.Retryable({
      destinationId,
      status,
      reason: "AmbiguousResponse",
    })
  }
  if (status === 429) {
    const retryAfterMillis = parseRetryAfterMillis(
      response.retryAfter,
      nowMillis,
    )
    return retryAfterMillis === undefined
      ? DeliveryOutcome.Retryable({
          destinationId,
          status,
          reason: "RateLimited",
        })
      : DeliveryOutcome.Retryable({
          destinationId,
          status,
          reason: "RateLimited",
          retryAfterMillis,
        })
  }
  if (status >= 500 && status < 600) {
    return DeliveryOutcome.Retryable({
      destinationId,
      status,
      reason: "ProviderFailure",
    })
  }
  if (status >= 300 && status < 500) {
    return DeliveryOutcome.Rejected({ destinationId, status })
  }
  return DeliveryOutcome.ProtocolFailure({ destinationId, status })
}

export const classifyDeliveryStatus = (
  destinationId: DestinationId,
  status: number,
): DeliveryOutcome =>
  classifyDeliveryResponse(
    destinationId,
    { status },
    0,
  )

export type DeliveryAttemptDecision = Data.TaggedEnum<{
  Terminal: {}
  RetryScheduled: {
    readonly delayMillis: number
  }
  Exhausted: {}
}>

export const DeliveryAttemptDecision =
  Data.taggedEnum<DeliveryAttemptDecision>()

export interface DeliveryAttempt {
  readonly ordinal: number
  readonly startedAtMillis: number
  readonly completedAtMillis: number
  readonly outcome: DeliveryOutcome
  readonly decision: DeliveryAttemptDecision
}

const AttemptOutcome = Schema.Literals([
  "Delivered",
  "Rejected",
  "Retryable",
  "ProtocolFailure",
  "TransportFailure",
  "TimedOut",
])

const AttemptDecision = Schema.Literals([
  "Terminal",
  "RetryScheduled",
  "Exhausted",
])

export interface DeliveryAttemptRecord extends
  Schema.Schema.Type<typeof DeliveryAttemptRecord> {}

export const DeliveryAttemptRecord = Schema.Struct({
  deliveryId: DeliveryId,
  ordinal: Schema.Int.check(Schema.isGreaterThan(0)),
  workerId: WorkerId,
  claimGeneration: ClaimGeneration,
  startedAtMillis: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  completedAtMillis: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  outcome: AttemptOutcome,
  decision: AttemptDecision,
  status: Schema.NullOr(Schema.Int),
  retryDelayMillis: Schema.NullOr(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  traceId: Schema.NullOr(
    Schema.String.check(
      Schema.isPattern(/^(?!0{32}$)[0-9a-f]{32}$/),
    ),
  ),
  spanId: Schema.NullOr(
    Schema.String.check(
      Schema.isPattern(/^(?!0{16}$)[0-9a-f]{16}$/),
    ),
  ),
}).check(
  Schema.makeFilter(
    (attempt) => {
      const carriesStatus = [
        "Delivered",
        "Rejected",
        "Retryable",
        "ProtocolFailure",
      ].includes(attempt.outcome)
      const retryable = [
        "Retryable",
        "TransportFailure",
        "TimedOut",
      ].includes(attempt.outcome)
      return attempt.completedAtMillis >= attempt.startedAtMillis &&
        (attempt.traceId === null) === (attempt.spanId === null) &&
        carriesStatus === (attempt.status !== null) &&
        (attempt.decision === "RetryScheduled") ===
          (attempt.retryDelayMillis !== null) &&
        (attempt.decision === "Terminal" ? !retryable : retryable)
    },
    { expected: "a consistent delivery-attempt record" },
  ),
)

export interface DeliveryStatus extends
  Schema.Schema.Type<typeof DeliveryStatus> {}

export const DeliveryStatus = Schema.Struct({
  delivery: Delivery,
  attempts: Schema.Array(DeliveryAttemptRecord),
})

export interface AttemptTraceCorrelation {
  readonly traceId: string | null
  readonly spanId: string | null
}

export const makeDeliveryAttemptRecord = (
  deliveryId: DeliveryId,
  claim: DeliveryClaim,
  attempt: DeliveryAttempt,
  trace: AttemptTraceCorrelation,
): DeliveryAttemptRecord => DeliveryAttemptRecord.make({
  deliveryId,
  ordinal: attempt.ordinal,
  workerId: claim.ownerId,
  claimGeneration: claim.generation,
  startedAtMillis: attempt.startedAtMillis,
  completedAtMillis: attempt.completedAtMillis,
  outcome: attempt.outcome._tag,
  decision: attempt.decision._tag,
  status: "status" in attempt.outcome ? attempt.outcome.status : null,
  retryDelayMillis: attempt.decision._tag === "RetryScheduled"
    ? attempt.decision.delayMillis
    : null,
  traceId: trace.traceId,
  spanId: trace.spanId,
})

interface DeliveryResultFields {
  readonly deliveryId: DeliveryId
  readonly destinationId: DestinationId
  readonly attempts: ReadonlyArray<DeliveryAttempt>
}

export type DeliveryResult = Data.TaggedEnum<{
  Delivered: DeliveryResultFields & {
    readonly status: number
  }
  Rejected: DeliveryResultFields & {
    readonly status: number
  }
  ProtocolFailure: DeliveryResultFields & {
    readonly status: number
  }
  Exhausted: DeliveryResultFields & {
    readonly lastOutcome: DeliveryOutcome
  }
}>

export const DeliveryResult = Data.taggedEnum<DeliveryResult>()

export const transitionDeliveryState = (
  current: DeliveryState,
  outcome: DeliveryOutcome,
): DeliveryState =>
  DeliveryState.match<DeliveryState>(current, {
    Pending: () =>
      DeliveryOutcome.$match(outcome, {
        Delivered: ({ status }) =>
          DeliveryState.cases.Delivered.make({ status }),
        Rejected: ({ status }) =>
          DeliveryState.cases.Rejected.make({ status }),
        Retryable: () => current,
        ProtocolFailure: () => current,
        TransportFailure: () => current,
        TimedOut: () => current,
      }),
    Delivered: () => current,
    Rejected: () => current,
    DeadLettered: () => current,
    Terminated: () => current,
  })

export const makeDeliveryRequest = (
  deliveryId: DeliveryId,
  event: RelayEvent,
  destination: Destination,
): DeliveryRequest => ({
  deliveryId,
  destinationId: destination.id,
  endpoint: destination.endpoint,
  authorization: destination.authorization,
  body: JSON.stringify(event),
})

export const runDelivery = Effect.fn("Relay.runDelivery")(
  function* (
    deliveryId: DeliveryId,
    event: RelayEvent,
    destination: Destination,
  ) {
    const client = yield* DestinationClient
    const response = yield* client.post(
      makeDeliveryRequest(deliveryId, event, destination),
    )
    const nowMillis = yield* Clock.currentTimeMillis

    return classifyDeliveryResponse(
      destination.id,
      response,
      nowMillis,
    )
  },
)

export const deliverCandidate = Effect.fn("Relay.deliverCandidate")(
  function* (
    deliveryId: DeliveryId,
    candidate: unknown,
    destination: Destination,
  ) {
    const event = yield* decodeIncomingEvent(candidate)
    return yield* runDelivery(deliveryId, event, destination)
  },
)

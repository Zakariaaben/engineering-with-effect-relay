import { Effect, Metric } from "effect"
import type {
  DeadLetterReason,
  DeliveryAttempt,
} from "./delivery.ts"

export interface DeliverySaturationSnapshot {
  readonly activeAttempts: number
  readonly activeAttemptLimit: number
  readonly admittedDeliveries: number
  readonly admissionCapacity: number
  readonly requestQueueDepth: number
  readonly requestQueueCapacity: number
}

const attemptDurationBoundaries = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
]

const retryDelayBoundaries = [
  0.01,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2,
  5,
  10,
  30,
  60,
]

export const makeDeliveryMetrics = () => {
  const admissionRejections = Metric.counter(
    "relay_delivery_admission_rejections_total",
    {
      description: "Deliveries rejected before admission",
      incremental: true,
    },
  )
  const admittedDeliveries = Metric.gauge(
    "relay_delivery_admitted",
    { description: "Deliveries currently admitted" },
  )
  const admissionCapacity = Metric.gauge(
    "relay_delivery_admission_capacity",
    { description: "Maximum deliveries admitted at once" },
  )
  const activeAttempts = Metric.gauge(
    "relay_delivery_active_attempts",
    { description: "Delivery attempts currently using capacity" },
  )
  const activeAttemptLimit = Metric.gauge(
    "relay_delivery_active_attempt_limit",
    { description: "Global concurrent delivery-attempt limit" },
  )
  const requestQueueDepth = Metric.gauge(
    "relay_delivery_request_queue_depth",
    { description: "Delivery jobs currently waiting in memory" },
  )
  const requestQueueCapacity = Metric.gauge(
    "relay_delivery_request_queue_capacity",
    { description: "Maximum delivery jobs admitted in memory" },
  )
  const attempts = Metric.counter("relay_delivery_attempts_total", {
    description: "Completed delivery attempts by outcome and decision",
    incremental: true,
  })
  const deadLetters = Metric.counter(
    "relay_delivery_dead_letters_total",
    {
      description: "Deliveries moved to dead-letter state by reason",
      incremental: true,
    },
  )
  const fencingRejections = Metric.counter(
    "relay_delivery_fencing_rejections_total",
    {
      description: "Stale delivery mutations rejected by operation",
      incremental: true,
    },
  )
  const attemptDuration = Metric.histogram(
    "relay_delivery_attempt_duration_seconds",
    {
      description: "Completed delivery-attempt duration",
      boundaries: attemptDurationBoundaries,
    },
  )
  const retryDelay = Metric.histogram(
    "relay_delivery_retry_delay_seconds",
    {
      description: "Delay selected for scheduled delivery retries",
      boundaries: retryDelayBoundaries,
    },
  )

  const initialize = (snapshot: DeliverySaturationSnapshot) =>
    Effect.all([
      Metric.update(admissionRejections, 0),
      Metric.update(admissionCapacity, snapshot.admissionCapacity),
      Metric.update(activeAttemptLimit, snapshot.activeAttemptLimit),
      Metric.update(
        requestQueueCapacity,
        snapshot.requestQueueCapacity,
      ),
      Metric.update(admittedDeliveries, snapshot.admittedDeliveries),
      Metric.update(activeAttempts, snapshot.activeAttempts),
      Metric.update(requestQueueDepth, snapshot.requestQueueDepth),
    ], { discard: true })

  const recordAdmissionRejection = Metric.update(
    admissionRejections,
    1,
  )

  const recordAttempt = (attempt: DeliveryAttempt) => {
    const durationSeconds = (
      attempt.completedAtMillis - attempt.startedAtMillis
    ) / 1_000
    const attributes = {
      decision: attempt.decision._tag,
      outcome: attempt.outcome._tag,
    }
    const updates: Array<Effect.Effect<void>> = [
      Metric.update(
        Metric.withAttributes(attempts, attributes),
        1,
      ),
      Metric.update(
        Metric.withAttributes(attemptDuration, {
          outcome: attempt.outcome._tag,
        }),
        durationSeconds,
      ),
    ]

    if (attempt.decision._tag === "RetryScheduled") {
      updates.push(
        Metric.update(
          Metric.withAttributes(retryDelay, {
            outcome: attempt.outcome._tag,
          }),
          attempt.decision.delayMillis / 1_000,
        ),
      )
    }

    return Effect.all(updates, { discard: true })
  }

  const recordDeadLetter = (reason: DeadLetterReason) =>
    Metric.update(
      Metric.withAttributes(deadLetters, { reason }),
      1,
    )

  const recordFencingRejection = (operation: string) =>
    Metric.update(
      Metric.withAttributes(fencingRejections, { operation }),
      1,
    )

  const setActiveAttempts = (value: number) =>
    Metric.update(activeAttempts, value)

  const setAdmittedDeliveries = (value: number) =>
    Metric.update(admittedDeliveries, value)

  const setSaturation = (snapshot: DeliverySaturationSnapshot) =>
    Effect.all([
      setActiveAttempts(snapshot.activeAttempts),
      setAdmittedDeliveries(snapshot.admittedDeliveries),
      Metric.update(requestQueueDepth, snapshot.requestQueueDepth),
    ], { discard: true })

  return {
    initialize,
    recordAdmissionRejection,
    recordAttempt,
    recordDeadLetter,
    recordFencingRejection,
    setActiveAttempts,
    setAdmittedDeliveries,
    setSaturation,
  }
}

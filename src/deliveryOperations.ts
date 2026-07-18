import { Context, Effect, Layer, Metric, Option } from "effect"
import type {
  DeadLetterRecoveryError,
  DeliveryRepositoryError,
} from "./errors.ts"
import type { DeliveryId, DeliveryStatus } from "./model.ts"
import { DeliveryRepository } from "./services.ts"

const deadLetterRetries = Metric.counter(
  "relay_delivery_dead_letter_retries_total",
  {
    description: "Dead-lettered deliveries returned to pending work",
    incremental: true,
  },
)

export class DeliveryOperations extends Context.Service<
  DeliveryOperations,
  {
    readonly status: (
      id: DeliveryId,
    ) => Effect.Effect<
      Option.Option<DeliveryStatus>,
      DeliveryRepositoryError
    >
    readonly listDeadLetters: (
      limit: number,
    ) => Effect.Effect<
      ReadonlyArray<DeliveryStatus>,
      DeliveryRepositoryError
    >
    readonly retryDeadLetter: (
      id: DeliveryId,
    ) => Effect.Effect<
      void,
      DeadLetterRecoveryError | DeliveryRepositoryError
    >
  }
>()("Relay/DeliveryOperations") {}

export const DeliveryOperationsLive = Layer.effect(
  DeliveryOperations,
  Effect.gen(function* () {
    const repository = yield* DeliveryRepository

    const status = Effect.fn("DeliveryOperations.status")(
      (id: DeliveryId) => repository.findStatus(id),
    )
    const listDeadLetters = Effect.fn(
      "DeliveryOperations.listDeadLetters",
    )((limit: number) => repository.listDeadLetters(limit))
    const retryDeadLetter = Effect.fn(
      "DeliveryOperations.retryDeadLetter",
    )(function* (id: DeliveryId) {
      const before = yield* repository.findStatus(id)
      yield* repository.retryDeadLetter(id)
      yield* Metric.update(deadLetterRetries, 1)

      const annotations: Record<string, unknown> = {
        "relay.delivery_id": id,
      }
      if (
        Option.isSome(before) &&
        before.value.delivery.state._tag === "DeadLettered"
      ) {
        annotations["relay.dead_letter_reason"] =
          before.value.delivery.state.reason
        annotations["relay.attempt_count"] = before.value.attempts.length
      }
      yield* Effect.logInfo("delivery.dead_letter.retried").pipe(
        Effect.annotateLogs(annotations),
      )
    })

    return DeliveryOperations.of({
      listDeadLetters,
      retryDeadLetter,
      status,
    })
  }),
)

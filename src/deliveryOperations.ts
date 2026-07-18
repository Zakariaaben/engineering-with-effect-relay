import { Context, Effect, Layer, Metric, Option } from "effect"
import {
  DeliveryRepositoryError,
  type DeadLetterDestinationMismatchError,
  type DeadLetterRecoveryError,
} from "./errors.ts"
import { DeliveryRouteSnapshot } from "./model.ts"
import type { DeliveryId, DeliveryStatus } from "./model.ts"
import { AppConfiguration } from "./configuration.ts"
import { DeliveryRepository } from "./services.ts"

const deadLetterActions = Metric.counter(
  "relay_delivery_dead_letter_actions_total",
  {
    description: "Operator actions applied to dead-lettered deliveries",
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
      DeliveryStatus,
      DeadLetterRecoveryError | DeliveryRepositoryError
    >
    readonly repairDeadLetter: (
      id: DeliveryId,
    ) => Effect.Effect<
      DeliveryStatus,
      | DeadLetterDestinationMismatchError
      | DeadLetterRecoveryError
      | DeliveryRepositoryError
    >
    readonly terminateDeadLetter: (
      id: DeliveryId,
    ) => Effect.Effect<
      DeliveryStatus,
      DeadLetterRecoveryError | DeliveryRepositoryError
    >
  }
>()("Relay/DeliveryOperations") {}

export const DeliveryOperationsLive = Layer.effect(
  DeliveryOperations,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const repository = yield* DeliveryRepository

    const status = Effect.fn("DeliveryOperations.status")(
      (id: DeliveryId) => repository.findStatus(id),
    )
    const listDeadLetters = Effect.fn(
      "DeliveryOperations.listDeadLetters",
    )((limit: number) => repository.listDeadLetters(limit))
    const applyAction = Effect.fn("DeliveryOperations.applyAction")(
      function* <E>(
        id: DeliveryId,
        action: "Retry" | "Repair" | "Terminate",
        mutation: Effect.Effect<void, E>,
      ) {
        const before = yield* repository.findStatus(id)
        yield* mutation
        yield* Metric.update(
          Metric.withAttributes(deadLetterActions, {
            action: action.toLowerCase(),
          }),
          1,
        )

        const annotations: Record<string, unknown> = {
          "relay.delivery_id": id,
          "relay.operation": action.toLowerCase(),
        }
        if (
          Option.isSome(before) &&
          before.value.delivery.state._tag === "DeadLettered"
        ) {
          annotations["relay.dead_letter_reason"] =
            before.value.delivery.state.reason
          annotations["relay.attempt_count"] = before.value.attempts.length
        }
        yield* Effect.logInfo("delivery.dead_letter.action_applied").pipe(
          Effect.annotateLogs(annotations),
        )

        const after = yield* repository.findStatus(id)
        return yield* Option.match(after, {
          onNone: () => Effect.fail(new DeliveryRepositoryError({
            operation: "findStatus",
            cause: "operated delivery disappeared from the repository",
          })),
          onSome: Effect.succeed,
        })
      },
    )
    const retryDeadLetter = Effect.fn(
      "DeliveryOperations.retryDeadLetter",
    )((id: DeliveryId) =>
      applyAction(id, "Retry", repository.retryDeadLetter(id)))
    const repairDeadLetter = Effect.fn(
      "DeliveryOperations.repairDeadLetter",
    )((id: DeliveryId) =>
      applyAction(
        id,
        "Repair",
        repository.repairDeadLetter(
          id,
          DeliveryRouteSnapshot.make({
            destinationId: configuration.destination.id,
            endpoint: configuration.destination.endpoint,
            configurationVersion:
              configuration.destinationConfigurationVersion,
          }),
        ),
      ))
    const terminateDeadLetter = Effect.fn(
      "DeliveryOperations.terminateDeadLetter",
    )((id: DeliveryId) =>
      applyAction(
        id,
        "Terminate",
        repository.terminateDeadLetter(id),
      ))

    return DeliveryOperations.of({
      listDeadLetters,
      repairDeadLetter,
      retryDeadLetter,
      status,
      terminateDeadLetter,
    })
  }),
)

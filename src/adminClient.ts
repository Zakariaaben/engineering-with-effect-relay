import { Config, Context, Effect, Layer, Schema } from "effect"
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient"
import {
  operationsAuthorizationClientLayer,
  RelayOperationsHttpApi,
} from "./operationsApi.ts"
import type { DeliveryId, DeliveryStatus } from "./model.ts"

export class AdminRequestError extends Schema.TaggedErrorClass<AdminRequestError>()(
  "AdminRequestError",
  {
    operation: Schema.Literals(["listDeadLetters", "retryDeadLetter"]),
    cause: Schema.Unknown,
  },
) {}

export class RelayAdminClient extends Context.Service<
  RelayAdminClient,
  {
    readonly listDeadLetters: () => Effect.Effect<
      ReadonlyArray<DeliveryStatus>,
      AdminRequestError
    >
    readonly retryDeadLetter: (
      deliveryId: DeliveryId,
    ) => Effect.Effect<DeliveryStatus, AdminRequestError>
  }
>()("Relay/AdminClient") {}

const configuration = Config.all({
  baseUrl: Config.url("RELAY_ADMIN_URL"),
  token: Config.redacted("RELAY_OPERATIONS_AUTHORIZATION"),
})

export const RelayAdminClientLive = Layer.effect(
  RelayAdminClient,
  Effect.gen(function* () {
    const { baseUrl, token } = yield* configuration
    const client = yield* HttpApiClient.make(
      RelayOperationsHttpApi,
      { baseUrl },
    ).pipe(
      Effect.provide(operationsAuthorizationClientLayer(token)),
    )

    const listDeadLetters = Effect.fn(
      "RelayAdminClient.listDeadLetters",
    )(() =>
      client.operations.listDeadLetters().pipe(
        Effect.mapError((cause) => new AdminRequestError({
          operation: "listDeadLetters",
          cause,
        })),
      ))

    const retryDeadLetter = Effect.fn(
      "RelayAdminClient.retryDeadLetter",
    )((deliveryId: DeliveryId) =>
      client.operations.retryDeadLetter({
        params: { deliveryId },
      }).pipe(
        Effect.mapError((cause) => new AdminRequestError({
          operation: "retryDeadLetter",
          cause,
        })),
      ))

    return RelayAdminClient.of({ listDeadLetters, retryDeadLetter })
  }),
)

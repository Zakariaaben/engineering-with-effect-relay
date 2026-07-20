import { Effect, Layer } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import {
  DestinationClient,
  type DeliveryRequest,
} from "../destination.ts"
import { DeliveryTransportError } from "../errors.ts"

export const DestinationClientLive = Layer.effect(
  DestinationClient,
  Effect.gen(function* () {
    const httpClient = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withScope,
    )

    const post = Effect.fn("DestinationClient.post")(
      function* (request: DeliveryRequest) {
        const httpRequest = HttpClientRequest.post(
          request.endpoint,
        ).pipe(
          HttpClientRequest.bearerToken(request.authorization),
          HttpClientRequest.setHeader(
            "Idempotency-Key",
            `"${request.deliveryId}"`,
          ),
          HttpClientRequest.bodyText(
            request.body,
            "application/json",
          ),
        )

        return yield* Effect.scoped(
          Effect.gen(function* () {
            const response = yield* httpClient.execute(httpRequest)
            const retryAfter = response.headers["retry-after"]

            return retryAfter === undefined
              ? { status: response.status }
              : { status: response.status, retryAfter }
          }),
        ).pipe(
          Effect.mapError((cause) =>
            new DeliveryTransportError({
              deliveryId: request.deliveryId,
              destinationId: request.destinationId,
              cause,
            })
          ),
        )
      },
    )

    return DestinationClient.of({ post })
  }),
)

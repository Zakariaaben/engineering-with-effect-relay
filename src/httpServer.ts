import { Effect, FileSystem, Layer, Schema } from "effect"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import type { DeliveryResult } from "./model.ts"
import {
  DeliveryId,
  DestinationId,
  RelayEvent,
} from "./model.ts"

const maxEventBodySize = FileSystem.KiB(16)

export interface DeliveryHttpResult extends
  Schema.Schema.Type<typeof DeliveryHttpResult> {}

export const DeliveryHttpResult = Schema.Struct({
  deliveryId: DeliveryId,
  destinationId: DestinationId,
  outcome: Schema.Literals([
    "Delivered",
    "Rejected",
    "ProtocolFailure",
    "Exhausted",
  ]),
})

export interface DeliveryHttpProblem extends
  Schema.Schema.Type<typeof DeliveryHttpProblem> {}

export const DeliveryHttpProblem = Schema.Struct({
  error: Schema.Literals([
    "invalid_event",
    "unsupported_media_type",
    "overloaded",
    "internal_error",
  ]),
})

const encodeResult = HttpServerResponse.schemaJson(DeliveryHttpResult)
const encodeProblem = HttpServerResponse.schemaJson(DeliveryHttpProblem)

const resultResponse = (result: DeliveryResult) =>
  encodeResult(
    DeliveryHttpResult.make({
      deliveryId: result.deliveryId,
      destinationId: result.destinationId,
      outcome: result._tag,
    }),
    { status: 200 },
  ).pipe(Effect.orDie)

const problemResponse = (
  error: DeliveryHttpProblem["error"],
  status: number,
) =>
  encodeProblem(
    DeliveryHttpProblem.make({ error }),
    { status },
  ).pipe(Effect.orDie)

const isJson = (contentType: string | undefined): boolean =>
  contentType
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase() === "application/json"

export const handleDeliveryHttpRequest = Effect.fn(
  "DeliveryHttp.handleRequest",
)(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest
  if (!isJson(request.headers["content-type"])) {
    return yield* problemResponse("unsupported_media_type", 415)
  }

  const event = yield* HttpServerRequest.schemaBodyJson(RelayEvent).pipe(
    Effect.provideService(
      HttpServerRequest.MaxBodySize,
      maxEventBodySize,
    ),
  )
  const supervisor = yield* DeliverySupervisor
  const result = yield* supervisor.deliver(event)
  return yield* resultResponse(result)
})

const handledDeliveryRequest = handleDeliveryHttpRequest().pipe(
  Effect.catchTags({
    DeliveryIdentityError: () =>
      problemResponse("internal_error", 500),
    DeliveryOverloaded: () =>
      problemResponse("overloaded", 503),
    HttpServerError: () =>
      problemResponse("invalid_event", 400),
    InvalidEventError: () =>
      problemResponse("invalid_event", 400),
    SchemaError: () =>
      problemResponse("invalid_event", 400),
  }),
)

const ResponsePolicy = HttpRouter.middleware(
  (httpEffect) =>
    Effect.map(httpEffect, (response) =>
      response.pipe(
        HttpServerResponse.setHeader("cache-control", "no-store"),
        HttpServerResponse.setHeader(
          "x-content-type-options",
          "nosniff",
        ),
      )),
  { global: true },
)

export const DeliveryHttpRoutes = Layer.mergeAll(
  HttpRouter.add(
    "POST",
    "/deliveries",
    handledDeliveryRequest,
  ),
  ResponsePolicy,
)

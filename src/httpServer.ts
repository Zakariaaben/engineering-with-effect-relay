import { timingSafeEqual } from "node:crypto"
import {
  Config,
  Context,
  Effect,
  FileSystem,
  Layer,
  Redacted,
  Schema,
} from "effect"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerError from "effect/unstable/http/HttpServerError"
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import * as HttpApi from "effect/unstable/httpapi/HttpApi"
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder"
import { HttpApiSchemaError } from "effect/unstable/httpapi/HttpApiError"
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint"
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup"
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware"
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity"
import * as OpenApi from "effect/unstable/httpapi/OpenApi"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import type { DeliveryResult } from "./model.ts"
import {
  DeliveryId,
  DestinationId,
  RelayEvent,
} from "./model.ts"
import { RelayReadiness } from "./readiness.ts"

const maxEventBodySize = FileSystem.KiB(16)

const isJson = (contentType: string | undefined): boolean =>
  contentType
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase() === "application/json"

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
}).annotate({ identifier: "DeliveryResult" })

export class InvalidEventProblem extends
  Schema.ErrorClass<InvalidEventProblem>("Relay/InvalidEventProblem")({
    error: Schema.Literal("invalid_event"),
  }, {
    description: "The request body is not a valid Relay event.",
    httpApiStatus: 400,
  })
{}

export class UnauthorizedProblem extends
  Schema.ErrorClass<UnauthorizedProblem>("Relay/UnauthorizedProblem")({
    error: Schema.Literal("unauthorized"),
  }, {
    description: "The bearer credential is missing or invalid.",
    httpApiStatus: 401,
  })
{}

export class UnsupportedMediaTypeProblem extends
  Schema.ErrorClass<UnsupportedMediaTypeProblem>(
    "Relay/UnsupportedMediaTypeProblem",
  )({
    error: Schema.Literal("unsupported_media_type"),
  }, {
    description: "The request body must use application/json.",
    httpApiStatus: 415,
  })
{}

export class DeliveryOverloadedProblem extends
  Schema.ErrorClass<DeliveryOverloadedProblem>(
    "Relay/DeliveryOverloadedProblem",
  )({
    error: Schema.Literal("overloaded"),
  }, {
    description: "Relay has no admission capacity for this delivery.",
    httpApiStatus: 503,
  })
{}

export class RelayNotReadyProblem extends
  Schema.ErrorClass<RelayNotReadyProblem>(
    "Relay/RelayNotReadyProblem",
  )({
    error: Schema.Literal("not_ready"),
  }, {
    description: "Relay is not accepting new delivery work.",
    httpApiStatus: 503,
  })
{}

export class DeliveryInternalProblem extends
  Schema.ErrorClass<DeliveryInternalProblem>(
    "Relay/DeliveryInternalProblem",
  )({
    error: Schema.Literal("internal_error"),
  }, {
    description: "Relay could not create or persist the delivery.",
    httpApiStatus: 500,
  })
{}

export class RequestContract extends
  HttpApiMiddleware.Service<RequestContract>()(
    "Relay/Http/RequestContract",
    {
      error: [InvalidEventProblem, UnsupportedMediaTypeProblem],
    },
  )
{}

export class DeliveryAuthorization extends
  HttpApiMiddleware.Service<DeliveryAuthorization>()(
    "Relay/Http/DeliveryAuthorization",
    {
      error: UnauthorizedProblem,
      requiredForClient: true,
      security: {
        bearerAuth: HttpApiSecurity.bearer,
      },
    },
  )
{}

export class IntakeAuthorization extends
  Context.Service<IntakeAuthorization, {
    readonly token: Redacted.Redacted
  }>()("Relay/Http/IntakeAuthorization")
{}

export const IntakeAuthorizationLive = Layer.effect(
  IntakeAuthorization,
  Config.redacted("RELAY_INTAKE_AUTHORIZATION").pipe(
    Config.map((token) => IntakeAuthorization.of({ token })),
  ),
)

const sameSecret = (
  left: Redacted.Redacted,
  right: Redacted.Redacted,
): boolean => {
  const leftBytes = Buffer.from(Redacted.value(left))
  const rightBytes = Buffer.from(Redacted.value(right))
  return leftBytes.byteLength === rightBytes.byteLength &&
    timingSafeEqual(leftBytes, rightBytes)
}

export const RequestContractLive = Layer.succeed(
  RequestContract,
  RequestContract.of((httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest
      if (!isJson(request.headers["content-type"])) {
        return yield* Effect.fail(
          new UnsupportedMediaTypeProblem({
            error: "unsupported_media_type",
          }),
        )
      }
      return yield* httpEffect
    }).pipe(
      Effect.provideService(
        HttpServerRequest.MaxBodySize,
        maxEventBodySize,
      ),
      Effect.catchIf(
        HttpApiSchemaError.is,
        () => Effect.fail(new InvalidEventProblem({
          error: "invalid_event",
        })),
      ),
      Effect.catchDefect((defect) =>
        HttpServerError.isHttpServerError(defect) &&
          defect.reason._tag === "RequestParseError"
          ? Effect.fail(new InvalidEventProblem({
              error: "invalid_event",
            }))
          : Effect.die(defect)
      ),
      Effect.flatMap((response) =>
        response.status === 415
          ? Effect.fail(new UnsupportedMediaTypeProblem({
              error: "unsupported_media_type",
            }))
          : Effect.succeed(response)
      ),
    )
  ),
)

export const DeliveryAuthorizationLive = Layer.effect(
  DeliveryAuthorization,
  Effect.map(IntakeAuthorization, ({ token }) =>
    DeliveryAuthorization.of({
      bearerAuth: (httpEffect, { credential }) =>
        sameSecret(credential, token)
          ? httpEffect
          : Effect.fail(new UnauthorizedProblem({
              error: "unauthorized",
            })),
    })
  ),
)

export const deliveryAuthorizationClientLayer = (
  token: Redacted.Redacted,
) =>
  HttpApiMiddleware.layerClient(
    DeliveryAuthorization,
    ({ next, request }) =>
      next(HttpClientRequest.bearerToken(request, token)),
  )

export const SubmitDelivery = HttpApiEndpoint.post(
  "submit",
  "/deliveries",
  {
    payload: RelayEvent,
    success: DeliveryHttpResult,
    error: [
      DeliveryOverloadedProblem,
      RelayNotReadyProblem,
      DeliveryInternalProblem,
    ],
  },
).pipe(
  (endpoint) => endpoint.middleware(RequestContract),
  (endpoint) => endpoint.middleware(DeliveryAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Deliver one event to the configured destination",
  ),
)

export const RelayHttpApi = HttpApi.make("RelayApi").add(
  HttpApiGroup.make("deliveries").add(SubmitDelivery),
).pipe(
  (api) => api.annotate(OpenApi.Title, "Relay intake API"),
  (api) => api.annotate(OpenApi.Version, "1.0.0"),
)

const toHttpResult = (
  result: DeliveryResult,
): DeliveryHttpResult =>
  DeliveryHttpResult.make({
    deliveryId: result.deliveryId,
    destinationId: result.destinationId,
    outcome: result._tag,
  })

export const DeliveryHttpHandlers = HttpApiBuilder.group(
  RelayHttpApi,
  "deliveries",
  (handlers) =>
    handlers.handle("submit", ({ payload }) =>
      Effect.gen(function* () {
        const readiness = yield* RelayReadiness
        if (!(yield* readiness.current)) {
          return yield* Effect.fail(
            new RelayNotReadyProblem({ error: "not_ready" }),
          )
        }
        const supervisor = yield* DeliverySupervisor
        return yield* supervisor.deliver(payload).pipe(
          Effect.map(toHttpResult),
          Effect.catchTags({
            DeliveryIdentityError: () =>
              Effect.fail(new DeliveryInternalProblem({
                error: "internal_error",
              })),
            DeliveryOverloaded: () =>
              Effect.fail(new DeliveryOverloadedProblem({
                error: "overloaded",
              })),
            InvalidEventError: () =>
              Effect.fail(new InvalidEventProblem({
                error: "invalid_event",
              })),
            RelayIntakeStoreError: () =>
              Effect.fail(new DeliveryInternalProblem({
                error: "internal_error",
              })),
          }),
        )
      }))
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

const ReadinessResponse = Schema.Struct({
  status: Schema.Literals(["ready", "not_ready"]),
})
const encodeReadiness = HttpServerResponse.schemaJson(
  ReadinessResponse,
)

const ReadinessHttpRoute = HttpRouter.add(
  "GET",
  "/health/ready",
  Effect.gen(function* () {
    const readiness = yield* RelayReadiness
    const ready = yield* readiness.current
    return yield* encodeReadiness(
      { status: ready ? "ready" : "not_ready" },
      { status: ready ? 200 : 503 },
    ).pipe(Effect.orDie)
  }),
)

export const DeliveryHttpRoutes = Layer.mergeAll(
  HttpApiBuilder.layer(RelayHttpApi, {
    openapiPath: "/openapi.json",
  }).pipe(
    Layer.provide(DeliveryHttpHandlers),
    Layer.provide(RequestContractLive),
    Layer.provide(DeliveryAuthorizationLive),
  ),
  ReadinessHttpRoute,
  ResponsePolicy,
)

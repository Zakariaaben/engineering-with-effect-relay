import { type Redacted, Schema } from "effect"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpApi from "effect/unstable/httpapi/HttpApi"
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint"
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup"
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware"
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity"
import * as OpenApi from "effect/unstable/httpapi/OpenApi"
import { DeliveryStatus } from "./delivery.ts"
import { DeliveryId } from "./identifiers.ts"
import { ReconciliationReport } from "./reconciler.ts"

export class UnauthorizedProblem extends
  Schema.ErrorClass<UnauthorizedProblem>("Relay/UnauthorizedProblem")({
    error: Schema.Literal("unauthorized"),
  }, {
    description: "The bearer credential is missing or invalid.",
    httpApiStatus: 401,
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

export class DeliveryNotFoundProblem extends
  Schema.ErrorClass<DeliveryNotFoundProblem>(
    "Relay/DeliveryNotFoundProblem",
  )({
    error: Schema.Literal("delivery_not_found"),
  }, {
    description: "No delivery exists with that identifier.",
    httpApiStatus: 404,
  })
{}

export class DeadLetterStateProblem extends
  Schema.ErrorClass<DeadLetterStateProblem>(
    "Relay/DeadLetterStateProblem",
  )({
    error: Schema.Literal("not_dead_lettered"),
  }, {
    description: "The delivery is no longer dead-lettered.",
    httpApiStatus: 409,
  })
{}

export class DestinationMismatchProblem extends
  Schema.ErrorClass<DestinationMismatchProblem>(
    "Relay/DestinationMismatchProblem",
  )({
    error: Schema.Literal("destination_mismatch"),
  }, {
    description:
      "The configured repair destination does not own this delivery.",
    httpApiStatus: 409,
  })
{}

export class OperationsAuthorization extends
  HttpApiMiddleware.Service<OperationsAuthorization>()(
    "Relay/Http/OperationsAuthorization",
    {
      error: UnauthorizedProblem,
      requiredForClient: true,
      security: {
        operationsBearerAuth: HttpApiSecurity.bearer,
      },
    },
  )
{}

export const operationsAuthorizationClientLayer = (
  token: Redacted.Redacted,
) =>
  HttpApiMiddleware.layerClient(
    OperationsAuthorization,
    ({ next, request }) =>
      next(HttpClientRequest.bearerToken(request, token)),
  )

export const InspectDelivery = HttpApiEndpoint.get(
  "inspect",
  "/operations/deliveries/:deliveryId",
  {
    params: { deliveryId: DeliveryId },
    success: DeliveryStatus,
    error: [DeliveryNotFoundProblem, DeliveryInternalProblem],
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Inspect one delivery and its durable attempt history",
  ),
)

export const ListDeadLetters = HttpApiEndpoint.get(
  "listDeadLetters",
  "/operations/dead-letters",
  {
    success: Schema.Array(DeliveryStatus),
    error: DeliveryInternalProblem,
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "List the first 50 dead-lettered deliveries",
  ),
)

export const RetryDeadLetter = HttpApiEndpoint.post(
  "retryDeadLetter",
  "/operations/dead-letters/:deliveryId/retry",
  {
    params: { deliveryId: DeliveryId },
    success: DeliveryStatus,
    error: [
      DeliveryNotFoundProblem,
      DeadLetterStateProblem,
      DeliveryInternalProblem,
    ],
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Return a dead letter to pending with its retained route",
  ),
)

export const RepairDeadLetter = HttpApiEndpoint.post(
  "repairDeadLetter",
  "/operations/dead-letters/:deliveryId/repair",
  {
    params: { deliveryId: DeliveryId },
    success: DeliveryStatus,
    error: [
      DeliveryNotFoundProblem,
      DeadLetterStateProblem,
      DestinationMismatchProblem,
      DeliveryInternalProblem,
    ],
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Replace a dead letter's route with the current trusted route",
  ),
)

export const TerminateDeadLetter = HttpApiEndpoint.post(
  "terminateDeadLetter",
  "/operations/dead-letters/:deliveryId/terminate",
  {
    params: { deliveryId: DeliveryId },
    success: DeliveryStatus,
    error: [
      DeliveryNotFoundProblem,
      DeadLetterStateProblem,
      DeliveryInternalProblem,
    ],
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Terminate a dead letter while retaining its history",
  ),
)

export const ReconcileDeliveries = HttpApiEndpoint.post(
  "reconcile",
  "/operations/reconcile",
  {
    success: ReconciliationReport,
    error: DeliveryInternalProblem,
  },
).pipe(
  (endpoint) => endpoint.middleware(OperationsAuthorization),
  (endpoint) => endpoint.annotate(
    OpenApi.Summary,
    "Run one bounded reconciliation pass",
  ),
)

export const RelayOperationsApiGroup = HttpApiGroup.make("operations")
  .add(InspectDelivery)
  .add(ListDeadLetters)
  .add(RetryDeadLetter)
  .add(RepairDeadLetter)
  .add(TerminateDeadLetter)
  .add(ReconcileDeliveries)

export const RelayOperationsHttpApi = HttpApi.make(
  "RelayOperationsApi",
).add(RelayOperationsApiGroup)

import { Context, Effect, Schema } from "effect"
import { DeliveryTransportError } from "./errors.ts"
import {
  ConfigurationVersion,
  DeliveryId,
  DestinationId,
} from "./identifiers.ts"

export interface Destination extends Schema.Schema.Type<typeof Destination> {}

export const Destination = Schema.Struct({
  id: DestinationId,
  endpoint: Schema.URL,
  authorization: Schema.Redacted(Schema.String),
})

export interface DeliveryRouteSnapshot extends
  Schema.Schema.Type<typeof DeliveryRouteSnapshot> {}

export const DeliveryRouteSnapshot = Schema.Struct({
  destinationId: DestinationId,
  endpoint: Schema.URL,
  configurationVersion: ConfigurationVersion,
})

export interface DeliveryRequest {
  readonly deliveryId: DeliveryId
  readonly destinationId: DestinationId
  readonly endpoint: URL
  readonly authorization: Destination["authorization"]
  readonly body: string
}

export interface DeliveryResponseEvidence {
  readonly status: number
  readonly retryAfter?: string
}

export class DestinationClient extends Context.Service<DestinationClient, {
  readonly post: (
    request: DeliveryRequest,
  ) => Effect.Effect<
    DeliveryResponseEvidence,
    DeliveryTransportError
  >
}>()("Relay/DestinationClient") {}

export type DestinationClientService =
  Context.Service.Shape<typeof DestinationClient>

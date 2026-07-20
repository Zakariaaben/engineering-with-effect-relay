import { Context, Data, Effect } from "effect"
import type { RelayEvent } from "./command.ts"
import type { Delivery, DeliveryClaim } from "./delivery.ts"
import type { DeliveryRouteSnapshot } from "./destination.ts"
import type { ClaimedDelivery } from "./deliveryRepository.ts"
import {
  IngestionConflictError,
  RelayIntakeStoreError,
} from "./errors.ts"
import type {
  DeliveryId,
  DestinationId,
  IngestionKey,
  RequestFingerprint,
  WorkerId,
} from "./identifiers.ts"

export interface ClaimRequest {
  readonly ownerId: WorkerId
  readonly leaseDurationMillis: number
}

export interface IntakeRecord {
  readonly ingestionKey: IngestionKey
  readonly requestFingerprint: RequestFingerprint
  readonly event: RelayEvent
  readonly deliveryId: DeliveryId
  readonly route: DeliveryRouteSnapshot
  readonly acceptedAtMillis: number
  readonly claim: ClaimRequest
}

export interface IntakeDecisionFields {
  readonly event: RelayEvent
  readonly delivery: Delivery
  readonly route: DeliveryRouteSnapshot
  readonly acceptedAtMillis: number
}

export interface AcceptedIntakeDecisionFields extends IntakeDecisionFields {
  readonly claim: DeliveryClaim
}

export type IntakeDecision = Data.TaggedEnum<{
  Accepted: AcceptedIntakeDecisionFields
  Replay: IntakeDecisionFields
}>

export const IntakeDecision = Data.taggedEnum<IntakeDecision>()

export class RelayIntakeStore extends Context.Service<RelayIntakeStore, {
  readonly accept: (
    record: IntakeRecord,
  ) => Effect.Effect<
    IntakeDecision,
    IngestionConflictError | RelayIntakeStoreError
  >
  readonly savePending: (
    event: RelayEvent,
    deliveryId: DeliveryId,
    destinationId: DestinationId,
    claim: ClaimRequest,
  ) => Effect.Effect<ClaimedDelivery, RelayIntakeStoreError>
}>()("Relay/RelayIntakeStore") {}

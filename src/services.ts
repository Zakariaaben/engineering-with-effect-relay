import { Context, Data, Effect, Option } from "effect"
import {
  DeliveryRepositoryError,
  IngestionConflictError,
  RelayIntakeStoreError,
} from "./errors.ts"
import type {
  Delivery,
  DeliveryId,
  DeliveryResult,
  DeliveryRouteSnapshot,
  DestinationId,
  IngestionKey,
  RequestFingerprint,
  RelayEvent,
} from "./model.ts"

export interface ClaimedDelivery {
  readonly delivery: Delivery
  readonly event: RelayEvent
  readonly route: Option.Option<DeliveryRouteSnapshot>
}

export interface IntakeRecord {
  readonly ingestionKey: IngestionKey
  readonly requestFingerprint: RequestFingerprint
  readonly event: RelayEvent
  readonly deliveryId: DeliveryId
  readonly route: DeliveryRouteSnapshot
  readonly acceptedAtMillis: number
}

export interface IntakeDecisionFields {
  readonly event: RelayEvent
  readonly delivery: Delivery
  readonly route: DeliveryRouteSnapshot
  readonly acceptedAtMillis: number
}

export type IntakeDecision = Data.TaggedEnum<{
  Accepted: IntakeDecisionFields
  Replay: IntakeDecisionFields
}>

export const IntakeDecision = Data.taggedEnum<IntakeDecision>()

export class DeliveryRepository extends Context.Service<DeliveryRepository, {
  readonly save: (
    delivery: Delivery,
  ) => Effect.Effect<void, DeliveryRepositoryError>
  readonly findById: (
    id: DeliveryId,
  ) => Effect.Effect<Option.Option<Delivery>, DeliveryRepositoryError>
  readonly resetClaims: () => Effect.Effect<void, DeliveryRepositoryError>
  readonly claimPending: (
    destinationId: DestinationId,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<ClaimedDelivery>, DeliveryRepositoryError>
  readonly completeClaim: (
    deliveryId: DeliveryId,
    result: DeliveryResult,
  ) => Effect.Effect<void, DeliveryRepositoryError>
  readonly releaseClaim: (
    deliveryId: DeliveryId,
  ) => Effect.Effect<void, DeliveryRepositoryError>
}>()("Relay/DeliveryRepository") {}

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
  ) => Effect.Effect<Delivery, RelayIntakeStoreError>
}>()("Relay/RelayIntakeStore") {}

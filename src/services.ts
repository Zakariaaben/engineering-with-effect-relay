import { Context, Data, Effect, Option } from "effect"
import {
  ClaimLostError,
  DeadLetterDestinationMismatchError,
  DeadLetterRecoveryError,
  DeliveryRepositoryError,
  IngestionConflictError,
  RelayIntakeStoreError,
} from "./errors.ts"
import type {
  Delivery,
  DeliveryAttemptRecord,
  DeliveryClaim,
  DeliveryId,
  DeliveryResult,
  DeliveryRouteSnapshot,
  DeliveryStatus,
  DestinationId,
  IngestionKey,
  RequestFingerprint,
  RelayEvent,
  WorkerId,
} from "./model.ts"

export interface ClaimedDelivery {
  readonly claim: DeliveryClaim
  readonly delivery: Delivery
  readonly event: RelayEvent
  readonly claimLagMillis: number
  readonly nextAttemptOrdinal: number
  readonly route: Option.Option<DeliveryRouteSnapshot>
}

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

export class DeliveryRepository extends Context.Service<DeliveryRepository, {
  readonly save: (
    delivery: Delivery,
  ) => Effect.Effect<void, DeliveryRepositoryError>
  readonly findById: (
    id: DeliveryId,
  ) => Effect.Effect<Option.Option<Delivery>, DeliveryRepositoryError>
  readonly findStatus: (
    id: DeliveryId,
  ) => Effect.Effect<Option.Option<DeliveryStatus>, DeliveryRepositoryError>
  readonly recordAttempt: (
    attempt: DeliveryAttemptRecord,
  ) => Effect.Effect<void, ClaimLostError | DeliveryRepositoryError>
  readonly listDeadLetters: (
    limit: number,
  ) => Effect.Effect<ReadonlyArray<DeliveryStatus>, DeliveryRepositoryError>
  readonly retryDeadLetter: (
    id: DeliveryId,
  ) => Effect.Effect<
    void,
    DeadLetterRecoveryError | DeliveryRepositoryError
  >
  readonly repairDeadLetter: (
    id: DeliveryId,
    route: DeliveryRouteSnapshot,
  ) => Effect.Effect<
    void,
    | DeadLetterDestinationMismatchError
    | DeadLetterRecoveryError
    | DeliveryRepositoryError
  >
  readonly terminateDeadLetter: (
    id: DeliveryId,
  ) => Effect.Effect<
    void,
    DeadLetterRecoveryError | DeliveryRepositoryError
  >
  readonly claimPending: (
    ownerId: WorkerId,
    destinationId: DestinationId,
    limit: number,
    leaseDurationMillis: number,
  ) => Effect.Effect<ReadonlyArray<ClaimedDelivery>, DeliveryRepositoryError>
  readonly renewClaim: (
    deliveryId: DeliveryId,
    claim: DeliveryClaim,
    leaseDurationMillis: number,
  ) => Effect.Effect<
    DeliveryClaim,
    ClaimLostError | DeliveryRepositoryError
  >
  readonly completeClaim: (
    deliveryId: DeliveryId,
    claim: DeliveryClaim,
    result: DeliveryResult,
  ) => Effect.Effect<void, ClaimLostError | DeliveryRepositoryError>
  readonly releaseClaim: (
    deliveryId: DeliveryId,
    claim: DeliveryClaim,
  ) => Effect.Effect<void, ClaimLostError | DeliveryRepositoryError>
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
    claim: ClaimRequest,
  ) => Effect.Effect<ClaimedDelivery, RelayIntakeStoreError>
}>()("Relay/RelayIntakeStore") {}

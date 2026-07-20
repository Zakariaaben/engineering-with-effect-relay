import { Context, Effect, Option } from "effect"
import type { RelayEvent } from "./command.ts"
import type {
  Delivery,
  DeliveryAttemptRecord,
  DeliveryClaim,
  DeliveryResult,
  DeliveryStatus,
} from "./delivery.ts"
import type { DeliveryRouteSnapshot } from "./destination.ts"
import {
  ClaimLostError,
  DeadLetterDestinationMismatchError,
  DeadLetterRecoveryError,
  DeliveryRepositoryError,
} from "./errors.ts"
import type {
  DeliveryId,
  DestinationId,
  WorkerId,
} from "./identifiers.ts"

export interface ClaimedDelivery {
  readonly claim: DeliveryClaim
  readonly delivery: Delivery
  readonly event: RelayEvent
  readonly claimLagMillis: number
  readonly nextAttemptOrdinal: number
  readonly route: Option.Option<DeliveryRouteSnapshot>
}

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

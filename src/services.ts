import { Context, Effect, Option } from "effect"
import {
  DeliveryRepositoryError,
  RelayIntakeStoreError,
} from "./errors.ts"
import type {
  Delivery,
  DeliveryId,
  DeliveryResult,
  DestinationId,
  RelayEvent,
} from "./model.ts"

export interface ClaimedDelivery {
  readonly delivery: Delivery
  readonly event: RelayEvent
}

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
  readonly savePending: (
    event: RelayEvent,
    deliveryId: DeliveryId,
    destinationId: DestinationId,
  ) => Effect.Effect<Delivery, RelayIntakeStoreError>
}>()("Relay/RelayIntakeStore") {}

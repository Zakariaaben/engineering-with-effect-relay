import { Context, Effect, Option } from "effect"
import {
  DeliveryRepositoryError,
  RelayIntakeStoreError,
} from "./errors.ts"
import type {
  Delivery,
  DeliveryId,
  DestinationId,
  RelayEvent,
} from "./model.ts"

export class DeliveryRepository extends Context.Service<DeliveryRepository, {
  readonly save: (
    delivery: Delivery,
  ) => Effect.Effect<void, DeliveryRepositoryError>
  readonly findById: (
    id: DeliveryId,
  ) => Effect.Effect<Option.Option<Delivery>, DeliveryRepositoryError>
}>()("Relay/DeliveryRepository") {}

export class RelayIntakeStore extends Context.Service<RelayIntakeStore, {
  readonly savePending: (
    event: RelayEvent,
    deliveryId: DeliveryId,
    destinationId: DestinationId,
  ) => Effect.Effect<Delivery, RelayIntakeStoreError>
}>()("Relay/RelayIntakeStore") {}

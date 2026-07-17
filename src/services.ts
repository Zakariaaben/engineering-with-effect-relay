import { Context, Effect, Option } from "effect"
import { DeliveryRepositoryError } from "./errors.ts"
import type {
  Delivery,
  DeliveryId,
} from "./model.ts"

export class DeliveryRepository extends Context.Service<DeliveryRepository, {
  readonly save: (
    delivery: Delivery,
  ) => Effect.Effect<void, DeliveryRepositoryError>
  readonly findById: (
    id: DeliveryId,
  ) => Effect.Effect<Option.Option<Delivery>, DeliveryRepositoryError>
}>()("Relay/DeliveryRepository") {}

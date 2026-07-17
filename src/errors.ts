import { Schema } from "effect"
import { DeliveryId, DestinationId } from "./model.ts"

export class InvalidEventError extends Schema.TaggedErrorClass<InvalidEventError>()(
  "InvalidEventError",
  {
    summary: Schema.String,
  },
) {}

export class DeliveryTransportError extends Schema.TaggedErrorClass<DeliveryTransportError>()(
  "DeliveryTransportError",
  {
    deliveryId: DeliveryId,
    destinationId: DestinationId,
    cause: Schema.Unknown,
  },
) {}

export class DeliveryIdentityError extends Schema.TaggedErrorClass<DeliveryIdentityError>()(
  "DeliveryIdentityError",
  {
    destinationId: DestinationId,
    cause: Schema.Unknown,
  },
) {}

export class DeliveryRepositoryError extends Schema.TaggedErrorClass<DeliveryRepositoryError>()(
  "DeliveryRepositoryError",
  {
    operation: Schema.Literals(["save", "findById"]),
    cause: Schema.Unknown,
  },
) {}

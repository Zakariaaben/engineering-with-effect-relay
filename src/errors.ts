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

export class DeliveryOverloaded extends Schema.TaggedErrorClass<DeliveryOverloaded>()(
  "DeliveryOverloaded",
  {
    admissionCapacity: Schema.Int.check(Schema.isGreaterThan(0)),
    destinationId: DestinationId,
  },
) {}

export class DeliveryRepositoryError extends Schema.TaggedErrorClass<DeliveryRepositoryError>()(
  "DeliveryRepositoryError",
  {
    operation: Schema.Literals([
      "save",
      "findById",
      "resetClaims",
      "claimPending",
      "completeClaim",
      "releaseClaim",
    ]),
    cause: Schema.Unknown,
  },
) {}

export class RelayIntakeStoreError extends Schema.TaggedErrorClass<RelayIntakeStoreError>()(
  "RelayIntakeStoreError",
  {
    operation: Schema.Literal("savePending"),
    cause: Schema.Unknown,
  },
) {}

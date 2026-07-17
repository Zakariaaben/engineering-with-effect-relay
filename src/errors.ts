import { Schema } from "effect"
import { DestinationId } from "./model.ts"

export class InvalidEventError extends Schema.TaggedErrorClass<InvalidEventError>()(
  "InvalidEventError",
  {
    summary: Schema.String,
  },
) {}

export class DeliveryTransportError extends Schema.TaggedErrorClass<DeliveryTransportError>()(
  "DeliveryTransportError",
  {
    destinationId: DestinationId,
    cause: Schema.Unknown,
  },
) {}

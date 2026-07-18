import { Schema } from "effect"
import {
  ClaimGeneration,
  DeliveryId,
  DestinationId,
  EventId,
  IngestionKey,
  WorkerId,
} from "./model.ts"

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

export class EventIdentityError extends Schema.TaggedErrorClass<EventIdentityError>()(
  "EventIdentityError",
  {
    operation: Schema.Literals([
      "fingerprint",
      "eventId",
      "deliveryId",
    ]),
    cause: Schema.Unknown,
  },
) {}

export class IngestionConflictError extends Schema.TaggedErrorClass<IngestionConflictError>()(
  "IngestionConflictError",
  {
    ingestionKey: IngestionKey,
    existingEventId: EventId,
  },
) {}

export class DeliveryOverloaded extends Schema.TaggedErrorClass<DeliveryOverloaded>()(
  "DeliveryOverloaded",
  {
    admissionCapacity: Schema.Int.check(Schema.isGreaterThan(0)),
    destinationId: DestinationId,
    limit: Schema.Literals([
      "GlobalAdmission",
      "DestinationAdmission",
    ]),
  },
) {}

export class ClaimLostError extends Schema.TaggedErrorClass<ClaimLostError>()(
  "ClaimLostError",
  {
    deliveryId: DeliveryId,
    ownerId: WorkerId,
    generation: ClaimGeneration,
    operation: Schema.Literals([
      "renew",
      "recordAttempt",
      "complete",
      "release",
    ]),
  },
) {}

export class DeadLetterRecoveryError extends
  Schema.TaggedErrorClass<DeadLetterRecoveryError>()(
    "DeadLetterRecoveryError",
    {
      deliveryId: DeliveryId,
      reason: Schema.Literals([
        "NotFound",
        "NotDeadLettered",
      ]),
    },
  ) {}

export class DeadLetterDestinationMismatchError extends
  Schema.TaggedErrorClass<DeadLetterDestinationMismatchError>()(
    "DeadLetterDestinationMismatchError",
    {
      deliveryId: DeliveryId,
      deliveryDestinationId: DestinationId,
      repairDestinationId: DestinationId,
    },
  ) {}

export class DeliveryRepositoryError extends Schema.TaggedErrorClass<DeliveryRepositoryError>()(
  "DeliveryRepositoryError",
  {
    operation: Schema.Literals([
      "save",
      "findById",
      "findStatus",
      "recordAttempt",
      "listDeadLetters",
      "retryDeadLetter",
      "repairDeadLetter",
      "terminateDeadLetter",
      "claimPending",
      "renewClaim",
      "completeClaim",
      "releaseClaim",
    ]),
    cause: Schema.Unknown,
  },
) {}

export class RelayIntakeStoreError extends Schema.TaggedErrorClass<RelayIntakeStoreError>()(
  "RelayIntakeStoreError",
  {
    operation: Schema.Literals(["savePending", "accept"]),
    cause: Schema.Unknown,
  },
) {}

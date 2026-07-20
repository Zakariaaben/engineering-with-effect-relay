import { Crypto, Effect, Schema } from "effect"

export const EventId = Schema.String.check(
  Schema.isPattern(/^evt-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("EventId"))
export type EventId = Schema.Schema.Type<typeof EventId>

export const DeliveryId = Schema.String.check(
  Schema.isPattern(/^dlv-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("DeliveryId"))
export type DeliveryId = Schema.Schema.Type<typeof DeliveryId>

export const WorkerId = Schema.String.check(
  Schema.isPattern(/^wrk-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("WorkerId"))
export type WorkerId = Schema.Schema.Type<typeof WorkerId>

export const ClaimGeneration = Schema.Int.check(
  Schema.isGreaterThan(0),
).pipe(Schema.brand("ClaimGeneration"))
export type ClaimGeneration = Schema.Schema.Type<typeof ClaimGeneration>

export const DestinationId = Schema.String.check(
  Schema.isPattern(/^dst-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("DestinationId"))
export type DestinationId = Schema.Schema.Type<typeof DestinationId>

export const IngestionKey = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/),
).pipe(Schema.brand("IngestionKey"))
export type IngestionKey = Schema.Schema.Type<typeof IngestionKey>

export const RequestFingerprint = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{64}$/),
).pipe(Schema.brand("RequestFingerprint"))
export type RequestFingerprint = Schema.Schema.Type<
  typeof RequestFingerprint
>

export const ConfigurationVersion = Schema.Int.check(
  Schema.isGreaterThan(0),
).pipe(Schema.brand("ConfigurationVersion"))
export type ConfigurationVersion = Schema.Schema.Type<
  typeof ConfigurationVersion
>

export const InvoiceId = Schema.String.check(
  Schema.isPattern(/^inv-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("InvoiceId"))
export type InvoiceId = Schema.Schema.Type<typeof InvoiceId>

export const AmountCents = Schema.Int.check(
  Schema.isGreaterThan(0),
).pipe(Schema.brand("AmountCents"))
export type AmountCents = Schema.Schema.Type<typeof AmountCents>

export const generateEventId = Effect.fn("Relay.generateEventId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return EventId.make(`evt-${uuid}`)
  },
)

export const generateDeliveryId = Effect.fn("Relay.generateDeliveryId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return DeliveryId.make(`dlv-${uuid}`)
  },
)

export const generateWorkerId = Effect.fn("Relay.generateWorkerId")(
  function* () {
    const crypto = yield* Crypto.Crypto
    const uuid = yield* crypto.randomUUIDv4
    return WorkerId.make(`wrk-${uuid}`)
  },
)

import { Schema } from "effect"

export const EventId = Schema.String.check(
  Schema.isPattern(/^evt-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("EventId"))
export type EventId = Schema.Schema.Type<typeof EventId>

export const DeliveryId = Schema.String.check(
  Schema.isPattern(/^dlv-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("DeliveryId"))
export type DeliveryId = Schema.Schema.Type<typeof DeliveryId>

export const DestinationId = Schema.String.check(
  Schema.isPattern(/^dst-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("DestinationId"))
export type DestinationId = Schema.Schema.Type<typeof DestinationId>

export const InvoiceId = Schema.String.check(
  Schema.isPattern(/^inv-[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("InvoiceId"))
export type InvoiceId = Schema.Schema.Type<typeof InvoiceId>

export const AmountCents = Schema.Int.check(
  Schema.isGreaterThan(0),
).pipe(Schema.brand("AmountCents"))
export type AmountCents = Schema.Schema.Type<typeof AmountCents>

export interface RelayEvent extends Schema.Schema.Type<typeof RelayEvent> {}

export const RelayEvent = Schema.Struct({
  id: EventId,
  type: Schema.Literal("invoice.created"),
  invoiceId: InvoiceId,
  amountCents: AmountCents,
})

export interface Delivery extends Schema.Schema.Type<typeof Delivery> {}

export const Delivery = Schema.Struct({
  id: DeliveryId,
  eventId: EventId,
  destinationId: DestinationId,
})

export const decodeRelayEvent = Schema.decodeUnknownEffect(RelayEvent)

export interface Destination {
  readonly id: DestinationId
  readonly endpoint: URL
  readonly authorization: string
}

export type DeliveryOutcome =
  | {
      readonly _tag: "Delivered"
      readonly destinationId: DestinationId
      readonly status: number
    }
  | {
      readonly _tag: "Rejected"
      readonly destinationId: DestinationId
      readonly status: number
    }

export const classifyDeliveryStatus = (
  destinationId: DestinationId,
  status: number,
): DeliveryOutcome =>
  status >= 200 && status < 300
    ? {
        _tag: "Delivered",
        destinationId,
        status,
      }
    : {
        _tag: "Rejected",
        destinationId,
        status,
      }

export interface DeliveryRequest {
  readonly endpoint: URL
  readonly authorization: string
  readonly body: string
}

export const makeDeliveryRequest = (
  event: RelayEvent,
  destination: Destination,
): DeliveryRequest => ({
  endpoint: destination.endpoint,
  authorization: destination.authorization,
  body: JSON.stringify(event),
})

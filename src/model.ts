import { Data, Effect, Schema } from "effect"

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
  type: Schema.Literal("invoice.created").pipe(
    Schema.withConstructorDefault(
      Effect.succeed("invoice.created"),
    ),
  ),
  invoiceId: InvoiceId,
  amountCents: AmountCents,
})

export const DeliveryState = Schema.TaggedUnion({
  Pending: {},
  Delivered: {
    status: Schema.Int,
  },
  Rejected: {
    status: Schema.Int,
  },
})
export type DeliveryState = Schema.Schema.Type<typeof DeliveryState>

export interface Delivery extends Schema.Schema.Type<typeof Delivery> {}

export const Delivery = Schema.Struct({
  id: DeliveryId,
  eventId: EventId,
  destinationId: DestinationId,
  state: DeliveryState,
})

export const decodeRelayEvent = Schema.decodeUnknownEffect(RelayEvent)
export const RelayEventFromJson = Schema.fromJsonString(RelayEvent)
export const decodeRelayEventJson = Schema.decodeUnknownEffect(
  RelayEventFromJson,
)
export const encodeRelayEventJson = Schema.encodeEffect(
  RelayEventFromJson,
)

export interface Destination extends Schema.Schema.Type<typeof Destination> {}

export const Destination = Schema.Struct({
  id: DestinationId,
  endpoint: Schema.URL,
  authorization: Schema.Redacted(Schema.String),
})

export type DeliveryOutcome = Data.TaggedEnum<{
  Delivered: {
    readonly destinationId: DestinationId
    readonly status: number
  }
  Rejected: {
    readonly destinationId: DestinationId
    readonly status: number
  }
}>

export const DeliveryOutcome = Data.taggedEnum<DeliveryOutcome>()

export const classifyDeliveryStatus = (
  destinationId: DestinationId,
  status: number,
): DeliveryOutcome =>
  status >= 200 && status < 300
    ? DeliveryOutcome.Delivered({
        destinationId,
        status,
      })
    : DeliveryOutcome.Rejected({
        destinationId,
        status,
      })

export const transitionDeliveryState = (
  current: DeliveryState,
  outcome: DeliveryOutcome,
): DeliveryState =>
  DeliveryState.match<DeliveryState>(current, {
    Pending: () =>
      DeliveryOutcome.$match(outcome, {
        Delivered: ({ status }) =>
          DeliveryState.cases.Delivered.make({ status }),
        Rejected: ({ status }) =>
          DeliveryState.cases.Rejected.make({ status }),
      }),
    Delivered: () => current,
    Rejected: () => current,
  })

export interface DeliveryRequest {
  readonly endpoint: URL
  readonly authorization: Destination["authorization"]
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

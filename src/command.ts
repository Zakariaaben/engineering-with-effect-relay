import { Effect, Schema } from "effect"
import { InvalidEventError } from "./errors.ts"
import {
  AmountCents,
  DeliveryId,
  EventId,
  InvoiceId,
} from "./identifiers.ts"

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

export interface EventSubmission extends
  Schema.Schema.Type<typeof EventSubmission> {}

export const EventSubmission = Schema.Struct({
  topic: Schema.Literal("invoice.created"),
  payload: Schema.Struct({
    invoiceId: InvoiceId,
    amountCents: AmountCents,
  }),
})

export interface EventAcceptance extends
  Schema.Schema.Type<typeof EventAcceptance> {}

export const EventAcceptance = Schema.Struct({
  eventId: EventId,
  deliveryId: DeliveryId,
  acceptedAtMillis: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  replayed: Schema.Boolean,
})

export const decodeRelayEvent = Schema.decodeUnknownEffect(RelayEvent)
export const RelayEventFromJson = Schema.fromJsonString(RelayEvent)
export const decodeRelayEventJson = Schema.decodeUnknownEffect(
  RelayEventFromJson,
)
export const encodeRelayEventJson = Schema.encodeEffect(
  RelayEventFromJson,
)

export const decodeIncomingEvent = Effect.fn("Relay.decodeIncomingEvent")(
  function* (candidate: unknown) {
    return yield* decodeRelayEvent(candidate).pipe(
      Effect.mapError((error) =>
        new InvalidEventError({ summary: error.message })
      ),
    )
  },
)

import { describe, expect, it } from "bun:test"
import { Effect, Schema } from "effect"
import {
  AmountCents,
  Delivery,
  DeliveryId,
  DeliveryState,
  DestinationId,
  EventId,
  InvoiceId,
  RelayEvent,
  decodeRelayEvent,
} from "../src/model.ts"

describe("C02-02 domain schemas", () => {
  it("constructs trusted records from checked scalar values", () => {
    const event = RelayEvent.make({
      id: EventId.make("evt-42"),
      invoiceId: InvoiceId.make("inv-7"),
      amountCents: AmountCents.make(2_500),
    })
    const delivery = Delivery.make({
      id: DeliveryId.make("dlv-9"),
      eventId: event.id,
      destinationId: DestinationId.make("dst-3"),
      state: DeliveryState.cases.Pending.make({}),
    })

    expect(Number(event.amountCents)).toBe(2_500)
    expect(event.type).toBe("invoice.created")
    expect(delivery.eventId).toBe(event.id)
  })

  it("decodes unknown event and delivery records", async () => {
    const event = await Effect.runPromise(
      decodeRelayEvent({
        id: "evt-42",
        type: "invoice.created",
        invoiceId: "inv-7",
        amountCents: 2_500,
      }),
    )
    const delivery = await Effect.runPromise(
      Schema.decodeUnknownEffect(Delivery)({
        id: "dlv-9",
        eventId: event.id,
        destinationId: "dst-3",
        state: { _tag: "Pending" },
      }),
    )

    expect(String(event.id)).toBe("evt-42")
    expect(String(delivery.destinationId)).toBe("dst-3")
  })

  it("rejects values that violate scalar constraints", async () => {
    const error = await Effect.runPromise(
      decodeRelayEvent(
        {
          id: "dlv-42",
          type: "invoice.created",
          invoiceId: "inv-7",
          amountCents: 0,
        },
        { errors: "all" },
      ).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Schema.SchemaError)
    expect(error.message).toContain("id")
    expect(error.message).toContain("amountCents")
  })
})

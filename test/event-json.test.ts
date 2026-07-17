import { describe, expect, it } from "bun:test"
import { Effect, Schema } from "effect"
import {
  decodeRelayEvent,
  decodeRelayEventJson,
  encodeRelayEventJson,
} from "../src/model.ts"
import { event } from "./fixtures.ts"

describe("C02-03 event JSON boundary", () => {
  it("round-trips a normalized event through its encoded JSON string", async () => {
    const encoded = await Effect.runPromise(
      encodeRelayEventJson(event),
    )
    const decoded = await Effect.runPromise(
      decodeRelayEventJson(encoded),
    )

    expect(decoded).toEqual(event)
  })

  it("keeps the constructor default out of unknown-input decoding", async () => {
    const error = await Effect.runPromise(
      decodeRelayEvent({
        id: "evt-42",
        invoiceId: "inv-7",
        amountCents: 2_500,
      }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Schema.SchemaError)
    expect(error.message).toContain("type")
  })

  it("distinguishes malformed JSON syntax from invalid event data", async () => {
    const syntaxError = await Effect.runPromise(
      decodeRelayEventJson("{").pipe(Effect.flip),
    )
    const dataError = await Effect.runPromise(
      decodeRelayEventJson(JSON.stringify({
        id: "evt-42",
        type: "invoice.created",
        invoiceId: "inv-7",
        amountCents: 0,
      })).pipe(Effect.flip),
    )

    expect(syntaxError).toBeInstanceOf(Schema.SchemaError)
    expect(dataError).toBeInstanceOf(Schema.SchemaError)
    expect(dataError.message).toContain("amountCents")
  })
})

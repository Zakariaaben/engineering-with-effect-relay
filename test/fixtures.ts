import {
  AmountCents,
  DestinationId,
  EventId,
  InvoiceId,
  RelayEvent,
  type Destination,
} from "../src/model.ts"

export const event: RelayEvent = RelayEvent.make({
  id: EventId.make("evt-1"),
  invoiceId: InvoiceId.make("inv-1"),
  amountCents: AmountCents.make(12_500),
})

export const destination: Destination = {
  id: DestinationId.make("dst-1"),
  endpoint: new URL("https://hooks.example.test/invoices"),
  authorization: "test-secret",
}

export const makeGate = <A>() => {
  let settle: ((value: A) => void) | undefined
  const promise = new Promise<A>((resolve) => {
    // The Promise constructor calls its executor synchronously.
    // Keep the defensive branch so the helper needs no unchecked assertion.
    settle = resolve
  })
  return {
    promise,
    resolve: (value: A) => {
      if (settle === undefined) {
        throw new Error("gate was not initialized")
      }
      settle(value)
    },
  }
}

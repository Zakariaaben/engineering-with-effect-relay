import type {
  Destination,
  RelayEvent,
} from "../src/model.ts"

export const event: RelayEvent = {
  id: "evt-1",
  type: "invoice.created",
  invoiceId: "inv-1",
  amountCents: 12_500,
}

export const destination: Destination = {
  id: "dst-1",
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

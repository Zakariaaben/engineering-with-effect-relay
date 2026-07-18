import { Effect, Layer, Redacted } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import type * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import {
  DestinationClient,
  type DestinationClientService,
} from "../src/destinationClient.ts"
import {
  AmountCents,
  Delivery,
  DeliveryId,
  DeliveryState,
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
  authorization: Redacted.make("test-secret"),
}

export const delivery: Delivery = Delivery.make({
  id: DeliveryId.make("dlv-1"),
  eventId: event.id,
  destinationId: destination.id,
  state: DeliveryState.cases.Pending.make({}),
})

export const provideDestinationClient = (
  client: DestinationClientService,
) => Effect.provideService(DestinationClient, client)

export const makeHttpClientLayer = (
  run: Parameters<typeof HttpClient.make>[0],
) => Layer.succeed(HttpClient.HttpClient, HttpClient.make(run))

export const makeHttpResponse = (
  request: HttpClientRequest.HttpClientRequest,
  status = 202,
  headers?: HeadersInit,
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(
      null,
      headers === undefined ? { status } : { status, headers },
    ),
  )

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

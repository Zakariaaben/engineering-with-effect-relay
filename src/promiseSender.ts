import {
  Redacted,
} from "effect"
import {
  classifyDeliveryResponse,
  makeDeliveryRequest,
  type DeliveryId,
  type DeliveryOutcome,
  type DeliveryRequest,
  type DeliveryResponseEvidence,
  type Destination,
  type RelayEvent,
} from "./model.ts"

export interface PromiseDestinationClient {
  readonly post: (
    request: DeliveryRequest & { readonly signal: AbortSignal },
  ) => Promise<number | DeliveryResponseEvidence>
}

interface PromiseHttpResponse {
  readonly status: number
  readonly headers?: {
    readonly get: (name: string) => string | null
  }
  readonly body: {
    readonly cancel: () => Promise<void>
  } | null
}

export type PromiseFetch = (
  input: URL,
  init: RequestInit,
) => Promise<PromiseHttpResponse>

export const makePromiseFetchDestinationClient = (
  fetch: PromiseFetch,
): PromiseDestinationClient => ({
  post: async ({
    deliveryId,
    endpoint,
    authorization,
    body,
    signal,
  }) => {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${Redacted.value(authorization)}`,
        "content-type": "application/json",
        "idempotency-key": `"${deliveryId}"`,
      },
      body,
      redirect: "manual",
      signal,
    })

    try {
      const retryAfter = response.headers?.get("retry-after")
      return retryAfter === null || retryAfter === undefined
        ? { status: response.status }
        : { status: response.status, retryAfter }
    } finally {
      await response.body?.cancel()
    }
  },
})

export const sendDeliveryWithPromise = async (
  deliveryId: DeliveryId,
  event: RelayEvent,
  destination: Destination,
  client: PromiseDestinationClient,
  signal: AbortSignal,
): Promise<DeliveryOutcome> => {
  const request = makeDeliveryRequest(
    deliveryId,
    event,
    destination,
  )
  const evidence = await client.post({ ...request, signal })
  const response = typeof evidence === "number"
    ? { status: evidence }
    : evidence

  return classifyDeliveryResponse(
    destination.id,
    response,
    Date.now(),
  )
}

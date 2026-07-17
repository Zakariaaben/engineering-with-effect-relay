import { Context, Redacted } from "effect"
import type {
  DeliveryRequest,
  DeliveryResponseEvidence,
} from "./model.ts"

export class DestinationClient extends Context.Service<DestinationClient, {
  readonly post: (
    request: DeliveryRequest & {
      readonly signal: AbortSignal
    },
  ) => Promise<number | DeliveryResponseEvidence>
}>()("Relay/DestinationClient") {}

export type DestinationClientService =
  Context.Service.Shape<typeof DestinationClient>

interface HttpResponse {
  readonly status: number
  readonly headers?: {
    readonly get: (name: string) => string | null
  }
  readonly body: {
    readonly cancel: () => Promise<void>
  } | null
}

export type Fetch = (
  input: URL,
  init: RequestInit,
) => Promise<HttpResponse>

export const makeFetchDestinationClient = (
  fetch: Fetch,
): DestinationClientService => DestinationClient.of({
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

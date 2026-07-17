import { Context } from "effect"
import type { DeliveryRequest } from "./model.ts"

export class DestinationClient extends Context.Service<DestinationClient, {
  readonly post: (
    request: DeliveryRequest & {
      readonly signal: AbortSignal
    },
  ) => Promise<number>
}>()("Relay/DestinationClient") {}

export type DestinationClientService =
  Context.Service.Shape<typeof DestinationClient>

interface HttpResponse {
  readonly status: number
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
  post: async ({ endpoint, authorization, body, signal }) => {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${authorization}`,
        "content-type": "application/json",
      },
      body,
      signal,
    })

    try {
      return response.status
    } finally {
      await response.body?.cancel()
    }
  },
})

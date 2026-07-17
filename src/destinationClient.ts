import type { DeliveryRequest } from "./model.ts"

export interface DestinationClient {
  readonly post: (
    request: DeliveryRequest & {
      readonly signal: AbortSignal
    },
  ) => Promise<number>
}

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
): DestinationClient => ({
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

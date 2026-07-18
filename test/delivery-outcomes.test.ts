import { describe, expect, it } from "bun:test"
import { Effect, Layer } from "effect"
import { DestinationClientLive } from "../src/destinationClient.ts"
import { sendDelivery } from "../src/effectSender.ts"
import {
  classifyDeliveryResponse,
  classifyDeliveryStatus,
  DeliveryId,
  parseRetryAfterMillis,
} from "../src/model.ts"
import {
  delivery,
  destination,
  event,
  makeHttpClientLayer,
  makeHttpResponse,
} from "./fixtures.ts"

describe("C05-06 delivery outcomes", () => {
  it.each([
    [200, "Delivered", undefined],
    [299, "Delivered", undefined],
    [300, "Rejected", undefined],
    [307, "Rejected", undefined],
    [400, "Rejected", undefined],
    [408, "Retryable", "AmbiguousResponse"],
    [425, "Retryable", "AmbiguousResponse"],
    [429, "Retryable", "RateLimited"],
    [499, "Rejected", undefined],
    [500, "Retryable", "ProviderFailure"],
    [599, "Retryable", "ProviderFailure"],
    [199, "ProtocolFailure", undefined],
    [600, "ProtocolFailure", undefined],
  ] as const)(
    "classifies status %i as %s",
    (status, tag, reason) => {
      const outcome = classifyDeliveryStatus(
        destination.id,
        status,
      )

      expect(outcome._tag).toBe(tag)
      expect("reason" in outcome ? outcome.reason : undefined)
        .toBe(reason)
    },
  )

  it("reuses one delivery key and lets a cooperating receiver deduplicate", async () => {
    const observed: Array<{
      readonly key: string | null
      readonly method: string
    }> = []
    const committed = new Set<string>()
    let remoteEffects = 0
    const clientLayer = DestinationClientLive.pipe(
      Layer.provide(
        makeHttpClientLayer((request) =>
          Effect.sync(() => {
            const key = request.headers["idempotency-key"] ?? null
            observed.push({
              key,
              method: request.method,
            })
            if (key !== null && !committed.has(key)) {
              committed.add(key)
              remoteEffects += 1
            }
            return makeHttpResponse(request)
          })
        ),
      ),
    )
    const nextDeliveryId = DeliveryId.make("dlv-2")

    const outcomes = await Effect.runPromise(
      Effect.all([
        sendDelivery(delivery.id, event, destination),
        sendDelivery(delivery.id, event, destination),
        sendDelivery(nextDeliveryId, event, destination),
      ]).pipe(Effect.provide(clientLayer)),
    )

    expect(outcomes.map(({ _tag }) => _tag)).toEqual([
      "Delivered",
      "Delivered",
      "Delivered",
    ])
    expect(observed).toEqual([
      { key: '"dlv-1"', method: "POST" },
      { key: '"dlv-1"', method: "POST" },
      { key: '"dlv-2"', method: "POST" },
    ])
    expect(remoteEffects).toBe(2)
  })

  it("turns Retry-After evidence into a provider delay", () => {
    const now = Date.parse("2026-01-01T00:00:00Z")

    expect(parseRetryAfterMillis("3", now)).toBe(3_000)
    expect(
      parseRetryAfterMillis("Thu, 01 Jan 2026 00:00:05 GMT", now),
    ).toBe(5_000)
    expect(parseRetryAfterMillis("not-a-delay", now)).toBeUndefined()
    expect(
      classifyDeliveryResponse(
        destination.id,
        { status: 429, retryAfter: "3" },
        now,
      ),
    ).toEqual({
      _tag: "Retryable",
      destinationId: destination.id,
      status: 429,
      reason: "RateLimited",
      retryAfterMillis: 3_000,
    })
  })
})

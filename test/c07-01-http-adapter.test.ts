import { describe, expect, it } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import * as HttpClientError from "effect/unstable/http/HttpClientError"
import type * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import { DestinationClientLive } from "../src/http/destination-live.ts"
import { runDelivery } from "../src/delivery.ts"
import { DeliveryTransportError } from "../src/errors.ts"
import { makeDeliveryRequest } from "../src/model.ts"
import { DestinationClient } from "../src/destination.ts"
import {
  delivery,
  destination,
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
} from "./fixtures.ts"

const bodyText = (
  request: HttpClientRequest.HttpClientRequest,
): string => {
  if (request.body._tag !== "Uint8Array") {
    throw new Error(`expected Uint8Array body, got ${request.body._tag}`)
  }
  return new TextDecoder().decode(request.body.body)
}

const destinationClientLayer = (
  run: Parameters<typeof makeHttpClientLayer>[0],
) => DestinationClientLive.pipe(
  Layer.provide(makeHttpClientLayer(run)),
)

describe("C07-01 trustworthy outgoing HTTP adapter", () => {
  it("constructs one authenticated request and extracts only declared response evidence", async () => {
    let calls = 0
    let observed: {
      readonly authorization: string | undefined
      readonly body: string
      readonly contentType: string | undefined
      readonly diagnostic: string
      readonly idempotencyKey: string | undefined
      readonly method: string
      readonly url: string
    } | undefined
    let transportSignal: AbortSignal | undefined

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DestinationClient
        return yield* client.post(
          makeDeliveryRequest(delivery.id, event, destination),
        )
      }).pipe(
        Effect.provide(
          destinationClientLayer((request, url, signal) =>
            Effect.sync(() => {
              calls += 1
              transportSignal = signal
              observed = {
                authorization: request.headers.authorization,
                body: bodyText(request),
                contentType: request.headers["content-type"],
                diagnostic: JSON.stringify(request),
                idempotencyKey: request.headers["idempotency-key"],
                method: request.method,
                url: url.href,
              }
              return HttpClientResponse.fromWeb(
                request,
                new Response("{not valid JSON", {
                  status: 429,
                  headers: {
                    "content-type": "application/json",
                    "retry-after": "3",
                  },
                }),
              )
            })
          ),
        ),
      ),
    )

    expect(result).toEqual({ status: 429, retryAfter: "3" })
    expect(calls).toBe(1)
    expect(observed).toEqual({
      authorization: "Bearer test-secret",
      body: JSON.stringify(event),
      contentType: "application/json",
      diagnostic: expect.stringContaining("<redacted>"),
      idempotencyKey: '"dlv-1"',
      method: "POST",
      url: destination.endpoint.href,
    })
    expect(observed?.diagnostic).not.toContain("test-secret")
    expect(transportSignal?.aborted).toBe(true)
  })

  it("translates a transport error without exposing the HTTP client type", async () => {
    const transportCause = new Error("connection reset")
    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* DestinationClient
        return yield* client.post(
          makeDeliveryRequest(delivery.id, event, destination),
        ).pipe(Effect.flip)
      }).pipe(
        Effect.provide(
          destinationClientLayer((request) =>
            Effect.fail(
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({
                  request,
                  cause: transportCause,
                }),
              }),
            )
          ),
        ),
      ),
    )

    expect(failure).toBeInstanceOf(DeliveryTransportError)
    expect(failure.deliveryId).toBe(delivery.id)
    expect(failure.destinationId).toBe(destination.id)
    expect(failure.cause).toBeInstanceOf(
      HttpClientError.HttpClientError,
    )
    if (!(failure.cause instanceof HttpClientError.HttpClientError)) {
      throw new Error("expected the original HTTP client error as cause")
    }
    expect(failure.cause.reason._tag).toBe("TransportError")
    expect(JSON.stringify(failure.cause)).not.toContain("test-secret")
  })

  it("keeps status classification and retry outside the adapter", async () => {
    let calls = 0
    const outcome = await Effect.runPromise(
      runDelivery(delivery.id, event, destination).pipe(
        Effect.provide(
          destinationClientLayer((request) =>
            Effect.sync(() => {
              calls += 1
              return makeHttpResponse(request, 503)
            })
          ),
        ),
      ),
    )

    expect(outcome).toEqual({
      _tag: "Retryable",
      destinationId: destination.id,
      status: 503,
      reason: "ProviderFailure",
    })
    expect(calls).toBe(1)
  })

  it("forwards interruption to the HTTP transport signal", async () => {
    const started = makeGate<AbortSignal>()
    const controller = new AbortController()
    const running = Effect.runPromiseExit(
      Effect.gen(function* () {
        const client = yield* DestinationClient
        return yield* client.post(
          makeDeliveryRequest(delivery.id, event, destination),
        )
      }).pipe(
        Effect.provide(
          destinationClientLayer(
            (_request, _url, signal) =>
              Effect.sync(() => started.resolve(signal)).pipe(
                Effect.andThen(Effect.never),
              ),
          ),
        ),
      ),
      { signal: controller.signal },
    )

    const transportSignal = await started.promise
    controller.abort("stop adapter test")
    const exit = await running

    expect(transportSignal.aborted).toBe(true)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isSuccess(exit)) {
      throw new Error("expected interruption")
    }
    expect(Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(Cause.hasFails(exit.cause)).toBe(false)
  })
})

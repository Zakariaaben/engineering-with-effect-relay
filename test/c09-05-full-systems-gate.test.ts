import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Effect,
  Option,
  Schema,
  Stream,
} from "effect"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  DeliveryStatus,
  EventAcceptance,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
  submission,
} from "./fixtures.ts"

const intakeToken = "full-systems-intake"
const operationsToken = "full-systems-operations"

const decodeAcceptance = async (response: Response) =>
  Effect.runPromise(
    Schema.decodeUnknownEffect(EventAcceptance)(await response.json()),
  )

const decodeStatus = async (response: Response) =>
  Effect.runPromise(
    Schema.decodeUnknownEffect(DeliveryStatus)(await response.json()),
  )

describe("C09-05 Full Systems gate", () => {
  it("composes intake, delivery, recovery, evidence, and shutdown boundaries", async () => {
    const responseStatuses = [202, 503, 202]
    const outboundIdempotencyKeys: Array<string | undefined> = []
    const application = await startRelayApplication({
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
        RELAY_DESTINATION_ID: "dst-full-systems",
        RELAY_DESTINATION_URL:
          "https://hooks.example.test/full-systems",
        RELAY_INTAKE_AUTHORIZATION: intakeToken,
        RELAY_OPERATIONS_AUTHORIZATION: operationsToken,
        RELAY_RECOVERY_POLL_INTERVAL: "1 hour",
        RELAY_RETRY_MAX_ATTEMPTS: 1,
      }),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          const status = responseStatuses[
            outboundIdempotencyKeys.length
          ] ?? 500
          outboundIdempotencyKeys.push(
            request.headers["idempotency-key"],
          )
          return makeHttpResponse(request, status)
        })
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })

    const postEvent = () =>
      fetch(`${application.httpAddress}/events`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${intakeToken}`,
          "content-type": "application/json",
          "idempotency-key": "full-systems:accepted-event",
        },
        body: JSON.stringify(submission),
      })
    const callOperations = (
      path: string,
      token: string,
      method: "GET" | "POST" = "GET",
    ) =>
      fetch(`${application.httpAddress}${path}`, {
        method,
        headers: { authorization: `Bearer ${token}` },
      })

    try {
      expect(await application.isReady()).toBe(true)

      const acceptedResult = Effect.runPromise(
        Stream.runHead(application.deliveryResults),
      )
      const firstResponse = await postEvent()
      const first = await decodeAcceptance(firstResponse)
      const delivered = Option.getOrThrow(await acceptedResult)

      expect(firstResponse.status).toBe(202)
      expect(first.replayed).toBe(false)
      expect(delivered._tag).toBe("Delivered")
      expect(delivered.deliveryId).toBe(first.deliveryId)

      const replayResponse = await postEvent()
      const replay = await decodeAcceptance(replayResponse)

      expect(replayResponse.status).toBe(202)
      expect(replay).toEqual({ ...first, replayed: true })
      expect(outboundIdempotencyKeys).toHaveLength(1)

      const exhausted = await application.deliver(event)
      expect(exhausted._tag).toBe("Exhausted")

      const deliveryPath =
        `/operations/deliveries/${exhausted.deliveryId}`
      const unauthorized = await callOperations(
        deliveryPath,
        intakeToken,
      )
      expect(unauthorized.status).toBe(401)

      const stoppedResponse = await callOperations(
        deliveryPath,
        operationsToken,
      )
      const stopped = await decodeStatus(stoppedResponse)
      expect(stopped.delivery.state._tag).toBe("DeadLettered")
      expect(stopped.attempts).toHaveLength(1)

      const retryResponse = await callOperations(
        `/operations/dead-letters/${exhausted.deliveryId}/retry`,
        operationsToken,
        "POST",
      )
      const pending = await decodeStatus(retryResponse)
      expect(pending.delivery.state._tag).toBe("Pending")
      expect(pending.attempts).toHaveLength(1)

      const reconciliationResponse = await callOperations(
        "/operations/reconcile",
        operationsToken,
        "POST",
      )
      expect(await reconciliationResponse.json()).toEqual({ claimed: 1 })

      const recoveredResponse = await callOperations(
        deliveryPath,
        operationsToken,
      )
      const recovered = await decodeStatus(recoveredResponse)

      expect(recovered.delivery.state).toEqual({
        _tag: "Delivered",
        status: 202,
      })
      expect(recovered.attempts.map((attempt) => attempt.ordinal)).toEqual(
        [1, 2],
      )
      expect(
        recovered.attempts.map((attempt) => attempt.deliveryId),
      ).toEqual([exhausted.deliveryId, exhausted.deliveryId])
      expect(outboundIdempotencyKeys.slice(1)).toEqual([
        `"${exhausted.deliveryId}"`,
        `"${exhausted.deliveryId}"`,
      ])
    } finally {
      await application.shutdown()
    }

    expect(await application.isReady()).toBe(false)
  })
})

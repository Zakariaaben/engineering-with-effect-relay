import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect, Layer } from "effect"
import { DeliveryRepositoryMemory } from "../src/layers.ts"
import {
  Delivery,
  type DeliveryId,
  DeliveryState,
} from "../src/model.ts"
import { RelayReadiness } from "../src/readiness.ts"
import { startRelayApplication } from "../src/runtime.ts"
import { RelayIntakeStore } from "../src/services.ts"
import {
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const configuration = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "shutdown-secret",
  RELAY_DESTINATION_CONCURRENCY: 1,
  RELAY_DESTINATION_ID: "dst-shutdown",
  RELAY_DESTINATION_URL: "https://hooks.example.test/shutdown",
  RELAY_GLOBAL_CONCURRENCY: 1,
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
})

const makeIntakeLayer = (
  deliveries: Map<DeliveryId, Delivery>,
) =>
  Layer.succeed(
    RelayIntakeStore,
    RelayIntakeStore.of({
      savePending: (acceptedEvent, deliveryId, destinationId) =>
        Effect.sync(() => {
          const delivery = Delivery.make({
            id: deliveryId,
            eventId: acceptedEvent.id,
            destinationId,
            state: DeliveryState.cases.Pending.make({}),
          })
          deliveries.set(delivery.id, delivery)
          return delivery
        }),
    }),
  )

const postDelivery = (address: string) =>
  fetch(`${address}/deliveries`, {
    method: "POST",
    headers: {
      authorization: "Bearer intake-secret",
      "content-type": "application/json",
    },
    body: JSON.stringify(event),
  })

describe("C07-12 startup readiness and shutdown", () => {
  it("does not become ready until required startup acquisition finishes", async () => {
    const acquisitionStarted = makeGate<void>()
    const releaseAcquisition = makeGate<void>()
    const deliveries = new Map<DeliveryId, Delivery>()
    let ready = false
    let hookRegistrations = 0

    const delayedIntakeLayer = Layer.effect(
      RelayIntakeStore,
      Effect.sync(() => acquisitionStarted.resolve(undefined)).pipe(
        Effect.andThen(
          Effect.promise(() => releaseAcquisition.promise),
        ),
        Effect.as(
          RelayIntakeStore.of({
            savePending: (acceptedEvent, deliveryId, destinationId) =>
              Effect.sync(() => {
                const delivery = Delivery.make({
                  id: deliveryId,
                  eventId: acceptedEvent.id,
                  destinationId,
                  state: DeliveryState.cases.Pending.make({}),
                })
                deliveries.set(delivery.id, delivery)
                return delivery
              }),
          }),
        ),
      ),
    )
    const readinessLayer = Layer.succeed(
      RelayReadiness,
      RelayReadiness.of({
        current: Effect.sync(() => ready),
        markReady: Effect.sync(() => {
          ready = true
        }),
        markNotReady: Effect.sync(() => {
          ready = false
        }),
      }),
    )

    const starting = startRelayApplication({
      configProvider: configuration(),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.succeed(makeHttpResponse(request))
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: Layer.merge(
        DeliveryRepositoryMemory,
        delayedIntakeLayer,
      ),
      readinessLayer,
      registerShutdownHook: () => {
        hookRegistrations += 1
        return () => {}
      },
    })

    await acquisitionStarted.promise
    expect(ready).toBe(false)
    expect(hookRegistrations).toBe(0)

    releaseAcquisition.resolve(undefined)
    const application = await starting
    try {
      expect(await application.isReady()).toBe(true)
      expect(hookRegistrations).toBe(1)
      const readinessResponse = await fetch(
        `${application.httpAddress}/health/ready`,
      )
      expect(readinessResponse.status).toBe(200)
      expect(await readinessResponse.json()).toEqual({
        status: "ready",
      })
    } finally {
      await application.shutdown()
    }
  })

  it("stops intake before interrupting active work and preserves committed intent", async () => {
    const markedNotReady = makeGate<void>()
    const releaseShutdown = makeGate<void>()
    const outboundStarted = makeGate<AbortSignal>()
    const deliveries = new Map<DeliveryId, Delivery>()
    let ready = false
    let outboundCalls = 0

    const readinessLayer = Layer.succeed(
      RelayReadiness,
      RelayReadiness.of({
        current: Effect.sync(() => ready),
        markReady: Effect.sync(() => {
          ready = true
        }),
        markNotReady: Effect.sync(() => {
          ready = false
          markedNotReady.resolve(undefined)
        }).pipe(
          Effect.andThen(
            Effect.promise(() => releaseShutdown.promise),
          ),
        ),
      }),
    )
    const application = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer: makeHttpClientLayer(
        (_request, _endpoint, signal) =>
          Effect.sync(() => {
            outboundCalls += 1
            outboundStarted.resolve(signal)
          }).pipe(Effect.andThen(Effect.never)),
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: Layer.merge(
        DeliveryRepositoryMemory,
        makeIntakeLayer(deliveries),
      ),
      readinessLayer,
      registerShutdownHook: () => () => {},
    })

    const activeDelivery = application.deliver(event)
    const clientSignal = await outboundStarted.promise
    expect(await application.isReady()).toBe(true)
    expect(deliveries.size).toBe(1)
    expect(Array.from(deliveries.values())[0]?.state._tag).toBe(
      "Pending",
    )

    const stopped = application.shutdown()
    await markedNotReady.promise

    expect(await application.isReady()).toBe(false)
    const readinessResponse = await fetch(
      `${application.httpAddress}/health/ready`,
    )
    expect(readinessResponse.status).toBe(503)
    expect(await readinessResponse.json()).toEqual({
      status: "not_ready",
    })

    const rejectedIntake = await postDelivery(application.httpAddress)
    expect(rejectedIntake.status).toBe(503)
    expect(await rejectedIntake.json()).toEqual({
      error: "not_ready",
    })
    expect(deliveries.size).toBe(1)
    expect(outboundCalls).toBe(1)

    releaseShutdown.resolve(undefined)
    await expect(activeDelivery).rejects.toBeDefined()
    await stopped

    expect(clientSignal.aborted).toBe(true)
    expect(Array.from(deliveries.values())[0]?.state._tag).toBe(
      "Pending",
    )
  })
})

import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect, Layer, Option } from "effect"
import {
  ClaimGeneration,
  Delivery,
  DeliveryClaim,
  type DeliveryId,
  DeliveryState,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "../src/services.ts"
import {
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const configuration = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "m6-secret",
  RELAY_DESTINATION_ID: "dst-m6",
  RELAY_DESTINATION_URL: "https://hooks.example.test/m6",
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
})

const makePersistenceLayer = (
  records: Map<DeliveryId, Delivery>,
  afterCommit: (delivery: Delivery) => Effect.Effect<void> = () =>
    Effect.void,
) => {
  const repository = Layer.succeed(
    DeliveryRepository,
    DeliveryRepository.of({
      save: (delivery) =>
        Effect.sync(() => {
          records.set(delivery.id, delivery)
        }),
      findById: (id) =>
        Effect.sync(() => Option.fromNullishOr(records.get(id))),
      claimPending: () => Effect.succeed([]),
      renewClaim: (_deliveryId, claim) => Effect.succeed(claim),
      completeClaim: () => Effect.void,
      releaseClaim: () => Effect.void,
    }),
  )
  const intake = Layer.succeed(
    RelayIntakeStore,
    RelayIntakeStore.of({
      accept: () => Effect.die(new Error("not used by this gate")),
      savePending: (
        acceptedEvent,
        deliveryId,
        destinationId,
        claimRequest,
      ) =>
        Effect.sync(() => {
          const delivery = Delivery.make({
            id: deliveryId,
            eventId: acceptedEvent.id,
            destinationId,
            state: DeliveryState.cases.Pending.make({}),
          })
          records.set(delivery.id, delivery)
          return {
            claim: DeliveryClaim.make({
              ownerId: claimRequest.ownerId,
              generation: ClaimGeneration.make(1),
              leaseExpiresAtMillis: Number.MAX_SAFE_INTEGER,
            }),
            delivery,
            event: acceptedEvent,
            route: Option.none(),
          }
        }).pipe(
          Effect.tap((persisted) => afterCommit(persisted.delivery)),
        ),
    }),
  )

  return Layer.merge(repository, intake)
}

describe("C07-13 M6 act gate", () => {
  it("keeps durable intent but loses its process-local execution path", async () => {
    const committed = makeGate<Delivery>()
    const records = new Map<DeliveryId, Delivery>()
    let outboundCalls = 0
    const httpClientLayer = makeHttpClientLayer((request) =>
      Effect.sync(() => {
        outboundCalls += 1
        return makeHttpResponse(request)
      })
    )
    const first = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: makePersistenceLayer(
        records,
        (delivery) =>
          Effect.sync(() => committed.resolve(delivery)).pipe(
            Effect.andThen(Effect.never),
          ),
      ),
      registerShutdownHook: () => () => {},
    })

    const intake = first.deliver(event).then(
      () => "Completed" as const,
      () => "Interrupted" as const,
    )
    const persisted = await committed.promise
    try {
      const beforeShutdown = await first.loadMetrics()

      expect(await first.isReady()).toBe(true)
      expect(records.get(persisted.id)?.state._tag).toBe("Pending")
      expect(beforeShutdown.admittedDeliveries).toBe(1)
      expect(beforeShutdown.activeDeliveries).toBe(0)
      expect(beforeShutdown.requestQueueDepth).toBe(0)
      expect(beforeShutdown.globalActive).toBe(0)
      expect(outboundCalls).toBe(0)
    } finally {
      await first.shutdown()
    }
    expect(await intake).toBe("Interrupted")

    const restarted = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: makePersistenceLayer(records),
      registerShutdownHook: () => () => {},
    })

    try {
      const afterRestart = await restarted.loadMetrics()

      expect(await restarted.isReady()).toBe(true)
      expect(records.get(persisted.id)?.state._tag).toBe("Pending")
      expect(afterRestart.admittedDeliveries).toBe(0)
      expect(afterRestart.activeDeliveries).toBe(0)
      expect(afterRestart.requestQueueDepth).toBe(0)
      expect(afterRestart.globalActive).toBe(0)
      expect(outboundCalls).toBe(0)
    } finally {
      await restarted.shutdown()
    }
  })
})

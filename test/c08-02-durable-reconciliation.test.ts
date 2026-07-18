import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Duration,
  Effect,
  Layer,
  ManagedRuntime,
  Option,
} from "effect"
import { TestClock } from "effect/testing"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDeliveryResilience,
  defaultDestinationConfigurationVersion,
} from "../src/configuration.ts"
import { DeliverySupervisor } from "../src/deliverySupervisor.ts"
import {
  Delivery,
  type DeliveryId,
  type DeliveryResult,
  DeliveryState,
  type EventId,
  type RelayEvent,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import { makeReconcilerLive } from "../src/reconciler.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "../src/services.ts"
import {
  destination,
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

interface DurableState {
  readonly claims: Set<DeliveryId>
  readonly deliveries: Map<DeliveryId, Delivery>
  readonly events: Map<EventId, RelayEvent>
  readonly observedClaimLimits: Array<number>
}

const makeDurableState = (): DurableState => ({
  claims: new Set(),
  deliveries: new Map(),
  events: new Map(),
  observedClaimLimits: [],
})

const stateAfter = (
  result: DeliveryResult,
): Delivery["state"] => {
  switch (result._tag) {
    case "Delivered":
      return DeliveryState.cases.Delivered.make({
        status: result.status,
      })
    case "Rejected":
      return DeliveryState.cases.Rejected.make({
        status: result.status,
      })
    case "ProtocolFailure":
    case "Exhausted":
      return DeliveryState.cases.Pending.make({})
  }
}

const makePersistenceLayer = (
  state: DurableState,
  options: {
    readonly afterCommit?: (delivery: Delivery) => Effect.Effect<void>
    readonly onComplete?: (delivery: Delivery) => Effect.Effect<void>
  } = {},
) => {
  const repository = DeliveryRepository.of({
    save: (delivery) =>
      Effect.sync(() => {
        state.deliveries.set(delivery.id, delivery)
        state.claims.delete(delivery.id)
      }),
    findById: (id) =>
      Effect.sync(() => Option.fromNullishOr(state.deliveries.get(id))),
    resetClaims: () => Effect.sync(() => state.claims.clear()),
    claimPending: (destinationId, limit) =>
      Effect.sync(() => {
        state.observedClaimLimits.push(limit)
        const claimed = []
        const deliveries = Array.from(state.deliveries.values()).sort(
          (left, right) => String(left.id).localeCompare(String(right.id)),
        )
        for (const delivery of deliveries) {
          if (
            claimed.length >= limit ||
            delivery.state._tag !== "Pending" ||
            delivery.destinationId !== destinationId ||
            state.claims.has(delivery.id)
          ) {
            continue
          }
          const acceptedEvent = state.events.get(delivery.eventId)
          if (acceptedEvent === undefined) continue
          state.claims.add(delivery.id)
          claimed.push({
            delivery,
            event: acceptedEvent,
            route: Option.none(),
          })
        }
        return claimed
      }),
    completeClaim: (deliveryId, result) =>
      Effect.gen(function* () {
        const current = state.deliveries.get(deliveryId)
        if (current === undefined || !state.claims.has(deliveryId)) {
          return
        }
        const completed = Delivery.make({
          ...current,
          state: stateAfter(result),
        })
        state.deliveries.set(deliveryId, completed)
        state.claims.delete(deliveryId)
        if (options.onComplete !== undefined) {
          yield* options.onComplete(completed)
        }
      }),
    releaseClaim: (deliveryId) =>
      Effect.sync(() => {
        state.claims.delete(deliveryId)
      }),
  })
  const intake = RelayIntakeStore.of({
    accept: () => Effect.die(new Error("not used by this gate")),
    savePending: (acceptedEvent, deliveryId, destinationId) => {
      const delivery = Delivery.make({
        id: deliveryId,
        eventId: acceptedEvent.id,
        destinationId,
        state: DeliveryState.cases.Pending.make({}),
      })
      return Effect.sync(() => {
        state.events.set(acceptedEvent.id, acceptedEvent)
        state.deliveries.set(delivery.id, delivery)
        state.claims.add(delivery.id)
        return delivery
      }).pipe(
        Effect.tap((persisted) =>
          options.afterCommit?.(persisted) ?? Effect.void
        ),
      )
    },
  })

  return Layer.merge(
    Layer.succeed(DeliveryRepository, repository),
    Layer.succeed(RelayIntakeStore, intake),
  )
}

const configuration = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "recovery-secret",
  RELAY_DESTINATION_ID: "dst-recovery",
  RELAY_DESTINATION_URL: "https://hooks.example.test/recovery",
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
})

describe("C08-02 durable reconciliation", () => {
  it("rediscovers a committed delivery after restart and keeps its identity", async () => {
    const state = makeDurableState()
    const committed = makeGate<Delivery>()
    const completed = makeGate<Delivery>()
    const outboundKeys: Array<string | undefined> = []
    const httpClientLayer = makeHttpClientLayer((request) =>
      Effect.sync(() => {
        outboundKeys.push(request.headers["idempotency-key"])
        return makeHttpResponse(request, 202)
      })
    )
    const first = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: makePersistenceLayer(state, {
        afterCommit: (delivery) =>
          Effect.sync(() => committed.resolve(delivery)).pipe(
            Effect.andThen(Effect.never),
          ),
      }),
      registerShutdownHook: () => () => {},
    })

    const intake = first.deliver(event).then(
      () => "Completed" as const,
      () => "Interrupted" as const,
    )
    const stranded = await committed.promise

    expect(state.deliveries.get(stranded.id)?.state._tag).toBe("Pending")
    expect(state.claims.has(stranded.id)).toBe(true)
    expect(outboundKeys).toEqual([])

    await first.shutdown()
    expect(await intake).toBe("Interrupted")

    const restarted = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: makePersistenceLayer(state, {
        onComplete: (delivery) =>
          Effect.sync(() => completed.resolve(delivery)),
      }),
      registerShutdownHook: () => () => {},
    })

    try {
      const recovered = await completed.promise

      expect(recovered.id).toBe(stranded.id)
      expect(recovered.state._tag).toBe("Delivered")
      expect(state.claims.has(stranded.id)).toBe(false)
      expect(state.observedClaimLimits).toContain(64)
      expect(outboundKeys).toEqual([`"${stranded.id}"`])
      expect(await restarted.isReady()).toBe(true)
    } finally {
      await restarted.shutdown()
    }
  })

  it("revisits the database on the configured schedule", async () => {
    const firstPass = makeGate<void>()
    const secondPass = makeGate<void>()
    let passes = 0
    const repository = DeliveryRepository.of({
      save: () => Effect.void,
      findById: () => Effect.succeed(Option.none()),
      resetClaims: () => Effect.void,
      claimPending: () => Effect.succeed([]),
      completeClaim: () => Effect.void,
      releaseClaim: () => Effect.void,
    })
    const supervisor = DeliverySupervisor.of({
      activeCount: () => Effect.succeed(0),
      concurrencyMetrics: () => Effect.succeed({
        globalActive: 0,
        activeByDestination: new Map(),
      }),
      deliver: () => Effect.die(new Error("not used by this test")),
      deliverTo: () => Effect.die(new Error("not used by this test")),
      enqueueClaimed: () => Effect.void,
      resumeClaimed: () => Effect.die(new Error("no claims expected")),
      loadMetrics: () => Effect.succeed({
        activeDeliveries: 0,
        admittedDeliveries: 0,
        globalActive: 0,
        activeByDestination: new Map(),
        globalConcurrencyLimit: 1,
        perDestinationConcurrencyLimit: 1,
        rejected: 0,
        requestQueueCapacity: 1,
        requestQueueDepth: 0,
      }),
    })
    const dependencies = Layer.mergeAll(
      Layer.succeed(
        AppConfiguration,
        AppConfiguration.of({
          destination,
          destinationConfigurationVersion:
            defaultDestinationConfigurationVersion,
          concurrency: { global: 1, perDestination: 1 },
          flow: defaultDeliveryFlow,
          recovery: {
            ...defaultDeliveryRecovery,
            pollInterval: Duration.seconds(5),
          },
          resilience: defaultDeliveryResilience,
        }),
      ),
      Layer.succeed(DeliveryRepository, repository),
      Layer.succeed(DeliverySupervisor, supervisor),
    )
    const reconciler = makeReconcilerLive({
      afterClaim: () =>
        Effect.sync(() => {
          passes += 1
          if (passes === 1) firstPass.resolve(undefined)
          if (passes === 2) secondPass.resolve(undefined)
        }),
    }).pipe(
      Layer.provide(dependencies),
      Layer.provideMerge(TestClock.layer()),
    )
    const runtime = ManagedRuntime.make(reconciler)

    try {
      await runtime.context()
      await firstPass.promise
      await runtime.runPromise(TestClock.adjust("5 seconds"))
      await secondPass.promise

      expect(passes).toBe(2)
    } finally {
      await runtime.dispose()
    }
  })
})

import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import { Effect, Layer, ManagedRuntime, Option } from "effect"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDeliveryResilience,
  defaultDestinationConfigurationVersion,
} from "../src/configuration.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  type DeliverySupervisorHooks,
  makeDeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { DeliveryRepositoryMemory } from "../src/layers.ts"
import {
  Delivery,
  ClaimGeneration,
  DeliveryClaim,
  type DeliveryId,
  DeliveryState,
  type EventId,
  type RelayEvent,
  WorkerId,
} from "../src/model.ts"
import { RelayIntakeStore } from "../src/services.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import { destination, event, makeGate } from "./fixtures.ts"

const makeDurableIntakeStore = (
  events: Map<EventId, RelayEvent>,
  deliveries: Map<DeliveryId, Delivery>,
) =>
  Layer.succeed(
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
          events.set(acceptedEvent.id, acceptedEvent)
          deliveries.set(delivery.id, delivery)
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
        }),
    }),
  )

const makeRuntime = (
  intakeStore: Layer.Layer<RelayIntakeStore>,
  post: () => Effect.Effect<{ readonly status: number }>,
  hooks: DeliverySupervisorHooks = {},
) =>
  ManagedRuntime.make(
    makeDeliverySupervisorLive(hooks).pipe(
      Layer.provide(DeliveryEventsLive),
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(
            AppConfiguration,
            AppConfiguration.of({
              destination,
              destinationConfigurationVersion:
                defaultDestinationConfigurationVersion,
              concurrency: { global: 2, perDestination: 1 },
              flow: defaultDeliveryFlow,
              recovery: defaultDeliveryRecovery,
              resilience: defaultDeliveryResilience,
            }),
          ),
          Layer.succeed(
            DestinationClient,
            DestinationClient.of({ post }),
          ),
          intakeStore,
          DeliveryRepositoryMemory,
          makeWorkerIdentityLayer(WorkerId.make("wrk-c07-06")),
          NodeCrypto.layer,
        ),
      ),
    ),
  )

describe("C07-06 crash after intake commit", () => {
  it("strands durable intent when the process stops before local enqueue", async () => {
    const events = new Map<EventId, RelayEvent>()
    const deliveries = new Map<DeliveryId, Delivery>()
    const committed = makeGate<Delivery>()
    let outboundCalls = 0
    const intakeStore = makeDurableIntakeStore(events, deliveries)
    const post = () => Effect.sync(() => {
      outboundCalls += 1
      return { status: 202 }
    })
    const firstRuntime = makeRuntime(intakeStore, post, {
      afterIntakeCommit: (delivery) =>
        Effect.sync(() => committed.resolve(delivery)).pipe(
          Effect.andThen(Effect.never),
        ),
    })

    const interrupted = firstRuntime.runPromise(
      Effect.flatMap(DeliverySupervisor, (supervisor) =>
        supervisor.deliver(event)
      ),
    ).then(
      () => false,
      () => true,
    )
    const persisted = await committed.promise

    expect(events.has(event.id)).toBe(true)
    expect(deliveries.get(persisted.id)?.state._tag).toBe("Pending")
    expect(outboundCalls).toBe(0)

    await firstRuntime.dispose()
    expect(await interrupted).toBe(true)

    const restartedRuntime = makeRuntime(intakeStore, post)
    try {
      const load = await restartedRuntime.runPromise(
        Effect.flatMap(DeliverySupervisor, (supervisor) =>
          supervisor.loadMetrics()
        ),
      )

      expect(load.activeDeliveries).toBe(0)
      expect(load.requestQueueDepth).toBe(0)
      expect(deliveries.get(persisted.id)?.state._tag).toBe("Pending")
      expect(outboundCalls).toBe(0)
    } finally {
      await restartedRuntime.dispose()
    }
  })
})

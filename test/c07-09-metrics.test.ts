import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Metric,
  Redacted,
} from "effect"
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
  DeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { RelayIntakeStoreMemory } from "../src/layers.ts"
import {
  Destination,
  DestinationId,
  WorkerId,
  type Destination as DestinationType,
} from "../src/model.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import { event, makeGate } from "./fixtures.ts"

const makeDestination = (id: string): DestinationType =>
  Destination.make({
    id: DestinationId.make(`dst-${id}`),
    endpoint: new URL(`https://${id}.example.test/hook`),
    authorization: Redacted.make(`secret-${id}`),
  })

const destinationA = makeDestination("metrics-a")
const destinationB = makeDestination("metrics-b")
const destinationC = makeDestination("metrics-c")

const stateFor = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Record<string, string> = {},
) => {
  const snapshot = snapshots.find((candidate) =>
    candidate.id === id &&
    Object.entries(attributes).every(
      ([key, value]) => candidate.attributes?.[key] === value,
    )
  )
  if (snapshot === undefined) {
    throw new Error(`Missing metric ${id}`)
  }
  return snapshot.state
}

describe("C07-09 delivery metrics", () => {
  it("distinguishes saturation from a slow or failing destination", async () => {
    const saturated = makeGate<void>()
    const release = makeGate<void>()
    let started = 0

    const runtime = ManagedRuntime.make(
      DeliverySupervisorLive.pipe(
        Layer.provide(DeliveryEventsLive),
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(
              AppConfiguration,
              AppConfiguration.of({
                destinationConfigurationVersion:
                  defaultDestinationConfigurationVersion,
                destination: destinationA,
                concurrency: { global: 2, perDestination: 1 },
                flow: {
                  ...defaultDeliveryFlow,
                  deliveryRequestsCapacity: 4,
                },
                recovery: defaultDeliveryRecovery,
                resilience: defaultDeliveryResilience,
              }),
            ),
            Layer.succeed(
              DestinationClient,
              DestinationClient.of({
                post: () =>
                  Effect.gen(function* () {
                    started += 1
                    if (started === 2) {
                      saturated.resolve(undefined)
                    }
                    yield* Effect.promise(() => release.promise)
                    return { status: 202 }
                  }),
              }),
            ),
            NodeCrypto.layer,
            RelayIntakeStoreMemory,
            makeWorkerIdentityLayer(WorkerId.make("wrk-metrics")),
          ),
        ),
        Layer.provideMerge(
          Layer.succeed(Metric.MetricRegistry, new Map()),
        ),
      ),
    )

    try {
      const observation = await runtime.runPromise(
        Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const first = yield* supervisor.deliverTo(
            event,
            destinationA,
          ).pipe(Effect.forkChild({ startImmediately: true }))
          const second = yield* supervisor.deliverTo(
            event,
            destinationB,
          ).pipe(Effect.forkChild({ startImmediately: true }))

          yield* Effect.promise(() => saturated.promise)

          const third = yield* supervisor.deliverTo(
            event,
            destinationA,
          ).pipe(Effect.forkChild({ startImmediately: true }))
          const fourth = yield* supervisor.deliverTo(
            event,
            destinationB,
          ).pipe(Effect.forkChild({ startImmediately: true }))
          yield* supervisor.deliverTo(event, destinationC).pipe(
            Effect.flip,
          )

          let load = yield* supervisor.loadMetrics()
          while (load.activeDeliveries < 4) {
            yield* Effect.yieldNow
            load = yield* supervisor.loadMetrics()
          }
          const atCapacity = yield* Metric.snapshot

          release.resolve(undefined)
          yield* Effect.forEach(
            [first, second, third, fourth],
            Fiber.join,
          )
          let settled = yield* supervisor.loadMetrics()
          while (settled.activeDeliveries > 0) {
            yield* Effect.yieldNow
            settled = yield* supervisor.loadMetrics()
          }
          const afterRecovery = yield* Metric.snapshot

          return { afterRecovery, atCapacity }
        }),
      )

      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_admission_rejections_total",
        ),
      ).toEqual({ count: 1, incremental: true })
      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_admitted",
        ),
      ).toEqual({ value: 4 })
      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_admission_capacity",
        ),
      ).toEqual({ value: 4 })
      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_active_attempts",
        ),
      ).toEqual({ value: 2 })
      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_active_attempt_limit",
        ),
      ).toEqual({ value: 2 })
      expect(
        stateFor(
          observation.atCapacity,
          "relay_delivery_request_queue_depth",
        ),
      ).toEqual({ value: 0 })

      expect(
        stateFor(
          observation.afterRecovery,
          "relay_delivery_attempts_total",
          { decision: "Terminal", outcome: "Delivered" },
        ),
      ).toEqual({ count: 4, incremental: true })
      expect(
        stateFor(
          observation.afterRecovery,
          "relay_delivery_attempt_duration_seconds",
          { outcome: "Delivered" },
        ),
      ).toMatchObject({ count: 4 })
      expect(
        observation.afterRecovery.every((snapshot) =>
          snapshot.attributes?.delivery_id === undefined &&
          snapshot.attributes?.destination_id === undefined
        ),
      ).toBe(true)
    } finally {
      await runtime.dispose()
    }
  })
})

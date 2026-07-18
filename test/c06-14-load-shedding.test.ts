import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
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
import { DeliveryOverloaded } from "../src/errors.ts"
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

const destinationA = makeDestination("admission-a")
const destinationB = makeDestination("admission-b")
const destinationC = makeDestination("admission-c")

describe("C06-14 overload admission", () => {
  it("bounds admitted work and exposes global and per-destination saturation", async () => {
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
            makeWorkerIdentityLayer(WorkerId.make("wrk-load-shedding")),
          ),
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
          const overload = yield* supervisor.deliverTo(
            event,
            destinationC,
          ).pipe(Effect.flip)
          let atCapacity = yield* supervisor.loadMetrics()
          while (atCapacity.activeDeliveries < 4) {
            yield* Effect.yieldNow
            atCapacity = yield* supervisor.loadMetrics()
          }

          release.resolve(undefined)
          const accepted = yield* Effect.forEach(
            [first, second, third, fourth],
            Fiber.join,
          )
          let settled = yield* supervisor.loadMetrics()
          while (settled.activeDeliveries > 0) {
            yield* Effect.yieldNow
            settled = yield* supervisor.loadMetrics()
          }

          return { accepted, atCapacity, overload, settled }
        }),
      )

      expect(observation.overload).toBeInstanceOf(DeliveryOverloaded)
      expect(observation.overload).toEqual(
        new DeliveryOverloaded({
          admissionCapacity: 4,
          destinationId: destinationC.id,
        }),
      )
      expect(
        observation.accepted.every(
          (result) => result._tag === "Delivered",
        ),
      ).toBe(true)
      expect(observation.atCapacity).toEqual({
        activeByDestination: new Map([
          [destinationA.id, 1],
          [destinationB.id, 1],
        ]),
        activeDeliveries: 4,
        admittedDeliveries: 4,
        globalActive: 2,
        globalConcurrencyLimit: 2,
        perDestinationConcurrencyLimit: 1,
        rejected: 1,
        requestQueueCapacity: 4,
        requestQueueDepth: 0,
      })
      expect(observation.settled).toEqual({
        activeByDestination: new Map(),
        activeDeliveries: 0,
        admittedDeliveries: 0,
        globalActive: 0,
        globalConcurrencyLimit: 2,
        perDestinationConcurrencyLimit: 1,
        rejected: 1,
        requestQueueCapacity: 4,
        requestQueueDepth: 0,
      })
    } finally {
      await runtime.dispose()
    }
  })
})

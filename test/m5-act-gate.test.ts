import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Context,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
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
  type DeliveryLoadMetrics,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { RelayIntakeStoreMemory } from "../src/layers.ts"
import { destination, event, makeGate } from "./fixtures.ts"
import { reproduceUnboundedProducerConsumer } from "./incidents/unboundedProducerConsumer.ts"

const awaitLoad = (
  supervisor: Context.Service.Shape<typeof DeliverySupervisor>,
  predicate: (metrics: DeliveryLoadMetrics) => boolean,
) =>
  Effect.gen(function* () {
    let metrics = yield* supervisor.loadMetrics()
    while (!predicate(metrics)) {
      yield* Effect.yieldNow
      metrics = yield* supervisor.loadMetrics()
    }
    return metrics
  })

describe("C06-15 M5 act gate", () => {
  it("reproduces bounded attempts with an unbounded waiting population", async () => {
    expect(await reproduceUnboundedProducerConsumer(12, 2)).toEqual({
      active: 2,
      maximumActive: 2,
      owned: 12,
      waiting: 10,
    })
  })

  it("keeps admission and active attempts bounded under sustained pressure", async () => {
    const admissionCapacity = 4
    const pressureCycles = 8
    const acceptedCount = admissionCapacity + pressureCycles
    const started = Array.from(
      { length: acceptedCount },
      () => makeGate<void>(),
    )
    const releases = Array.from(
      { length: acceptedCount },
      () => makeGate<void>(),
    )
    let attempt = 0

    const runtime = ManagedRuntime.make(
      DeliverySupervisorLive.pipe(
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
                flow: {
                  ...defaultDeliveryFlow,
                  deliveryRequestsCapacity: admissionCapacity,
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
                    const index = attempt
                    attempt += 1
                    const startedGate = started[index]
                    const releaseGate = releases[index]
                    if (
                      startedGate === undefined ||
                      releaseGate === undefined
                    ) {
                      return yield* Effect.die(
                        new Error("unexpected delivery attempt"),
                      )
                    }
                    startedGate.resolve(undefined)
                    yield* Effect.promise(() => releaseGate.promise)
                    return { status: 202 }
                  }),
              }),
            ),
            NodeCrypto.layer,
            RelayIntakeStoreMemory,
          ),
        ),
      ),
    )

    try {
      const observation = await runtime.runPromise(
        Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const initial = yield* Effect.forEach(
            Array.from({ length: admissionCapacity }),
            () =>
              supervisor.deliver(event).pipe(
                Effect.forkChild({ startImmediately: true }),
              ),
          )
          const replacements: Array<(typeof initial)[number]> = []
          const overloadTags: Array<string> = []
          const saturatedSnapshots: Array<DeliveryLoadMetrics> = []

          yield* Effect.promise(() => started[0]!.promise)
          yield* awaitLoad(
            supervisor,
            (metrics) =>
              metrics.admittedDeliveries === admissionCapacity &&
              metrics.activeDeliveries === admissionCapacity &&
              metrics.globalActive === 1,
          )

          for (let cycle = 0; cycle < pressureCycles; cycle += 1) {
            const overloaded = yield* supervisor.deliver(event).pipe(
              Effect.flip,
            )
            overloadTags.push(overloaded._tag)
            saturatedSnapshots.push(yield* supervisor.loadMetrics())

            releases[cycle]!.resolve(undefined)
            yield* awaitLoad(
              supervisor,
              (metrics) =>
                metrics.admittedDeliveries === admissionCapacity - 1,
            )

            replacements.push(
              yield* supervisor.deliver(event).pipe(
                Effect.forkChild({ startImmediately: true }),
              ),
            )
            yield* Effect.promise(() => started[cycle + 1]!.promise)
            yield* awaitLoad(
              supervisor,
              (metrics) =>
                metrics.admittedDeliveries === admissionCapacity &&
                metrics.activeDeliveries === admissionCapacity &&
                metrics.globalActive === 1,
            )
          }

          for (
            let index = pressureCycles;
            index < acceptedCount;
            index += 1
          ) {
            yield* Effect.promise(() => started[index]!.promise)
            releases[index]!.resolve(undefined)
          }

          const results = yield* Effect.forEach(
            [...initial, ...replacements],
            Fiber.join,
          )
          const settled = yield* awaitLoad(
            supervisor,
            (metrics) =>
              metrics.admittedDeliveries === 0 &&
              metrics.activeDeliveries === 0,
          )

          return {
            overloadTags,
            results,
            saturatedSnapshots,
            settled,
          }
        }),
      )

      expect(observation.overloadTags).toEqual(
        Array.from({ length: pressureCycles }, () => "DeliveryOverloaded"),
      )
      expect(observation.results).toHaveLength(acceptedCount)
      expect(
        observation.results.every(({ _tag }) => _tag === "Delivered"),
      ).toBe(true)
      expect(
        observation.saturatedSnapshots.every(
          (metrics) =>
            metrics.admittedDeliveries === admissionCapacity &&
            metrics.activeDeliveries === admissionCapacity &&
            metrics.globalActive === 1 &&
            metrics.activeByDestination.get(destination.id) === 1 &&
            metrics.requestQueueDepth === 0,
        ),
      ).toBe(true)
      expect(observation.settled).toEqual({
        activeByDestination: new Map(),
        activeDeliveries: 0,
        admittedDeliveries: 0,
        globalActive: 0,
        globalConcurrencyLimit: 2,
        perDestinationConcurrencyLimit: 1,
        rejected: pressureCycles,
        requestQueueCapacity: admissionCapacity,
        requestQueueDepth: 0,
      })
      expect(attempt).toBe(acceptedCount)
    } finally {
      await runtime.dispose()
    }
  })
})

import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Context,
  Duration,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Redacted,
} from "effect"
import { TestClock } from "effect/testing"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDestinationConfigurationVersion,
} from "../src/configuration.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  DeliverySupervisorLive,
  type DeliveryLoadMetrics,
} from "../src/deliverySupervisor.ts"
import {
  DestinationClient,
  type DestinationClientService,
} from "../src/destinationClient.ts"
import { DeliveryOverloaded } from "../src/errors.ts"
import { RelayIntakeStoreMemory } from "../src/layers.ts"
import {
  Destination,
  DestinationId,
  WorkerId,
} from "../src/model.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import { event } from "./fixtures.ts"

const failingDestination = Destination.make({
  id: DestinationId.make("dst-game-day-failing"),
  endpoint: new URL("https://failing.example.test/hook"),
  authorization: Redacted.make("failing-secret"),
})

const healthyDestination = Destination.make({
  id: DestinationId.make("dst-game-day-healthy"),
  endpoint: new URL("https://healthy.example.test/hook"),
  authorization: Redacted.make("healthy-secret"),
})

const makeRuntime = (
  perDestinationAdmissionCapacity: number,
  post: DestinationClientService["post"],
) => {
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      AppConfiguration,
      AppConfiguration.of({
        destination: failingDestination,
        destinationConfigurationVersion:
          defaultDestinationConfigurationVersion,
        concurrency: { global: 3, perDestination: 2 },
        flow: {
          ...defaultDeliveryFlow,
          deliveryRequestsCapacity: 4,
          deliveryRequestsPerDestinationCapacity:
            perDestinationAdmissionCapacity,
        },
        recovery: defaultDeliveryRecovery,
        resilience: {
          attemptTimeout: Duration.seconds(1),
          maxAttempts: 3,
          maxElapsed: Duration.seconds(10),
          baseDelay: Duration.seconds(1),
          maxDelay: Duration.seconds(1),
        },
      }),
    ),
    Layer.succeed(
      DestinationClient,
      DestinationClient.of({ post }),
    ),
    NodeCrypto.layer,
    RelayIntakeStoreMemory,
    makeWorkerIdentityLayer(WorkerId.make("wrk-c09-game-day")),
  )
  const supervisor = DeliverySupervisorLive.pipe(
    Layer.provide(DeliveryEventsLive),
    Layer.provide(dependencies),
  )

  return ManagedRuntime.make(
    Layer.merge(supervisor, TestClock.layer()),
  )
}

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

const awaitAttempts = (
  attempts: () => number,
  expected: number,
) =>
  Effect.gen(function* () {
    while (attempts() < expected) {
      yield* Effect.yieldNow
    }
  })

describe("C09-01 retry-storm game day", () => {
  it("reproduces healthy-traffic rejection with one global admission pool", async () => {
    const runtime = makeRuntime(
      4,
      ({ destinationId }) =>
        destinationId === failingDestination.id
          ? Effect.never
          : Effect.succeed({ status: 202 }),
    )

    try {
      const observation = await runtime.runPromise(
        Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const failing = yield* Effect.forEach(
            Array.from({ length: 4 }),
            () =>
              supervisor.deliverTo(event, failingDestination).pipe(
                Effect.forkChild({ startImmediately: true }),
              ),
          )
          const saturated = yield* awaitLoad(
            supervisor,
            (metrics) =>
              metrics.admittedByDestination.get(
                failingDestination.id,
              ) === 4,
          )
          const healthy = yield* supervisor.deliverTo(
            event,
            healthyDestination,
          ).pipe(Effect.flip)

          yield* Effect.forEach(failing, Fiber.interrupt, {
            discard: true,
          })
          return { healthy, saturated }
        }),
      )

      expect(observation.saturated.admittedDeliveries).toBe(4)
      expect(observation.healthy).toEqual(
        new DeliveryOverloaded({
          admissionCapacity: 4,
          destinationId: healthyDestination.id,
          limit: "GlobalAdmission",
        }),
      )
    } finally {
      await runtime.dispose()
    }
  })

  it("contains one destination and preserves healthy progress", async () => {
    let failingAttempts = 0
    let healthyAttempts = 0
    const runtime = makeRuntime(
      2,
      ({ destinationId }) =>
        Effect.sync(() => {
          if (destinationId === failingDestination.id) {
            failingAttempts += 1
            return { status: 429, retryAfter: "1" }
          }
          healthyAttempts += 1
          return { status: 202 }
        }),
    )

    try {
      const observation = await runtime.runPromise(
        Effect.gen(function* () {
          const supervisor = yield* DeliverySupervisor
          const failing = yield* Effect.forEach(
            Array.from({ length: 2 }),
            () =>
              supervisor.deliverTo(event, failingDestination).pipe(
                Effect.forkChild({ startImmediately: true }),
              ),
          )

          yield* awaitAttempts(() => failingAttempts, 2)
          const excess = yield* supervisor.deliverTo(
            event,
            failingDestination,
          ).pipe(Effect.flip)
          const healthy = yield* supervisor.deliverTo(
            event,
            healthyDestination,
          )
          const duringBackoff = yield* supervisor.loadMetrics()

          yield* TestClock.adjust("999 millis")
          const attemptsBeforeDelay = failingAttempts
          yield* TestClock.adjust("1 millis")
          yield* awaitAttempts(() => failingAttempts, 4)
          yield* TestClock.adjust("1 second")
          const exhausted = yield* Effect.forEach(failing, Fiber.join)
          const settled = yield* supervisor.loadMetrics()

          return {
            attemptsBeforeDelay,
            duringBackoff,
            excess,
            exhausted,
            healthy,
            settled,
          }
        }),
      )

      expect(observation.excess).toEqual(
        new DeliveryOverloaded({
          admissionCapacity: 2,
          destinationId: failingDestination.id,
          limit: "DestinationAdmission",
        }),
      )
      expect(observation.healthy._tag).toBe("Delivered")
      expect(healthyAttempts).toBe(1)
      expect(observation.duringBackoff.admittedDeliveries).toBe(2)
      expect(
        observation.duringBackoff.admittedByDestination.get(
          failingDestination.id,
        ),
      ).toBe(2)
      expect(observation.duringBackoff.globalActive).toBe(0)
      expect(observation.attemptsBeforeDelay).toBe(2)
      expect(failingAttempts).toBe(6)
      expect(
        observation.exhausted.every(
          (result) =>
            result._tag === "Exhausted" &&
            result.attempts.length === 3,
        ),
      ).toBe(true)
      expect(observation.settled.admittedDeliveries).toBe(0)
      expect(observation.settled.admittedByDestination).toEqual(
        new Map(),
      )
    } finally {
      await runtime.dispose()
    }
  })
})

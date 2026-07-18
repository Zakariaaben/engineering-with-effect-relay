import { describe, expect, it } from "bun:test"
import {
  Crypto,
  Duration,
  Effect,
  Fiber,
  Layer,
  Random,
} from "effect"
import { TestClock } from "effect/testing"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDestinationConfigurationVersion,
  type DeliveryResilience,
} from "../src/configuration.ts"
import {
  DeliverySupervisor,
  DeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  observeDeliveryAttempt,
  runDeliveryWithRetry,
} from "../src/deliveryEngine.ts"
import {
  DestinationClient,
  type DestinationClientService,
} from "../src/destinationClient.ts"
import { RelayIntakeStoreMemory } from "../src/layers.ts"
import {
  DeliveryId,
  DeliveryOutcome,
  WorkerId,
} from "../src/model.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import { destination, event, makeGate } from "./fixtures.ts"
import { reproduceImmediateRetryStorm } from "./incidents/unboundedRetryStorm.ts"

const resilience = (
  overrides: Partial<DeliveryResilience> = {},
): DeliveryResilience => ({
  attemptTimeout: Duration.seconds(1),
  maxAttempts: 4,
  maxElapsed: Duration.seconds(5),
  baseDelay: Duration.seconds(1),
  maxDelay: Duration.seconds(2),
  ...overrides,
})

const makeTestCrypto = () => {
  let sequence = 0
  return Crypto.make({
    randomBytes: (size) => {
      const bytes = new Uint8Array(size)
      bytes.fill(sequence)
      sequence += 1
      return bytes
    },
    digest: (_algorithm, data) => Effect.succeed(data),
  })
}

const makeM4Layer = (
  post: DestinationClientService["post"],
  deliveryResilience: DeliveryResilience,
) => {
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      AppConfiguration,
      AppConfiguration.of({
        destination,
        destinationConfigurationVersion:
          defaultDestinationConfigurationVersion,
        concurrency: { global: 4, perDestination: 2 },
        flow: defaultDeliveryFlow,
        recovery: defaultDeliveryRecovery,
        resilience: deliveryResilience,
      }),
    ),
    Layer.succeed(
      DestinationClient,
      DestinationClient.of({ post }),
    ),
    Layer.succeed(Crypto.Crypto, makeTestCrypto()),
    RelayIntakeStoreMemory,
    makeWorkerIdentityLayer(WorkerId.make("wrk-m4-gate")),
  )
  const supervisor = DeliverySupervisorLive.pipe(
    Layer.provide(DeliveryEventsLive),
    Layer.provide(dependencies),
  )
  return Layer.merge(supervisor, TestClock.layer())
}

const runM4 = <A, E>(
  post: DestinationClientService["post"],
  deliveryResilience: DeliveryResilience,
  program: Effect.Effect<A, E, DeliverySupervisor>,
) =>
  Effect.runPromise(
    program.pipe(
      Effect.provide(makeM4Layer(post, deliveryResilience)),
      Random.withSeed("relay-m4"),
    ),
  )

describe("C05-08 M4 act gate", () => {
  it("reproduces an immediate unbounded retry burst", async () => {
    expect(await reproduceImmediateRetryStorm(8, 4)).toEqual({
      totalAttempts: 32,
      exhaustedDeliveries: 8,
      maximumBurst: 32,
    })
  })

  it("reports a duplicate after timeout ambiguity without changing identity", async () => {
    const firstStarted = makeGate<void>()
    const deliveryIds: Array<string> = []
    let remoteEffects = 0
    let calls = 0
    const post: DestinationClientService["post"] = ({
      deliveryId,
    }) =>
      Effect.suspend(() => {
        calls += 1
        deliveryIds.push(deliveryId)
        remoteEffects += 1
        if (calls === 1) {
          firstStarted.resolve(undefined)
          return Effect.never
        }
        return Effect.succeed({ status: 202 })
      })

    const result = await runM4(
      post,
      resilience({ maxDelay: Duration.seconds(1) }),
      Effect.gen(function* () {
        const supervisor = yield* DeliverySupervisor
        const fiber = yield* supervisor.deliver(event).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(() => firstStarted.promise)
        yield* TestClock.adjust("2 seconds")
        return yield* Fiber.join(fiber)
      }),
    )

    expect(result._tag).toBe("Delivered")
    expect(result.attempts.map(({ outcome }) => outcome._tag)).toEqual([
      "TimedOut",
      "Delivered",
    ])
    expect(result.attempts.map(({ decision }) => decision._tag)).toEqual([
      "RetryScheduled",
      "Terminal",
    ])
    expect(deliveryIds[0]).toBe(deliveryIds[1])
    expect(remoteEffects).toBe(2)
  })

  it("honors Retry-After before the next attempt", async () => {
    const firstStarted = makeGate<void>()
    let calls = 0
    const post: DestinationClientService["post"] = () =>
      Effect.suspend(() => {
        calls += 1
        if (calls === 1) {
          firstStarted.resolve(undefined)
          return Effect.succeed({ status: 429, retryAfter: "3" })
        }
        return Effect.succeed({ status: 202 })
      })

    const result = await runM4(
      post,
      resilience(),
      Effect.gen(function* () {
        const supervisor = yield* DeliverySupervisor
        const fiber = yield* supervisor.deliver(event).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(() => firstStarted.promise)

        yield* TestClock.adjust("2999 millis")
        expect(calls).toBe(1)

        yield* TestClock.adjust("1 milli")
        return yield* Fiber.join(fiber)
      }),
    )

    expect(result._tag).toBe("Delivered")
    expect(result.attempts).toHaveLength(2)
    expect(result.attempts[0]?.decision).toEqual({
      _tag: "RetryScheduled",
      delayMillis: 3_000,
    })
    expect(result.attempts[1]?.startedAtMillis).toBe(3_000)
  })

  it("clamps an in-flight timeout to the remaining delivery window", async () => {
    const started = makeGate<void>()
    const post: DestinationClientService["post"] = () =>
      Effect.sync(() => started.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
      )

    const result = await runM4(
      post,
      resilience({
        attemptTimeout: Duration.seconds(10),
        maxElapsed: Duration.seconds(2),
      }),
      Effect.gen(function* () {
        const supervisor = yield* DeliverySupervisor
        const fiber = yield* supervisor.deliver(event).pipe(
          Effect.forkChild,
        )
        yield* Effect.promise(() => started.promise)
        yield* TestClock.adjust("2 seconds")
        return yield* Fiber.join(fiber)
      }),
    )

    expect(result._tag).toBe("Exhausted")
    expect(result.attempts).toHaveLength(1)
    expect(result.attempts[0]?.outcome._tag).toBe("TimedOut")
    expect(result.attempts[0]?.completedAtMillis).toBe(2_000)
  })

  it("bounds a transient fleet and exposes ordered exhaustion history", async () => {
    const deliveryCount = 8
    const attemptsByDelivery = new Map<string, number>()
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const fleet = Effect.all(
          Array.from({ length: deliveryCount }, (_, index) => {
            const deliveryId = DeliveryId.make(`dlv-fleet-${index}`)
            return runDeliveryWithRetry(
              deliveryId,
              destination.id,
              resilience(),
              (ordinal, remaining) =>
                Effect.sync(() => {
                  attemptsByDelivery.set(
                    deliveryId,
                    (attemptsByDelivery.get(deliveryId) ?? 0) + 1,
                  )
                }).pipe(
                  Effect.andThen(
                    observeDeliveryAttempt(
                      ordinal,
                      destination.id,
                      Duration.min(Duration.seconds(1), remaining),
                      Effect.succeed(
                        DeliveryOutcome.Retryable({
                          destinationId: destination.id,
                          status: 503,
                          reason: "ProviderFailure",
                        }),
                      ),
                    ),
                  ),
                ),
            )
          }),
          { concurrency: "unbounded" },
        )
        const fiber = yield* fleet.pipe(Effect.forkChild)
        yield* TestClock.adjust(Duration.infinity)
        return yield* Fiber.join(fiber)
      }).pipe(
        Effect.provide(TestClock.layer()),
        Random.withSeed("relay-m4-fleet"),
      ),
    )

    expect(results.every(({ _tag }) => _tag === "Exhausted")).toBe(true)
    expect(
      Array.from(attemptsByDelivery.values()).reduce(
        (total, attempts) => total + attempts,
        0,
      ),
    ).toBeLessThanOrEqual(deliveryCount * 4)
    expect(attemptsByDelivery.size).toBe(deliveryCount)
    expect(
      Array.from(attemptsByDelivery.values())
        .every((attempts) => attempts <= 4),
    ).toBe(true)

    for (const result of results) {
      expect(
        result.attempts.map(({ ordinal }) => ordinal),
      ).toEqual(
        Array.from(
          { length: result.attempts.length },
          (_, index) => index + 1,
        ),
      )
      expect(result.attempts.at(-1)?.decision._tag).toBe("Exhausted")
      expect(
        result.attempts.every(
          ({ startedAtMillis }) => startedAtMillis <= 5_000,
        ),
      ).toBe(true)
    }
  })
})

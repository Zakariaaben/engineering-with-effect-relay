import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect, Layer, Option } from "effect"
import { TestClock } from "effect/testing"
import { ClaimLostError } from "../src/errors.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  ClaimGeneration,
  DeliveryAttemptDecision,
  DeliveryOutcome,
  WorkerId,
  makeDeliveryAttemptRecord,
  type DeliveryAttempt,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "../src/services.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import {
  delivery,
  destination,
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const workerA = WorkerId.make("wrk-operations-a")
const workerB = WorkerId.make("wrk-operations-b")
const leaseDurationMillis = 30_000
const traceA = {
  traceId: "11111111111111111111111111111111",
  spanId: "aaaaaaaaaaaaaaaa",
}
const traceB = {
  traceId: "22222222222222222222222222222222",
  spanId: "bbbbbbbbbbbbbbbb",
}

const memoryWithTestClock = Layer.merge(
  RelayPersistenceMemory,
  TestClock.layer(),
)

const runMemory = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DeliveryRepository | RelayIntakeStore | TestClock.TestClock
  >,
) => Effect.runPromise(program.pipe(Effect.provide(memoryWithTestClock)))

describe("C08-12 distributed delivery operations", () => {
  it("persists the supervisor attempt before dead-lettering poison work", async () => {
    const application = await startRelayApplication({
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
        RELAY_DESTINATION_ID: "dst-operations",
        RELAY_DESTINATION_URL: "https://hooks.example.test/operations",
        RELAY_INTAKE_AUTHORIZATION: "intake-secret",
        RELAY_RETRY_MAX_ATTEMPTS: 1,
      }),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.succeed(makeHttpResponse(request, 599))
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
      workerIdentityLayer: makeWorkerIdentityLayer(workerA),
    })

    try {
      const result = await application.deliver(event)
      const status = await application.deliveryStatus(result.deliveryId)
      const deadLetters = await application.deadLetters(10)

      expect(result._tag).toBe("Exhausted")
      expect(status?.delivery.state).toEqual({
        _tag: "DeadLettered",
        reason: "RetryBudgetExhausted",
      })
      expect(status?.attempts).toEqual([
        expect.objectContaining({
          ordinal: 1,
          workerId: workerA,
          claimGeneration: ClaimGeneration.make(1),
          outcome: "Retryable",
          decision: "Exhausted",
          status: 599,
        }),
      ])
      expect(deadLetters.map(({ delivery }) => delivery.id)).toEqual([
        result.deliveryId,
      ])
    } finally {
      await application.shutdown()
    }
  })

  it("preserves history across repair and fences the replaced worker", async () => {
    const evidence = await runMemory(Effect.gen(function* () {
      const intake = yield* RelayIntakeStore
      const repository = yield* DeliveryRepository
      const first = yield* intake.savePending(
        event,
        delivery.id,
        destination.id,
        { ownerId: workerA, leaseDurationMillis },
      )
      const firstAttempt = {
        ordinal: 1,
        startedAtMillis: 0,
        completedAtMillis: 10,
        outcome: DeliveryOutcome.Retryable({
          destinationId: destination.id,
          status: 503,
          reason: "ProviderFailure",
        }),
        decision: DeliveryAttemptDecision.RetryScheduled({
          delayMillis: 100,
        }),
      } satisfies DeliveryAttempt
      const secondAttempt = {
        ordinal: 2,
        startedAtMillis: 110,
        completedAtMillis: 120,
        outcome: DeliveryOutcome.Retryable({
          destinationId: destination.id,
          status: 503,
          reason: "ProviderFailure",
        }),
        decision: DeliveryAttemptDecision.Exhausted(),
      } satisfies DeliveryAttempt

      yield* repository.recordAttempt(
        makeDeliveryAttemptRecord(
          delivery.id,
          first.claim,
          firstAttempt,
          traceA,
        ),
      )
      yield* repository.recordAttempt(
        makeDeliveryAttemptRecord(
          delivery.id,
          first.claim,
          secondAttempt,
          traceA,
        ),
      )
      const deadLetters = yield* repository.listDeadLetters(10)

      yield* repository.retryDeadLetter(delivery.id)
      const [second] = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      if (second === undefined) {
        return yield* Effect.die(new Error("expected repaired work"))
      }
      const thirdAttempt = {
        ordinal: second.nextAttemptOrdinal,
        startedAtMillis: 130,
        completedAtMillis: 140,
        outcome: DeliveryOutcome.Delivered({
          destinationId: destination.id,
          status: 202,
        }),
        decision: DeliveryAttemptDecision.Terminal(),
      } satisfies DeliveryAttempt
      const staleRecord = yield* Effect.flip(
        repository.recordAttempt(
          makeDeliveryAttemptRecord(
            delivery.id,
            first.claim,
            thirdAttempt,
            traceA,
          ),
        ),
      )
      yield* repository.recordAttempt(
        makeDeliveryAttemptRecord(
          delivery.id,
          second.claim,
          thirdAttempt,
          traceB,
        ),
      )
      return {
        deadLetters,
        finalStatus: Option.getOrThrow(
          yield* repository.findStatus(delivery.id),
        ),
        second,
        staleRecord,
      }
    }))

    expect(evidence.deadLetters).toHaveLength(1)
    expect(evidence.second.claim.generation).toBe(ClaimGeneration.make(2))
    expect(evidence.second.nextAttemptOrdinal).toBe(3)
    expect(evidence.staleRecord).toBeInstanceOf(ClaimLostError)
    expect(evidence.finalStatus.delivery.state._tag).toBe("Delivered")
    expect(evidence.finalStatus.attempts.map((attempt) => ({
      ordinal: attempt.ordinal,
      workerId: attempt.workerId,
      generation: attempt.claimGeneration,
      traceId: attempt.traceId,
    }))).toEqual([
      {
        ordinal: 1,
        workerId: workerA,
        generation: ClaimGeneration.make(1),
        traceId: traceA.traceId,
      },
      {
        ordinal: 2,
        workerId: workerA,
        generation: ClaimGeneration.make(1),
        traceId: traceA.traceId,
      },
      {
        ordinal: 3,
        workerId: workerB,
        generation: ClaimGeneration.make(2),
        traceId: traceB.traceId,
      },
    ])
  })

  it("keeps scheduled retries ineligible and measures later claim lag", async () => {
    const evidence = await runMemory(Effect.gen(function* () {
      const intake = yield* RelayIntakeStore
      const repository = yield* DeliveryRepository
      const first = yield* intake.savePending(
        event,
        delivery.id,
        destination.id,
        { ownerId: workerA, leaseDurationMillis },
      )
      const attempt = {
        ordinal: 1,
        startedAtMillis: 0,
        completedAtMillis: 0,
        outcome: DeliveryOutcome.Retryable({
          destinationId: destination.id,
          status: 503,
          reason: "ProviderFailure",
        }),
        decision: DeliveryAttemptDecision.RetryScheduled({
          delayMillis: 1_000,
        }),
      } satisfies DeliveryAttempt

      yield* repository.recordAttempt(
        makeDeliveryAttemptRecord(
          delivery.id,
          first.claim,
          attempt,
          traceA,
        ),
      )
      yield* repository.releaseClaim(delivery.id, first.claim)
      const beforeEligible = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      yield* TestClock.adjust("1500 millis")
      const afterEligible = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )

      return { afterEligible, beforeEligible }
    }))

    expect(evidence.beforeEligible).toEqual([])
    expect(evidence.afterEligible).toEqual([
      expect.objectContaining({
        claimLagMillis: 500,
        nextAttemptOrdinal: 2,
      }),
    ])
  })
})

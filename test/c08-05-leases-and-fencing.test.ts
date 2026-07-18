import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
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
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  DeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { ClaimLostError } from "../src/errors.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  ClaimGeneration,
  DeliveryResult,
  WorkerId,
} from "../src/model.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "../src/services.ts"
import { delivery, destination, event } from "./fixtures.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"

const workerA = WorkerId.make("wrk-lease-a")
const workerB = WorkerId.make("wrk-lease-b")
const leaseDurationMillis = 10_000

const persistenceWithTestClock = Layer.merge(
  RelayPersistenceMemory,
  TestClock.layer(),
)

const run = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DeliveryRepository | RelayIntakeStore | TestClock.TestClock
  >,
) => Effect.runPromise(program.pipe(Effect.provide(persistenceWithTestClock)))

const saveInitiallyClaimed = Effect.gen(function* () {
  const intake = yield* RelayIntakeStore
  return yield* intake.savePending(
    event,
    delivery.id,
    destination.id,
    {
      ownerId: workerA,
      leaseDurationMillis,
    },
  )
})

const delivered = DeliveryResult.Delivered({
  attempts: [],
  deliveryId: delivery.id,
  destinationId: destination.id,
  status: 202,
})

describe("C08-05 claims, leases, and fencing", () => {
  it("reclaims an expired lease with a higher generation and fences the stale worker", async () => {
    const evidence = await run(Effect.gen(function* () {
      const repository = yield* DeliveryRepository
      const first = yield* saveInitiallyClaimed

      const beforeExpiry = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      yield* TestClock.adjust("10 seconds")
      const [second] = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      if (second === undefined) {
        return yield* Effect.die(new Error("expected an expired claim"))
      }

      const staleCompletion = yield* Effect.flip(
        repository.completeClaim(delivery.id, first.claim, delivered),
      )
      const afterStaleCompletion = yield* repository.findById(delivery.id)

      yield* repository.completeClaim(delivery.id, second.claim, delivered)
      const afterCurrentCompletion = yield* repository.findById(delivery.id)
      const duplicateCompletion = yield* Effect.flip(
        repository.completeClaim(delivery.id, second.claim, delivered),
      )

      return {
        afterCurrentCompletion,
        afterStaleCompletion,
        beforeExpiry,
        duplicateCompletion,
        first,
        second,
        staleCompletion,
      }
    }))

    expect(evidence.beforeExpiry).toEqual([])
    expect(evidence.first.claim.generation).toBe(ClaimGeneration.make(1))
    expect(evidence.second.claim.generation).toBe(ClaimGeneration.make(2))
    expect(evidence.staleCompletion).toBeInstanceOf(ClaimLostError)
    expect(
      Option.getOrThrow(evidence.afterStaleCompletion).state._tag,
    ).toBe("Pending")
    expect(
      Option.getOrThrow(evidence.afterCurrentCompletion).state._tag,
    ).toBe("Delivered")
    expect(evidence.duplicateCompletion).toBeInstanceOf(ClaimLostError)
  })

  it("extends ownership only when the current generation renews before expiry", async () => {
    const evidence = await run(Effect.gen(function* () {
      const repository = yield* DeliveryRepository
      const first = yield* saveInitiallyClaimed

      yield* TestClock.adjust("8 seconds")
      const renewed = yield* repository.renewClaim(
        delivery.id,
        first.claim,
        leaseDurationMillis,
      )
      yield* TestClock.adjust("2 seconds")
      const atOriginalExpiry = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      yield* TestClock.adjust("8 seconds")
      const afterRenewedExpiry = yield* repository.claimPending(
        workerB,
        destination.id,
        1,
        leaseDurationMillis,
      )
      const staleRenewal = yield* Effect.flip(
        repository.renewClaim(
          delivery.id,
          renewed,
          leaseDurationMillis,
        ),
      )

      return {
        afterRenewedExpiry,
        atOriginalExpiry,
        first,
        renewed,
        staleRenewal,
      }
    }))

    expect(evidence.renewed.generation).toBe(evidence.first.claim.generation)
    expect(evidence.renewed.leaseExpiresAtMillis).toBe(18_000)
    expect(evidence.atOriginalExpiry).toEqual([])
    expect(evidence.afterRenewedExpiry[0]?.claim.generation).toBe(
      ClaimGeneration.make(2),
    )
    expect(evidence.staleRenewal).toBeInstanceOf(ClaimLostError)
  })

  it("renews a claim before and during long-running delivery work", async () => {
    let renewals = 0

    await Effect.runPromise(Effect.gen(function* () {
      const outboundStarted = yield* Deferred.make<void>()
      const repository = DeliveryRepository.of({
        save: () => Effect.void,
        findById: () => Effect.succeed(Option.none()),
        findStatus: () => Effect.succeed(Option.none()),
        recordAttempt: () => Effect.void,
        listDeadLetters: () => Effect.succeed([]),
        retryDeadLetter: () => Effect.void,
        claimPending: () => Effect.succeed([]),
        renewClaim: (_deliveryId, current, durationMillis) =>
          Effect.sync(() => {
            renewals += 1
            return {
              ...current,
              leaseExpiresAtMillis:
                current.leaseExpiresAtMillis + durationMillis,
            }
          }),
        completeClaim: () => Effect.void,
        releaseClaim: () => Effect.void,
      })
      const intake = RelayIntakeStore.of({
        accept: () => Effect.die(new Error("not used by this test")),
        savePending: () => Effect.die(new Error("not used by this test")),
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
              claimLeaseDuration: Duration.seconds(30),
              claimRenewInterval: Duration.seconds(10),
            },
            resilience: defaultDeliveryResilience,
          }),
        ),
        Layer.succeed(
          DestinationClient,
          DestinationClient.of({
            post: () =>
              Deferred.succeed(outboundStarted, undefined).pipe(
                Effect.andThen(Effect.never),
              ),
          }),
        ),
        Layer.succeed(DeliveryRepository, repository),
        Layer.succeed(RelayIntakeStore, intake),
        makeWorkerIdentityLayer(workerA),
        NodeCrypto.layer,
      )
      const supervisor = DeliverySupervisorLive.pipe(
        Layer.provide(DeliveryEventsLive),
        Layer.provide(dependencies),
      )
      const testLayer = Layer.merge(supervisor, TestClock.layer())

      yield* Effect.gen(function* () {
        const service = yield* DeliverySupervisor
        const fiber = yield* service.resumeClaimed({
          claim: {
            ownerId: workerA,
            generation: ClaimGeneration.make(1),
            leaseExpiresAtMillis: 30_000,
          },
          claimLagMillis: 0,
          delivery,
          event,
          nextAttemptOrdinal: 1,
          route: Option.none(),
        }).pipe(Effect.forkChild)

        yield* Deferred.await(outboundStarted)
        expect(renewals).toBe(1)

        yield* TestClock.adjust("10 seconds")
        expect(renewals).toBe(2)

        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.provide(testLayer))
    }))
  })
})

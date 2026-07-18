import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Context,
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
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  type DeliverySupervisorHooks,
  makeDeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import {
  ClaimGeneration,
  type DeliveryId,
  WorkerId,
} from "../src/model.ts"
import {
  DeliveryRepository,
  RelayIntakeStore,
} from "../src/services.ts"
import { makeWorkerIdentityLayer } from "../src/workerIdentity.ts"
import { destination, event, makeGate } from "./fixtures.ts"

const workerA = WorkerId.make("wrk-m7-m8-a")
const workerB = WorkerId.make("wrk-m7-m8-b")
const leaseDurationMillis = 30_000

const configuration = Layer.succeed(
  AppConfiguration,
  AppConfiguration.of({
    destination,
    destinationConfigurationVersion:
      defaultDestinationConfigurationVersion,
    concurrency: { global: 1, perDestination: 1 },
    flow: defaultDeliveryFlow,
    recovery: {
      ...defaultDeliveryRecovery,
      claimLeaseDuration: Duration.millis(leaseDurationMillis),
      claimRenewInterval: Duration.seconds(10),
    },
    resilience: {
      ...defaultDeliveryResilience,
      maxAttempts: 1,
    },
  }),
)

const makeReceiver = (deduplicate: boolean) => {
  const applied = new Set<DeliveryId>()
  const attempts: Array<DeliveryId> = []
  let effects = 0

  const client = DestinationClient.of({
    post: (request) =>
      Effect.sync(() => {
        attempts.push(request.deliveryId)
        if (!deduplicate || !applied.has(request.deliveryId)) {
          applied.add(request.deliveryId)
          effects += 1
        }
        return { status: 202 }
      }),
  })

  return {
    attempts,
    client,
    effectCount: () => effects,
  }
}

const makeWorkerRuntime = (
  workerId: WorkerId,
  repository: Context.Service.Shape<typeof DeliveryRepository>,
  intake: Context.Service.Shape<typeof RelayIntakeStore>,
  destinationClient: Context.Service.Shape<typeof DestinationClient>,
  hooks: DeliverySupervisorHooks = {},
) => {
  const dependencies = Layer.mergeAll(
    configuration,
    Layer.succeed(DeliveryRepository, repository),
    Layer.succeed(RelayIntakeStore, intake),
    Layer.succeed(DestinationClient, destinationClient),
    makeWorkerIdentityLayer(workerId),
    NodeCrypto.layer,
  )
  const supervisor = makeDeliverySupervisorLive(hooks).pipe(
    Layer.provide(DeliveryEventsLive),
    Layer.provide(dependencies),
  )

  return ManagedRuntime.make(
    Layer.merge(supervisor, TestClock.layer()),
  )
}

const scenarios = [
  {
    deduplicate: false,
    expectedEffects: 2,
    name: "a receiver that ignores the stable key",
  },
  {
    deduplicate: true,
    expectedEffects: 1,
    name: "a receiver that atomically enforces the stable key",
  },
] as const

describe("Relay M7/M8 act gate", () => {
  for (const scenario of scenarios) {
    it(`survives remote success and local uncertainty with ${scenario.name}`, async () => {
      const persistence = ManagedRuntime.make(RelayPersistenceMemory)
      const persistenceContext = await persistence.context()
      const repository = Context.get(
        persistenceContext,
        DeliveryRepository,
      )
      const intake = Context.get(persistenceContext, RelayIntakeStore)
      const receiver = makeReceiver(scenario.deduplicate)
      const responseObserved = makeGate<DeliveryId>()

      const crashRepository = DeliveryRepository.of({
        ...repository,
        releaseClaim: (deliveryId, claim) =>
          claim.ownerId === workerA
            ? Effect.void
            : repository.releaseClaim(deliveryId, claim),
      })
      const first = makeWorkerRuntime(
        workerA,
        crashRepository,
        intake,
        receiver.client,
        {
          afterAttemptObserved: (deliveryId) =>
            Effect.sync(() => responseObserved.resolve(deliveryId)).pipe(
              Effect.andThen(Effect.never),
            ),
        },
      )

      try {
        const firstContext = await first.context()
        const firstSupervisor = Context.get(
          firstContext,
          DeliverySupervisor,
        )
        const firstResult = first.runPromise(
          firstSupervisor.deliver(event),
        ).then(
          () => "Completed" as const,
          () => "Interrupted" as const,
        )
        const deliveryId = await responseObserved.promise
        const uncertain = Option.getOrThrow(
          await first.runPromise(repository.findStatus(deliveryId)),
        )

        expect(uncertain.delivery.state._tag).toBe("Pending")
        expect(uncertain.attempts).toEqual([])

        await first.dispose()
        expect(await firstResult).toBe("Interrupted")

        const second = makeWorkerRuntime(
          workerB,
          repository,
          intake,
          receiver.client,
        )
        try {
          const secondContext = await second.context()
          const secondSupervisor = Context.get(
            secondContext,
            DeliverySupervisor,
          )
          const beforeExpiry = await second.runPromise(
            repository.claimPending(
              workerB,
              destination.id,
              1,
              leaseDurationMillis,
            ),
          )

          await second.runPromise(TestClock.adjust("30 seconds"))
          const [replacement] = await second.runPromise(
            repository.claimPending(
              workerB,
              destination.id,
              1,
              leaseDurationMillis,
            ),
          )
          if (replacement === undefined) {
            throw new Error("expected expired work to be reclaimed")
          }

          const recovered = await second.runPromise(
            secondSupervisor.resumeClaimed(replacement),
          )
          const finalStatus = Option.getOrThrow(
            await second.runPromise(repository.findStatus(deliveryId)),
          )

          expect(beforeExpiry).toEqual([])
          expect(replacement.claim.generation).toBe(
            ClaimGeneration.make(2),
          )
          expect(replacement.nextAttemptOrdinal).toBe(1)
          expect(recovered._tag).toBe("Delivered")
          expect(finalStatus.delivery.state._tag).toBe("Delivered")
          expect(finalStatus.attempts).toEqual([
            expect.objectContaining({
              claimGeneration: ClaimGeneration.make(2),
              decision: "Terminal",
              ordinal: 1,
              outcome: "Delivered",
              workerId: workerB,
            }),
          ])
          expect(receiver.attempts).toEqual([deliveryId, deliveryId])
          expect(receiver.effectCount()).toBe(scenario.expectedEffects)
        } finally {
          await second.dispose()
        }
      } finally {
        await first.dispose()
        await persistence.dispose()
      }
    })
  }

  it("makes the terminal attempt and local state one durable mutation", async () => {
    const persistence = ManagedRuntime.make(RelayPersistenceMemory)
    const persistenceContext = await persistence.context()
    const repository = Context.get(persistenceContext, DeliveryRepository)
    const intake = Context.get(persistenceContext, RelayIntakeStore)
    const receiver = makeReceiver(false)
    const attemptRecorded = makeGate<DeliveryId>()
    const first = makeWorkerRuntime(
      workerA,
      repository,
      intake,
      receiver.client,
      {
        afterAttemptRecorded: (deliveryId) =>
          Effect.sync(() => attemptRecorded.resolve(deliveryId)).pipe(
            Effect.andThen(Effect.never),
          ),
      },
    )

    try {
      const firstContext = await first.context()
      const firstSupervisor = Context.get(
        firstContext,
        DeliverySupervisor,
      )
      const firstResult = first.runPromise(
        firstSupervisor.deliver(event),
      ).then(
        () => "Completed" as const,
        () => "Interrupted" as const,
      )
      const deliveryId = await attemptRecorded.promise
      const committed = Option.getOrThrow(
        await first.runPromise(repository.findStatus(deliveryId)),
      )

      expect(committed.delivery.state._tag).toBe("Delivered")
      expect(committed.attempts).toHaveLength(1)

      await first.dispose()
      expect(await firstResult).toBe("Interrupted")

      const second = makeWorkerRuntime(
        workerB,
        repository,
        intake,
        receiver.client,
      )
      try {
        await second.context()
        await second.runPromise(TestClock.adjust("30 seconds"))
        const reclaimed = await second.runPromise(
          repository.claimPending(
            workerB,
            destination.id,
            1,
            leaseDurationMillis,
          ),
        )

        expect(reclaimed).toEqual([])
        expect(receiver.attempts).toEqual([deliveryId])
        expect(receiver.effectCount()).toBe(1)
      } finally {
        await second.dispose()
      }
    } finally {
      await first.dispose()
      await persistence.dispose()
    }
  })
})

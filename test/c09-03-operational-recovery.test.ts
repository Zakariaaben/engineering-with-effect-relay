import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Effect,
  Layer,
  ManagedRuntime,
  Option,
  Redacted,
} from "effect"
import { TestClock } from "effect/testing"
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryRecovery,
  defaultDeliveryResilience,
} from "../src/configuration.ts"
import {
  DeliveryOperations,
  DeliveryOperationsLive,
} from "../src/deliveryOperations.ts"
import {
  DeadLetterStateProblem,
  deliveryAuthorizationClientLayer,
  operationsAuthorizationClientLayer,
  RelayHttpApi,
  UnauthorizedProblem,
} from "../src/httpServer.ts"
import { RelayPersistenceMemory } from "../src/adapters/memoryPersistence.ts"
import {
  ClaimGeneration,
  ConfigurationVersion,
  DeliveryAttemptDecision,
  DeliveryId,
  DeliveryOutcome,
  DeliveryRouteSnapshot,
  EventId,
  IngestionKey,
  RequestFingerprint,
  WorkerId,
  makeDeliveryAttemptRecord,
  type DeliveryAttempt,
} from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  DeliveryRepository,
  IntakeDecision,
  RelayIntakeStore,
} from "../src/services.ts"
import {
  destination,
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const intakeToken = Redacted.make("intake-operations-secret")
const operationsToken = Redacted.make("operations-secret")

const callOperations = <A>(
  baseUrl: string,
  token: Redacted.Redacted,
  use: (
    client: HttpApiClient.ForApi<typeof RelayHttpApi>,
  ) => Effect.Effect<A, unknown>,
) =>
  Effect.gen(function* () {
    const client = yield* HttpApiClient.make(RelayHttpApi, { baseUrl })
    return yield* use(client)
  }).pipe(
    Effect.provide(deliveryAuthorizationClientLayer(intakeToken)),
    Effect.provide(operationsAuthorizationClientLayer(token)),
    Effect.provide(NodeHttpClient.layerNodeHttp),
    Effect.runPromise,
  )

describe("C09-03 operational recovery", () => {
  it("separates operations authorization and applies conditional retry or termination", async () => {
    const application = await startRelayApplication({
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
        RELAY_DESTINATION_ID: "dst-operations-capstone",
        RELAY_DESTINATION_URL:
          "https://hooks.example.test/operations-capstone",
        RELAY_INTAKE_AUTHORIZATION: Redacted.value(intakeToken),
        RELAY_OPERATIONS_AUTHORIZATION:
          Redacted.value(operationsToken),
        RELAY_RECOVERY_POLL_INTERVAL: "1 hour",
        RELAY_RETRY_MAX_ATTEMPTS: 1,
      }),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.succeed(makeHttpResponse(request, 599))
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })

    try {
      const terminatedResult = await application.deliver(event)
      const retriedResult = await application.deliver(event)

      const unauthorized = await callOperations(
        application.httpAddress,
        intakeToken,
        (client) =>
          client.operations.inspect({
            params: { deliveryId: terminatedResult.deliveryId },
          }).pipe(Effect.flip),
      )
      expect(unauthorized).toBeInstanceOf(UnauthorizedProblem)

      const inspected = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.inspect({
          params: { deliveryId: terminatedResult.deliveryId },
        }),
      )
      expect(inspected.delivery.state._tag).toBe("DeadLettered")
      expect(inspected.attempts).toHaveLength(1)

      const listed = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.listDeadLetters(),
      )
      expect(listed.map(({ delivery }) => delivery.id).sort()).toEqual(
        [terminatedResult.deliveryId, retriedResult.deliveryId].sort(),
      )

      const terminated = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.terminateDeadLetter({
          params: { deliveryId: terminatedResult.deliveryId },
        }),
      )
      expect(terminated.delivery.state).toEqual({
        _tag: "Terminated",
        reason: "OperatorTerminated",
      })
      expect(terminated.attempts).toEqual(inspected.attempts)

      const repeatedTermination = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) =>
          client.operations.terminateDeadLetter({
            params: { deliveryId: terminatedResult.deliveryId },
          }).pipe(Effect.flip),
      )
      expect(repeatedTermination).toBeInstanceOf(DeadLetterStateProblem)

      const retried = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.retryDeadLetter({
          params: { deliveryId: retriedResult.deliveryId },
        }),
      )
      expect(retried.delivery.state._tag).toBe("Pending")
      expect(retried.attempts).toHaveLength(1)

      const remaining = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.listDeadLetters(),
      )
      expect(remaining).toEqual([])

      const reconciliation = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.reconcile(),
      )
      expect(reconciliation).toEqual({ claimed: 1 })

      const replayed = await callOperations(
        application.httpAddress,
        operationsToken,
        (client) => client.operations.inspect({
          params: { deliveryId: retriedResult.deliveryId },
        }),
      )
      expect(replayed.delivery.state._tag).toBe("DeadLettered")
      expect(replayed.attempts).toHaveLength(2)
    } finally {
      await application.shutdown()
    }
  })

  it("retains the accepted route on retry and installs the trusted current route on repair", async () => {
    const oldRoute = DeliveryRouteSnapshot.make({
      destinationId: destination.id,
      endpoint: new URL("https://old.example.test/invoices"),
      configurationVersion: ConfigurationVersion.make(1),
    })
    const currentRoute = DeliveryRouteSnapshot.make({
      destinationId: destination.id,
      endpoint: new URL("https://current.example.test/invoices"),
      configurationVersion: ConfigurationVersion.make(2),
    })
    const configuration = Layer.succeed(
      AppConfiguration,
      AppConfiguration.of({
        concurrency: { global: 1, perDestination: 1 },
        destination: {
          ...destination,
          endpoint: currentRoute.endpoint,
        },
        destinationConfigurationVersion:
          currentRoute.configurationVersion,
        flow: defaultDeliveryFlow,
        recovery: defaultDeliveryRecovery,
        resilience: defaultDeliveryResilience,
      }),
    )
    const dependencies = Layer.mergeAll(
      configuration,
      RelayPersistenceMemory,
      TestClock.layer(),
    )
    const operations = DeliveryOperationsLive.pipe(
      Layer.provide(dependencies),
    )
    const runtime = ManagedRuntime.make(
      Layer.mergeAll(operations, dependencies),
    )

    try {
      const evidence = await runtime.runPromise(Effect.gen(function* () {
        const service = yield* DeliveryOperations
        const repository = yield* DeliveryRepository
        const intake = yield* RelayIntakeStore

        const createDeadLetter = Effect.fn("Test.createDeadLetter")(
          function* (suffix: "retry" | "repair") {
            const deliveryId = DeliveryId.make(
              `dlv-operations-${suffix}`,
            )
            const accepted = yield* intake.accept({
              acceptedAtMillis: 0,
              claim: {
                leaseDurationMillis: 30_000,
                ownerId: WorkerId.make("wrk-operations-original"),
              },
              deliveryId,
              event: {
                ...event,
                id: EventId.make(`evt-operations-${suffix}`),
              },
              ingestionKey: IngestionKey.make(
                `operations:${suffix}`,
              ),
              requestFingerprint: RequestFingerprint.make(
                suffix === "retry" ? "a".repeat(64) : "b".repeat(64),
              ),
              route: oldRoute,
            })
            if (!IntakeDecision.$is("Accepted")(accepted)) {
              return yield* Effect.die(new Error("expected acceptance"))
            }
            const attempt = {
              completedAtMillis: 10,
              decision: DeliveryAttemptDecision.Exhausted(),
              ordinal: 1,
              outcome: DeliveryOutcome.Retryable({
                destinationId: destination.id,
                reason: "ProviderFailure",
                status: 503,
              }),
              startedAtMillis: 0,
            } satisfies DeliveryAttempt
            yield* repository.recordAttempt(
              makeDeliveryAttemptRecord(
                deliveryId,
                accepted.claim,
                attempt,
                { spanId: null, traceId: null },
              ),
            )
            return deliveryId
          },
        )

        const retryId = yield* createDeadLetter("retry")
        const retryStatus = yield* service.retryDeadLetter(retryId)
        const [retried] = yield* repository.claimPending(
          WorkerId.make("wrk-operations-retry"),
          destination.id,
          1,
          30_000,
        )
        if (retried === undefined) {
          return yield* Effect.die(new Error("expected retried claim"))
        }

        const repairId = yield* createDeadLetter("repair")
        const repairStatus = yield* service.repairDeadLetter(repairId)
        const [repaired] = yield* repository.claimPending(
          WorkerId.make("wrk-operations-repair"),
          destination.id,
          1,
          30_000,
        )
        if (repaired === undefined) {
          return yield* Effect.die(new Error("expected repaired claim"))
        }

        return { repaired, repairStatus, retried, retryStatus }
      }))

      expect(evidence.retryStatus.attempts).toHaveLength(1)
      expect(evidence.repairStatus.attempts).toHaveLength(1)
      expect(evidence.retried.claim.generation).toBe(
        ClaimGeneration.make(2),
      )
      expect(evidence.repaired.claim.generation).toBe(
        ClaimGeneration.make(2),
      )
      expect(evidence.retried.nextAttemptOrdinal).toBe(2)
      expect(evidence.repaired.nextAttemptOrdinal).toBe(2)
      expect(
        Option.getOrThrow(evidence.retried.route).endpoint.href,
      ).toBe(oldRoute.endpoint.href)
      expect(
        Option.getOrThrow(evidence.repaired.route).endpoint.href,
      ).toBe(currentRoute.endpoint.href)
      expect(
        Option.getOrThrow(evidence.repaired.route).configurationVersion,
      ).toBe(ConfigurationVersion.make(2))
    } finally {
      await runtime.dispose()
    }
  })
})

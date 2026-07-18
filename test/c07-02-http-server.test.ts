import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
} from "effect"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import { DeliveryOperations } from "../src/deliveryOperations.ts"
import { DeliverySupervisor } from "../src/deliverySupervisor.ts"
import { EventIntake } from "../src/eventIntake.ts"
import { DeliveryOverloaded } from "../src/errors.ts"
import {
  DeliveryHttpRoutes,
  IntakeAuthorization,
  OperationsAuthorization,
} from "../src/httpServer.ts"
import { RelayPersistenceMemory } from "../src/layers.ts"
import { DestinationId } from "../src/model.ts"
import { startRelayApplication } from "../src/runtime.ts"
import { RelayReadiness } from "../src/readiness.ts"
import { Reconciler } from "../src/reconciler.ts"
import {
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const configuration = (capacity = 4) =>
  ConfigProvider.fromUnknown({
    RELAY_DELIVERY_REQUESTS_CAPACITY: capacity,
    RELAY_DESTINATION_AUTHORIZATION: "http-secret",
    RELAY_DESTINATION_CONCURRENCY: 1,
    RELAY_DESTINATION_ID: "dst-http",
    RELAY_DESTINATION_URL: "https://hooks.example.test/http",
    RELAY_GLOBAL_CONCURRENCY: 1,
    RELAY_INTAKE_AUTHORIZATION: "intake-secret",
    RELAY_OPERATIONS_AUTHORIZATION: "operations-secret",
  })

const post = (
  address: string,
  body: string,
  options?: {
    readonly contentType?: string
    readonly signal?: AbortSignal
  },
) =>
  fetch(`${address}/deliveries`, {
    method: "POST",
    headers: {
      authorization: "Bearer intake-secret",
      "content-type": options?.contentType ?? "application/json",
    },
    body,
    ...(options?.signal === undefined
      ? {}
      : { signal: options.signal }),
  })

type DeliverySupervisorService = Context.Service.Shape<
  typeof DeliverySupervisor
>

const makeHandler = (
  deliver: DeliverySupervisorService["deliver"],
) => {
  const service = DeliverySupervisor.of({
    activeCount: () => Effect.succeed(0),
    concurrencyMetrics: () => Effect.succeed({
      globalActive: 0,
      activeByDestination: new Map(),
    }),
    deliver,
    deliverTo: (candidate) => deliver(candidate),
    enqueueClaimed: () => Effect.void,
    resumeClaimed: () =>
      Effect.die(new Error("not used by this adapter test")),
    loadMetrics: () => Effect.succeed({
      activeDeliveries: 0,
      admittedByDestination: new Map(),
      admittedDeliveries: 0,
      globalActive: 0,
      activeByDestination: new Map(),
      globalConcurrencyLimit: 1,
      perDestinationAdmissionCapacity: 1,
      perDestinationConcurrencyLimit: 1,
      rejected: 0,
      requestQueueCapacity: 1,
      requestQueueDepth: 0,
    }),
  })

  const webHandler = HttpRouter.toWebHandler(
    DeliveryHttpRoutes.pipe(
      Layer.provide(HttpServer.layerServices),
      Layer.provide(Layer.succeed(
        IntakeAuthorization,
        IntakeAuthorization.of({
          token: Redacted.make("intake-secret"),
        }),
      )),
      Layer.provide(Layer.succeed(
        OperationsAuthorization,
        OperationsAuthorization.of({
          operationsBearerAuth: (effect) => effect,
        }),
      )),
    ),
    { disableLogger: true },
  )
  const readiness = RelayReadiness.of({
    current: Effect.succeed(true),
    markReady: Effect.void,
    markNotReady: Effect.void,
  })
  const eventIntake = EventIntake.of({
    accept: () => Effect.die(new Error("not used by this adapter test")),
  })
  const operations = DeliveryOperations.of({
    status: () => Effect.succeed(Option.none()),
    listDeadLetters: () => Effect.succeed([]),
    retryDeadLetter: () => Effect.die(new Error("not used")),
    repairDeadLetter: () => Effect.die(new Error("not used")),
    terminateDeadLetter: () => Effect.die(new Error("not used")),
  })
  const reconciler = Reconciler.of({
    reconcileOnce: () => Effect.succeed({ claimed: 0 }),
  })
  return {
    dispose: webHandler.dispose,
    handler: (request: Request) =>
      webHandler.handler(
        request,
        Context.make(DeliverySupervisor, service).pipe(
          Context.add(EventIntake, eventIntake),
          Context.add(DeliveryOperations, operations),
          Context.add(Reconciler, reconciler),
          Context.add(RelayReadiness, readiness),
        ),
      ),
  }
}

describe("C07-02 HTTP server boundary", () => {
  it("decodes before the service and maps domain evidence to HTTP deliberately", async () => {
    const statuses = [202, 400]
    let outboundCalls = 0
    const application = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          const status = statuses[outboundCalls] ?? 202
          outboundCalls += 1
          return makeHttpResponse(request, status)
        }),
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })

    try {
      const delivered = await post(
        application.httpAddress,
        JSON.stringify(event),
      )
      expect(delivered.status).toBe(200)
      expect(delivered.headers.get("cache-control")).toBe("no-store")
      expect(delivered.headers.get("x-content-type-options")).toBe(
        "nosniff",
      )
      expect(await delivered.json()).toEqual({
        deliveryId: expect.stringMatching(/^dlv-/),
        destinationId: "dst-http",
        outcome: "Delivered",
      })

      const rejected = await post(
        application.httpAddress,
        JSON.stringify(event),
      )
      expect(rejected.status).toBe(200)
      expect(await rejected.json()).toEqual({
        deliveryId: expect.stringMatching(/^dlv-/),
        destinationId: "dst-http",
        outcome: "Rejected",
      })

      const malformed = await post(
        application.httpAddress,
        "{not valid JSON",
      )
      expect(malformed.status).toBe(400)
      expect(await malformed.json()).toEqual({
        error: "invalid_event",
      })

      const unsupported = await post(
        application.httpAddress,
        JSON.stringify(event),
        { contentType: "text/plain" },
      )
      expect(unsupported.status).toBe(415)
      expect(await unsupported.json()).toEqual({
        error: "unsupported_media_type",
      })

      const oversized = await post(
        application.httpAddress,
        JSON.stringify({
          ...event,
          padding: "x".repeat(17 * 1_024),
        }),
      )
      expect(oversized.status).toBe(400)
      expect(outboundCalls).toBe(2)

      const missing = await fetch(`${application.httpAddress}/missing`)
      expect(missing.status).toBe(404)
    } finally {
      await application.shutdown()
    }
  })

  it("maps overload and interrupts service work when the request is aborted", async () => {
    const destinationId = DestinationId.make("dst-http")
    const overloadedHandler = makeHandler(() =>
      Effect.fail(
        new DeliveryOverloaded({
          admissionCapacity: 1,
          destinationId,
          limit: "GlobalAdmission",
        }),
      )
    )

    try {
      const overloaded = await overloadedHandler.handler(
        new Request("http://relay.test/deliveries", {
          method: "POST",
          headers: {
            authorization: "Bearer intake-secret",
            "content-type": "application/json",
          },
          body: JSON.stringify(event),
        }),
      )
      expect(overloaded.status).toBe(503)
      expect(await overloaded.json()).toEqual({ error: "overloaded" })
    } finally {
      await overloadedHandler.dispose()
    }

    const started = makeGate<void>()
    const interrupted = makeGate<void>()
    const controller = new AbortController()
    const interruptedHandler = makeHandler(() =>
      Effect.acquireUseRelease(
        Effect.sync(() => started.resolve(undefined)),
        () => Effect.never,
        () => Effect.sync(() => interrupted.resolve(undefined)),
      ),
    )
    const response = interruptedHandler.handler(
      new Request("http://relay.test/deliveries", {
        method: "POST",
        headers: {
          authorization: "Bearer intake-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify(event),
        signal: controller.signal,
      }),
    )

    try {
      await started.promise
      controller.abort("client disconnected")
      await interrupted.promise
      expect((await response).status).toBe(499)
    } finally {
      controller.abort("test cleanup")
      await interruptedHandler.dispose()
    }
  })
})

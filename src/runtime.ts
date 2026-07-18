import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as Http from "node:http"
import {
  ConfigProvider,
  Context,
  Effect,
  Layer,
  ManagedRuntime,
  Option,
  Stream,
  Tracer,
} from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpServer from "effect/unstable/http/HttpServer"
import { DeliveryEvents } from "./deliveryEvents.ts"
import { DeliveryOperations } from "./deliveryOperations.ts"
import { EventIntake } from "./eventIntake.ts"
import {
  DeliverySupervisor,
  type DeliveryConcurrencyMetrics,
  type DeliveryLoadMetrics,
} from "./deliverySupervisor.ts"
import {
  makeRelayHttpApplicationLayer,
  RelayPersistenceLive,
  type RelayHttpServerLayer,
  type RelayPersistenceLayer,
} from "./layers.ts"
import type {
  DeliveryResult,
  DeliveryId,
  DeliveryStatus,
  EventAcceptance,
  IngestionKey,
} from "./model.ts"
import { RelayReadiness, RelayReadinessLive } from "./readiness.ts"
import {
  WorkerIdentity,
  WorkerIdentityLive,
} from "./workerIdentity.ts"

export type RegisterShutdownHook = (
  shutdown: () => Promise<void>,
) => () => void

export interface RelayApplication {
  readonly accept: (
    ingestionKey: IngestionKey,
    candidate: unknown,
  ) => Promise<EventAcceptance>
  readonly deliver: (candidate: unknown) => Promise<DeliveryResult>
  readonly deliveryStatus: (
    id: DeliveryId,
  ) => Promise<DeliveryStatus | undefined>
  readonly deadLetters: (
    limit: number,
  ) => Promise<ReadonlyArray<DeliveryStatus>>
  readonly retryDeadLetter: (id: DeliveryId) => Promise<DeliveryStatus>
  readonly repairDeadLetter: (id: DeliveryId) => Promise<DeliveryStatus>
  readonly terminateDeadLetter: (id: DeliveryId) => Promise<DeliveryStatus>
  readonly deliveryResults: Stream.Stream<DeliveryResult>
  readonly activeDeliveryCount: () => Promise<number>
  readonly concurrencyMetrics: () => Promise<DeliveryConcurrencyMetrics>
  readonly loadMetrics: () => Promise<DeliveryLoadMetrics>
  readonly httpAddress: string
  readonly isReady: () => Promise<boolean>
  readonly shutdown: () => Promise<void>
}

const acceptConfiguredEvent = Effect.fn(
  "Relay.acceptConfiguredEvent",
)(function* (ingestionKey: IngestionKey, candidate: unknown) {
  const intake = yield* EventIntake
  return yield* intake.accept(ingestionKey, candidate)
})

const deliverConfiguredCandidate = Effect.fn(
  "Relay.deliverConfiguredCandidate",
)(function* (candidate: unknown) {
  const supervisor = yield* DeliverySupervisor
  return yield* supervisor.deliver(candidate)
})

const activeDeliveryCount = Effect.fn(
  "Relay.activeDeliveryCount",
)(function* () {
  const supervisor = yield* DeliverySupervisor
  return yield* supervisor.activeCount()
})

const concurrencyMetrics = Effect.fn(
  "Relay.concurrencyMetrics",
)(function* () {
  const supervisor = yield* DeliverySupervisor
  return yield* supervisor.concurrencyMetrics()
})

const loadMetrics = Effect.fn(
  "Relay.loadMetrics",
)(function* () {
  const supervisor = yield* DeliverySupervisor
  return yield* supervisor.loadMetrics()
})

const deliveryResults = Effect.fn(
  "Relay.deliveryResults",
)(function* () {
  const events = yield* DeliveryEvents
  return events.results
})

const deliveryStatus = Effect.fn("Relay.deliveryStatus")(
  function* (id: DeliveryId) {
    const operations = yield* DeliveryOperations
    return Option.getOrUndefined(yield* operations.status(id))
  },
)

const deadLetters = Effect.fn("Relay.deadLetters")(
  function* (limit: number) {
    const operations = yield* DeliveryOperations
    return yield* operations.listDeadLetters(limit)
  },
)

const retryDeadLetter = Effect.fn("Relay.retryDeadLetter")(
  function* (id: DeliveryId) {
    const operations = yield* DeliveryOperations
    return yield* operations.retryDeadLetter(id)
  },
)

const repairDeadLetter = Effect.fn("Relay.repairDeadLetter")(
  function* (id: DeliveryId) {
    const operations = yield* DeliveryOperations
    return yield* operations.repairDeadLetter(id)
  },
)

const terminateDeadLetter = Effect.fn("Relay.terminateDeadLetter")(
  function* (id: DeliveryId) {
    const operations = yield* DeliveryOperations
    return yield* operations.terminateDeadLetter(id)
  },
)

const httpAddress = Effect.fn("Relay.httpAddress")(function* () {
  const server = yield* HttpServer.HttpServer
  return HttpServer.formatAddress(server.address)
})

export const startRelayApplication = async (options: {
  readonly httpClientLayer?: Layer.Layer<HttpClient.HttpClient>
  readonly httpServerLayer?: RelayHttpServerLayer
  readonly persistenceLayer?: RelayPersistenceLayer
  readonly readinessLayer?: Layer.Layer<RelayReadiness>
  readonly workerIdentityLayer?: Layer.Layer<
    WorkerIdentity,
    unknown
  >
  readonly tracer?: Tracer.Tracer
  readonly configProvider: ConfigProvider.ConfigProvider
  readonly registerShutdownHook: RegisterShutdownHook
}): Promise<RelayApplication> => {
  const applicationLayer = makeRelayHttpApplicationLayer(
    options.httpClientLayer ?? NodeHttpClient.layerNodeHttp,
    options.httpServerLayer ?? NodeHttpServer.layer(
      Http.createServer,
      { host: "127.0.0.1", port: 3_000 },
    ),
    options.configProvider,
    options.persistenceLayer ?? RelayPersistenceLive,
    options.readinessLayer ?? RelayReadinessLive,
    options.workerIdentityLayer ?? WorkerIdentityLive,
  )
  const runtimeLayer = options.tracer === undefined
    ? applicationLayer
    : applicationLayer.pipe(
      Layer.provideMerge(
        Layer.succeed(Tracer.Tracer, options.tracer),
      ),
    )
  const runtime = ManagedRuntime.make(runtimeLayer)
  let removeShutdownHook = () => {}
  let shutdownPromise: Promise<void> | undefined
  let readiness: Context.Service.Shape<typeof RelayReadiness> | undefined
  const shutdown = (): Promise<void> => {
    if (shutdownPromise !== undefined) {
      return shutdownPromise
    }
    removeShutdownHook()
    const markNotReady = readiness === undefined
      ? Promise.resolve()
      : runtime.runPromise(readiness.markNotReady)
    shutdownPromise = markNotReady.then(
      () => runtime.dispose(),
      async (error) => {
        await runtime.dispose()
        throw error
      },
    )
    return shutdownPromise
  }

  try {
    const context = await runtime.context()
    const startedReadiness = Context.get(context, RelayReadiness)
    readiness = startedReadiness
    const results = await runtime.runPromise(deliveryResults())
    const address = await runtime.runPromise(httpAddress())
    removeShutdownHook = options.registerShutdownHook(shutdown)
    await runtime.runPromise(startedReadiness.markReady)

    return {
      accept: (ingestionKey, candidate) =>
        runtime.runPromise(acceptConfiguredEvent(ingestionKey, candidate)),
      activeDeliveryCount: () =>
        runtime.runPromise(activeDeliveryCount()),
      concurrencyMetrics: () =>
        runtime.runPromise(concurrencyMetrics()),
      deadLetters: (limit) => runtime.runPromise(deadLetters(limit)),
      deliveryResults: results,
      deliver: (candidate) =>
        runtime.runPromise(deliverConfiguredCandidate(candidate)),
      deliveryStatus: (id) => runtime.runPromise(deliveryStatus(id)),
      loadMetrics: () => runtime.runPromise(loadMetrics()),
      repairDeadLetter: (id) =>
        runtime.runPromise(repairDeadLetter(id)),
      httpAddress: address,
      isReady: () =>
        shutdownPromise === undefined
          ? runtime.runPromise(startedReadiness.current)
          : Promise.resolve(false),
      retryDeadLetter: (id) =>
        runtime.runPromise(retryDeadLetter(id)),
      terminateDeadLetter: (id) =>
        runtime.runPromise(terminateDeadLetter(id)),
      shutdown,
    }
  } catch (error) {
    await shutdown()
    throw error
  }
}

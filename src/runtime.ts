import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as Http from "node:http"
import {
  ConfigProvider,
  Context,
  Effect,
  Layer,
  ManagedRuntime,
  Stream,
  Tracer,
} from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpServer from "effect/unstable/http/HttpServer"
import { DeliveryEvents } from "./deliveryEvents.ts"
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
import type { DeliveryResult } from "./model.ts"
import { RelayReadiness, RelayReadinessLive } from "./readiness.ts"

export type RegisterShutdownHook = (
  shutdown: () => Promise<void>,
) => () => void

export interface RelayApplication {
  readonly deliver: (candidate: unknown) => Promise<DeliveryResult>
  readonly deliveryResults: Stream.Stream<DeliveryResult>
  readonly activeDeliveryCount: () => Promise<number>
  readonly concurrencyMetrics: () => Promise<DeliveryConcurrencyMetrics>
  readonly loadMetrics: () => Promise<DeliveryLoadMetrics>
  readonly httpAddress: string
  readonly isReady: () => Promise<boolean>
  readonly shutdown: () => Promise<void>
}

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

const httpAddress = Effect.fn("Relay.httpAddress")(function* () {
  const server = yield* HttpServer.HttpServer
  return HttpServer.formatAddress(server.address)
})

export const startRelayApplication = async (options: {
  readonly httpClientLayer?: Layer.Layer<HttpClient.HttpClient>
  readonly httpServerLayer?: RelayHttpServerLayer
  readonly persistenceLayer?: RelayPersistenceLayer
  readonly readinessLayer?: Layer.Layer<RelayReadiness>
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
      activeDeliveryCount: () =>
        runtime.runPromise(activeDeliveryCount()),
      concurrencyMetrics: () =>
        runtime.runPromise(concurrencyMetrics()),
      deliveryResults: results,
      deliver: (candidate) =>
        runtime.runPromise(deliverConfiguredCandidate(candidate)),
      loadMetrics: () => runtime.runPromise(loadMetrics()),
      httpAddress: address,
      isReady: () =>
        shutdownPromise === undefined
          ? runtime.runPromise(startedReadiness.current)
          : Promise.resolve(false),
      shutdown,
    }
  } catch (error) {
    await shutdown()
    throw error
  }
}

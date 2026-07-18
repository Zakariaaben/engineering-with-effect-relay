import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as Http from "node:http"
import {
  ConfigProvider,
  Effect,
  Layer,
  ManagedRuntime,
  Stream,
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
  type RelayHttpServerLayer,
} from "./layers.ts"
import type { DeliveryResult } from "./model.ts"

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
  readonly configProvider: ConfigProvider.ConfigProvider
  readonly registerShutdownHook: RegisterShutdownHook
}): Promise<RelayApplication> => {
  const runtime = ManagedRuntime.make(
    makeRelayHttpApplicationLayer(
      options.httpClientLayer ?? NodeHttpClient.layerNodeHttp,
      options.httpServerLayer ?? NodeHttpServer.layer(
        Http.createServer,
        { host: "127.0.0.1", port: 3_000 },
      ),
      options.configProvider,
    ),
  )
  let removeShutdownHook = () => {}
  let shutdownPromise: Promise<void> | undefined
  const shutdown = (): Promise<void> => {
    if (shutdownPromise !== undefined) {
      return shutdownPromise
    }
    removeShutdownHook()
    shutdownPromise = runtime.dispose()
    return shutdownPromise
  }

  try {
    await runtime.context()
    const results = await runtime.runPromise(deliveryResults())
    const address = await runtime.runPromise(httpAddress())
    removeShutdownHook = options.registerShutdownHook(shutdown)

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
      shutdown,
    }
  } catch (error) {
    await shutdown()
    throw error
  }
}

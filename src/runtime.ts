import {
  ConfigProvider,
  Effect,
  ManagedRuntime,
  Stream,
} from "effect"
import { DeliveryEvents } from "./deliveryEvents.ts"
import {
  DeliverySupervisor,
  type DeliveryConcurrencyMetrics,
  type DeliveryLoadMetrics,
} from "./deliverySupervisor.ts"
import type { Fetch } from "./destinationClient.ts"
import { makeRelayApplicationLayer } from "./layers.ts"
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

export const startRelayApplication = async (options: {
  readonly fetch: Fetch
  readonly configProvider: ConfigProvider.ConfigProvider
  readonly registerShutdownHook: RegisterShutdownHook
}): Promise<RelayApplication> => {
  const runtime = ManagedRuntime.make(
    makeRelayApplicationLayer(
      options.fetch,
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
      shutdown,
    }
  } catch (error) {
    await shutdown()
    throw error
  }
}

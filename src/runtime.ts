import {
  ConfigProvider,
  Effect,
  ManagedRuntime,
} from "effect"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import type { Fetch } from "./destinationClient.ts"
import { makeRelayApplicationLayer } from "./layers.ts"
import type { DeliveryOutcome } from "./model.ts"

export type RegisterShutdownHook = (
  shutdown: () => Promise<void>,
) => () => void

export interface RelayApplication {
  readonly deliver: (candidate: unknown) => Promise<DeliveryOutcome>
  readonly activeDeliveryCount: () => Promise<number>
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
    removeShutdownHook = options.registerShutdownHook(shutdown)
  } catch (error) {
    await shutdown()
    throw error
  }

  return {
    activeDeliveryCount: () =>
      runtime.runPromise(activeDeliveryCount()),
    deliver: (candidate) =>
      runtime.runPromise(deliverConfiguredCandidate(candidate)),
    shutdown,
  }
}

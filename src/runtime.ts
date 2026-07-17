import {
  ConfigProvider,
  Effect,
  ManagedRuntime,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import type { Fetch } from "./destinationClient.ts"
import { makeRelayApplicationLayer } from "./layers.ts"
import type { DeliveryOutcome } from "./model.ts"
import { deliverCandidate } from "./workflow.ts"

export type RegisterShutdownHook = (
  shutdown: () => Promise<void>,
) => () => void

export interface RelayApplication {
  readonly deliver: (candidate: unknown) => Promise<DeliveryOutcome>
  readonly shutdown: () => Promise<void>
}

const deliverConfiguredCandidate = Effect.fn(
  "Relay.deliverConfiguredCandidate",
)(function* (candidate: unknown) {
  const configuration = yield* AppConfiguration
  return yield* deliverCandidate(
    candidate,
    configuration.destination,
  )
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
    deliver: (candidate) =>
      runtime.runPromise(deliverConfiguredCandidate(candidate)),
    shutdown,
  }
}

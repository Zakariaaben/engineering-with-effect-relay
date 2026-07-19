import { describe, expect, it } from "bun:test"
import { Config, ConfigProvider, Effect } from "effect"
import { RelayPersistenceMemory } from "../src/adapters/memoryPersistence.ts"
import {
  type RegisterShutdownHook,
  startRelayApplication,
} from "../src/runtime.ts"
import {
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const validConfig = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_ID: "dst-runtime",
  RELAY_DESTINATION_URL: "https://hooks.example.test/runtime",
  RELAY_DESTINATION_AUTHORIZATION: "runtime-secret",
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
  RELAY_OPERATIONS_AUTHORIZATION: "operations-secret",
})

const makeShutdownHarness = () => {
  let hook: (() => Promise<void>) | undefined
  let registrations = 0
  let removals = 0
  const register: RegisterShutdownHook = (candidate) => {
    registrations += 1
    hook = candidate
    return () => {
      removals += 1
      hook = undefined
    }
  }

  return {
    register,
    trigger: () => {
      if (hook === undefined) {
        throw new Error("shutdown hook is not registered")
      }
      return hook()
    },
    registrationCount: () => registrations,
    removalCount: () => removals,
  }
}

describe("C03-08 runtime boundary", () => {
  it("builds before returning and exposes one Promise-facing delivery boundary", async () => {
    const requests: Array<{
      readonly endpoint: string
      readonly authorization: string | null
    }> = []
    const httpClientLayer = makeHttpClientLayer(
      (request, endpoint) =>
        Effect.sync(() => {
          requests.push({
            endpoint: endpoint.href,
            authorization: request.headers.authorization ?? null,
          })
          return makeHttpResponse(request)
        }),
    )
    const shutdown = makeShutdownHarness()

    const application = await startRelayApplication({
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      configProvider: validConfig(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: shutdown.register,
    })

    expect(shutdown.registrationCount()).toBe(1)
    expect((await application.deliver(event))._tag).toBe("Delivered")
    expect(await application.activeDeliveryCount()).toBe(0)
    expect(requests).toEqual([{
      endpoint: "https://hooks.example.test/runtime",
      authorization: "Bearer runtime-secret",
    }])

    const firstShutdown = application.shutdown()
    const repeatedShutdown = application.shutdown()
    expect(repeatedShutdown).toBe(firstShutdown)
    await firstShutdown
    expect(shutdown.removalCount()).toBe(1)
  })

  it("fails startup before registering a hook when configuration is invalid", async () => {
    const shutdown = makeShutdownHarness()

    await expect(startRelayApplication({
      httpServerLayer: makeTestHttpServerLayer(),
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
      }),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: shutdown.register,
    })).rejects.toBeInstanceOf(Config.ConfigError)

    expect(shutdown.registrationCount()).toBe(0)
  })

  it("lets the shutdown hook interrupt an in-flight Promise bridge", async () => {
    const started = makeGate<AbortSignal>()
    const httpClientLayer = makeHttpClientLayer(
      (_request, _endpoint, signal) =>
        Effect.sync(() => started.resolve(signal)).pipe(
          Effect.andThen(Effect.never),
        ),
    )
    const shutdown = makeShutdownHarness()
    const application = await startRelayApplication({
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      configProvider: validConfig(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: shutdown.register,
    })

    const delivery = application.deliver(event)
    const clientSignal = await started.promise
    expect(await application.activeDeliveryCount()).toBe(1)
    const stopped = shutdown.trigger()

    await expect(delivery).rejects.toBeDefined()
    await stopped
    expect(clientSignal.aborted).toBe(true)
    expect(shutdown.removalCount()).toBe(1)
  })
})

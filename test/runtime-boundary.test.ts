import { describe, expect, it } from "bun:test"
import { Config, ConfigProvider } from "effect"
import type { Fetch } from "../src/destinationClient.ts"
import {
  type RegisterShutdownHook,
  startRelayApplication,
} from "../src/runtime.ts"
import { event, makeGate } from "./fixtures.ts"

const validConfig = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_ID: "dst-runtime",
  RELAY_DESTINATION_URL: "https://hooks.example.test/runtime",
  RELAY_DESTINATION_AUTHORIZATION: "runtime-secret",
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
    let discardedBodies = 0
    const fetch: Fetch = async (endpoint, init) => {
      requests.push({
        endpoint: endpoint.href,
        authorization: new Headers(init.headers).get("authorization"),
      })
      return {
        status: 202,
        body: {
          cancel: async () => {
            discardedBodies += 1
          },
        },
      }
    }
    const shutdown = makeShutdownHarness()

    const application = await startRelayApplication({
      fetch,
      configProvider: validConfig(),
      registerShutdownHook: shutdown.register,
    })

    expect(shutdown.registrationCount()).toBe(1)
    expect((await application.deliver(event))._tag).toBe("Delivered")
    expect(requests).toEqual([{
      endpoint: "https://hooks.example.test/runtime",
      authorization: "Bearer runtime-secret",
    }])
    expect(discardedBodies).toBe(1)

    const firstShutdown = application.shutdown()
    const repeatedShutdown = application.shutdown()
    expect(repeatedShutdown).toBe(firstShutdown)
    await firstShutdown
    expect(shutdown.removalCount()).toBe(1)
  })

  it("fails startup before registering a hook when configuration is invalid", async () => {
    const shutdown = makeShutdownHarness()
    const fetch: Fetch = async () => ({ status: 202, body: null })

    await expect(startRelayApplication({
      fetch,
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
      }),
      registerShutdownHook: shutdown.register,
    })).rejects.toBeInstanceOf(Config.ConfigError)

    expect(shutdown.registrationCount()).toBe(0)
  })

  it("lets the shutdown hook interrupt an in-flight Promise bridge", async () => {
    const started = makeGate<AbortSignal>()
    const fetch: Fetch = async (_endpoint, init) => {
      const signal = init.signal
      if (!(signal instanceof AbortSignal)) {
        throw new Error("expected the Effect cancellation signal")
      }
      started.resolve(signal)
      return new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => reject(signal.reason),
          { once: true },
        )
      })
    }
    const shutdown = makeShutdownHarness()
    const application = await startRelayApplication({
      fetch,
      configProvider: validConfig(),
      registerShutdownHook: shutdown.register,
    })

    const delivery = application.deliver(event)
    const clientSignal = await started.promise
    const stopped = shutdown.trigger()

    await expect(delivery).rejects.toBeDefined()
    await stopped
    expect(clientSignal.aborted).toBe(true)
    expect(shutdown.removalCount()).toBe(1)
  })
})

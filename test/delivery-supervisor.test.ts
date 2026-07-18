import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect } from "effect"
import { startRelayApplication } from "../src/runtime.ts"
import {
  event,
  makeGate,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const validConfig = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_ID: "dst-supervisor",
  RELAY_DESTINATION_URL: "https://hooks.example.test/supervisor",
  RELAY_DESTINATION_AUTHORIZATION: "supervisor-secret",
  RELAY_INTAKE_AUTHORIZATION: "intake-secret",
})

describe("C04-10 delivery supervisor", () => {
  it("tracks dynamic deliveries and removes each completed fiber", async () => {
    const firstStarted = makeGate<void>()
    const secondStarted = makeGate<void>()
    const releaseFirst = makeGate<void>()
    const releaseSecond = makeGate<void>()
    let calls = 0

    const httpClientLayer = makeHttpClientLayer((request) =>
      Effect.suspend(() => {
        const call = calls++
        if (call === 0) {
          firstStarted.resolve(undefined)
          return Effect.promise(() => releaseFirst.promise).pipe(
            Effect.as(makeHttpResponse(request)),
          )
        }
        secondStarted.resolve(undefined)
        return Effect.promise(() => releaseSecond.promise).pipe(
          Effect.as(makeHttpResponse(request)),
        )
      })
    )
    const application = await startRelayApplication({
      httpClientLayer,
      httpServerLayer: makeTestHttpServerLayer(),
      configProvider: validConfig(),
      registerShutdownHook: () => () => {},
    })

    const first = application.deliver(event)
    const second = application.deliver(event)
    await Promise.all([
      firstStarted.promise,
      secondStarted.promise,
    ])

    expect(await application.activeDeliveryCount()).toBe(2)

    releaseFirst.resolve(undefined)
    await first
    expect(await application.activeDeliveryCount()).toBe(1)

    releaseSecond.resolve(undefined)
    await second
    expect(await application.activeDeliveryCount()).toBe(0)

    await application.shutdown()
  })
})

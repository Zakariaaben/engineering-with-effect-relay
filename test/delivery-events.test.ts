import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Effect,
  Fiber,
  Stream,
} from "effect"
import { DeliveryEvents } from "../src/deliveryEvents.ts"
import { DeliverySupervisor } from "../src/deliverySupervisor.ts"
import type { Fetch } from "../src/destinationClient.ts"
import { makeRelayApplicationLayer } from "../src/layers.ts"
import { event } from "./fixtures.ts"

const configuration = ConfigProvider.fromUnknown({
  RELAY_DESTINATION_ID: "dst-events",
  RELAY_DESTINATION_URL: "https://hooks.example.test/events",
  RELAY_DESTINATION_AUTHORIZATION: "events-secret",
})

describe("C06-06 delivery event boundary", () => {
  it("broadcasts completed results through Stream without exposing PubSub", async () => {
    const fetch: Fetch = async () => ({ status: 202, body: null })
    const observed = Effect.gen(function* () {
      const events = yield* DeliveryEvents
      const supervisor = yield* DeliverySupervisor
      const first = yield* Effect.forkChild(
        events.results.pipe(Stream.take(1), Stream.runCollect),
        { startImmediately: true },
      )
      const second = yield* Effect.forkChild(
        events.results.pipe(Stream.take(1), Stream.runCollect),
        { startImmediately: true },
      )

      const result = yield* supervisor.deliver(event)

      return {
        result,
        first: yield* Fiber.join(first),
        second: yield* Fiber.join(second),
        serviceKeys: Object.keys(events).sort(),
      }
    }).pipe(
      Effect.provide(
        makeRelayApplicationLayer(fetch, configuration),
      ),
    )

    const result = await Effect.runPromise(observed)

    expect(result.first).toEqual([result.result])
    expect(result.second).toEqual([result.result])
    expect(result.serviceKeys).toEqual(["publish", "results"])
  })
})

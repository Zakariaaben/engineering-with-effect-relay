import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Deferred,
  Effect,
  Fiber,
  Option,
  Ref,
  Stream,
} from "effect"
import { DeliveryEvents } from "../src/deliveryEvents.ts"
import { DeliverySupervisor } from "../src/deliverySupervisor.ts"
import type { Fetch } from "../src/destinationClient.ts"
import { makeRelayApplicationLayer } from "../src/layers.ts"
import {
  DeliveryId,
  DeliveryResult,
  DestinationId,
} from "../src/model.ts"
import { event } from "./fixtures.ts"

const configuration = (capacity = 64) => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_ID: "dst-events",
  RELAY_DESTINATION_URL: "https://hooks.example.test/events",
  RELAY_DESTINATION_AUTHORIZATION: "events-secret",
  RELAY_DELIVERY_EVENTS_CAPACITY: capacity,
})

const destinationId = DestinationId.make("dst-events")
const completed = (index: number) =>
  DeliveryResult.Delivered({
    deliveryId: DeliveryId.make(`dlv-events-${index}`),
    destinationId,
    attempts: [],
    status: 202,
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
        makeRelayApplicationLayer(fetch, configuration()),
      ),
    )

    const result = await Effect.runPromise(observed)

    expect(result.first).toEqual([result.result])
    expect(result.second).toEqual([result.result])
    expect(result.serviceKeys).toEqual(["publish", "results"])
  })
})

describe("C06-07 delivery event backpressure", () => {
  it("bounds retained results and backpressures a full feed", async () => {
    const fetch: Fetch = async () => ({ status: 202, body: null })
    const observed = Effect.gen(function* () {
      const events = yield* DeliveryEvents
      const firstReceived = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const fourthPublished = yield* Deferred.make<void>()
      const received = yield* Ref.make<ReadonlyArray<string>>([])
      const consumer = yield* events.results.pipe(
        Stream.take(4),
        Stream.runForEach((result) =>
          Ref.update(
            received,
            (current) => [...current, result.deliveryId],
          ).pipe(
            Effect.andThen(
              result.deliveryId === "dlv-events-1"
                ? Deferred.succeed(firstReceived, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirst)),
                  )
                : Effect.void,
            ),
          )
        ),
        Effect.forkChild({ startImmediately: true }),
      )

      yield* events.publish(completed(1))
      yield* Deferred.await(firstReceived)
      yield* events.publish(completed(2))
      yield* events.publish(completed(3))
      const fourthPublisher = yield* events.publish(completed(4)).pipe(
        Effect.andThen(Deferred.succeed(fourthPublished, undefined)),
        Effect.forkChild({ startImmediately: true }),
      )

      expect(yield* Deferred.poll(fourthPublished)).toEqual(Option.none())

      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Deferred.await(fourthPublished)
      yield* Fiber.join(fourthPublisher)
      yield* Fiber.join(consumer)

      return yield* Ref.get(received)
    }).pipe(
      Effect.provide(
        makeRelayApplicationLayer(fetch, configuration(2)),
      ),
    )

    expect(await Effect.runPromise(observed)).toEqual([
      "dlv-events-1",
      "dlv-events-2",
      "dlv-events-3",
      "dlv-events-4",
    ])
  })
})

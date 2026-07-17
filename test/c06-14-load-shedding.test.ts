import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
} from "effect"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryResilience,
} from "../src/configuration.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import {
  DeliverySupervisor,
  DeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import { Destination } from "../src/model.ts"
import { destination, event } from "./fixtures.ts"

const runtime = ManagedRuntime.make(
  DeliverySupervisorLive.pipe(
    Layer.provide(DeliveryEventsLive),
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(
          AppConfiguration,
          AppConfiguration.of({
            destination,
            concurrency: { global: 2, perDestination: 2 },
            flow: defaultDeliveryFlow,
            resilience: defaultDeliveryResilience,
          }),
        ),
        Layer.succeed(
          DestinationClient,
          DestinationClient.of({
            post: ({ signal }) =>
              new Promise((_resolve, reject) => {
                signal.addEventListener(
                  "abort",
                  () => reject(signal.reason),
                  { once: true },
                )
              }),
          }),
        ),
        NodeCrypto.layer,
      ),
    ),
  ),
)

describe("C06-14 overload admission", () => {
  it("reproduces an unbounded population waiting behind bounded attempts", async () => {
    const observation = await runtime.runPromise(
      Effect.gen(function* () {
        const supervisor = yield* DeliverySupervisor
        const fibers = yield* Effect.forEach(
          Array.from({ length: 8 }),
          () =>
            supervisor.deliverTo(
              event,
              Destination.make(destination),
            ).pipe(
              Effect.forkChild({ startImmediately: true }),
            ),
        )

        const result = {
          activeAttempts: (
            yield* supervisor.concurrencyMetrics()
          ).globalActive,
          ownedDeliveries: yield* supervisor.activeCount(),
        }

        yield* Effect.forEach(fibers, Fiber.interrupt, {
          discard: true,
        })
        return result
      }),
    )

    expect(observation).toEqual({
      activeAttempts: 2,
      ownedDeliveries: 8,
    })
  })
})

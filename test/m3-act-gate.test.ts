import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import {
  Context,
  Effect,
  Layer,
  ManagedRuntime,
  Redacted,
} from "effect"
import {
  AppConfiguration,
  defaultDeliveryFlow,
  defaultDeliveryResilience,
} from "../src/configuration.ts"
import {
  DeliverySupervisor,
  DeliverySupervisorLive,
} from "../src/deliverySupervisor.ts"
import { DeliveryEventsLive } from "../src/deliveryEvents.ts"
import { DestinationClient } from "../src/destinationClient.ts"
import {
  Destination,
  DestinationId,
  type Destination as DestinationType,
} from "../src/model.ts"
import { event, makeGate } from "./fixtures.ts"
import { reproduceUnboundedDeliveryPressure } from "./incidents/unboundedDeliveryPressure.ts"

const destinationA = Destination.make({
  id: DestinationId.make("dst-capacity-a"),
  endpoint: new URL("https://a.example.test/hook"),
  authorization: Redacted.make("secret-a"),
})
const destinationB = Destination.make({
  id: DestinationId.make("dst-capacity-b"),
  endpoint: new URL("https://b.example.test/hook"),
  authorization: Redacted.make("secret-b"),
})

const makeSupervisorRuntime = (
  post: Context.Service.Shape<typeof DestinationClient>["post"],
  concurrency: { readonly global: number; readonly perDestination: number },
) => {
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      AppConfiguration,
      AppConfiguration.of({
        destination: destinationA,
        concurrency,
        flow: defaultDeliveryFlow,
        resilience: defaultDeliveryResilience,
      }),
    ),
    Layer.succeed(
      DestinationClient,
      DestinationClient.of({ post }),
    ),
    NodeCrypto.layer,
  )

  return ManagedRuntime.make(
    DeliverySupervisorLive.pipe(
      Layer.provide(DeliveryEventsLive),
      Layer.provide(dependencies),
    ),
  )
}

const deliverTo = (
  runtime: ReturnType<typeof makeSupervisorRuntime>,
  destination: DestinationType,
) =>
  runtime.runPromise(
    Effect.flatMap(DeliverySupervisor, (supervisor) =>
      supervisor.deliverTo(event, destination)
    ),
  )

const metrics = (runtime: ReturnType<typeof makeSupervisorRuntime>) =>
  runtime.runPromise(
    Effect.flatMap(DeliverySupervisor, (supervisor) =>
      supervisor.concurrencyMetrics()
    ),
  )

describe("C04-12 M3 act gate", () => {
  it("reproduces the unbounded socket-pressure incident", async () => {
    expect(await reproduceUnboundedDeliveryPressure(8)).toEqual({
      active: 8,
      maximumActive: 8,
      started: 8,
    })
  })

  it("keeps active sends within global and per-destination bounds", async () => {
    const destinationSaturated = makeGate<void>()
    const saturated = makeGate<void>()
    const release = makeGate<void>()
    let activeGlobal = 0
    let maximumGlobal = 0
    const activeByDestination = new Map<string, number>()
    const maximumByDestination = new Map<string, number>()

    const runtime = makeSupervisorRuntime(
      async ({ endpoint }) => {
        const key = endpoint.hostname
        const destinationActive = (activeByDestination.get(key) ?? 0) + 1
        activeByDestination.set(key, destinationActive)
        maximumByDestination.set(
          key,
          Math.max(maximumByDestination.get(key) ?? 0, destinationActive),
        )
        activeGlobal += 1
        maximumGlobal = Math.max(maximumGlobal, activeGlobal)

        if (key === "a.example.test" && destinationActive === 2) {
          destinationSaturated.resolve(undefined)
        }
        if (activeGlobal === 3) {
          saturated.resolve(undefined)
        }

        try {
          await release.promise
          return 202
        } finally {
          activeGlobal -= 1
          activeByDestination.set(key, destinationActive - 1)
        }
      },
      { global: 3, perDestination: 2 },
    )

    try {
      await runtime.context()
      const destinationADeliveries = [
        deliverTo(runtime, destinationA),
        deliverTo(runtime, destinationA),
        deliverTo(runtime, destinationA),
      ]

      await destinationSaturated.promise
      const atDestinationCapacity = await metrics(runtime)
      expect(atDestinationCapacity.globalActive).toBe(2)
      expect(
        atDestinationCapacity.activeByDestination.get(destinationA.id),
      ).toBe(2)

      const destinationBDeliveries = [
        deliverTo(runtime, destinationB),
        deliverTo(runtime, destinationB),
        deliverTo(runtime, destinationB),
      ]

      await saturated.promise
      const atCapacity = await metrics(runtime)
      expect(atCapacity.globalActive).toBe(3)
      expect(atCapacity.activeByDestination.get(destinationA.id)).toBe(2)
      expect(atCapacity.activeByDestination.get(destinationB.id)).toBe(1)
      expect(
        Array.from(atCapacity.activeByDestination.values())
          .reduce((sum, current) => sum + current, 0),
      ).toBe(3)
      expect(
        Array.from(atCapacity.activeByDestination.values())
          .every((current) => current <= 2),
      ).toBe(true)

      release.resolve(undefined)
      await Promise.all([
        ...destinationADeliveries,
        ...destinationBDeliveries,
      ])

      expect(maximumGlobal).toBe(3)
      expect(maximumByDestination.get("a.example.test")).toBe(2)
      expect(
        Array.from(maximumByDestination.values())
          .every((maximum) => maximum <= 2),
      ).toBe(true)
      expect(await metrics(runtime)).toEqual({
        globalActive: 0,
        activeByDestination: new Map(),
      })
    } finally {
      await runtime.dispose()
    }
  })

  it("interrupts active and permit-waiting deliveries on shutdown", async () => {
    const saturated = makeGate<void>()
    let active = 0
    let maximumActive = 0
    let started = 0
    let aborted = 0

    const runtime = makeSupervisorRuntime(
      ({ signal }) => {
        active += 1
        started += 1
        maximumActive = Math.max(maximumActive, active)
        if (active === 2) {
          saturated.resolve(undefined)
        }

        return new Promise((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              active -= 1
              aborted += 1
              reject(signal.reason)
            },
            { once: true },
          )
        })
      },
      { global: 2, perDestination: 1 },
    )

    await runtime.context()
    const deliveries = [
      deliverTo(runtime, destinationA),
      deliverTo(runtime, destinationA),
      deliverTo(runtime, destinationA),
      deliverTo(runtime, destinationB),
      deliverTo(runtime, destinationB),
      deliverTo(runtime, destinationB),
    ]

    await saturated.promise
    expect((await metrics(runtime)).globalActive).toBe(2)

    await runtime.dispose()
    const exits = await Promise.allSettled(deliveries)

    expect(exits.every(({ status }) => status === "rejected")).toBe(true)
    expect(maximumActive).toBe(2)
    expect(active).toBe(0)
    expect(aborted).toBe(started)
  })
})

import { describe, expect, it } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import { makeRelayAdapterLayer } from "../src/app/layer.ts"
import { sendDelivery } from "../src/effectSender.ts"
import { DeliveryRepository } from "../src/services.ts"
import {
  delivery,
  destination,
  event,
  makeHttpClientLayer,
  makeHttpResponse,
} from "./fixtures.ts"

class SharedConnection extends Context.Service<SharedConnection, {
  readonly id: number
}>()("C03-06/SharedConnection") {}

class Reader extends Context.Service<Reader, {
  readonly connectionId: number
}>()("C03-06/Reader") {}

class Writer extends Context.Service<Writer, {
  readonly connectionId: number
}>()("C03-06/Writer") {}

const makeConsumerGraph = (isolated: boolean) => {
  let acquisitions = 0
  let releases = 0
  const ConnectionLive = Layer.effect(
    SharedConnection,
    Effect.acquireRelease(
      Effect.sync(() =>
        SharedConnection.of({ id: ++acquisitions })
      ),
      () => Effect.sync(() => {
        releases += 1
      }),
    ),
  )
  const dependency = isolated
    ? Layer.fresh(ConnectionLive)
    : ConnectionLive
  const ReaderLive = Layer.effect(
    Reader,
    Effect.map(SharedConnection, ({ id }) => Reader.of({ connectionId: id })),
  )
  const WriterLive = Layer.effect(
    Writer,
    Effect.map(SharedConnection, ({ id }) => Writer.of({ connectionId: id })),
  )
  const layer = Layer.merge(
    ReaderLive.pipe(Layer.provide(dependency)),
    WriterLive.pipe(Layer.provide(dependency)),
  )

  return {
    layer,
    acquisitionCount: () => acquisitions,
    releaseCount: () => releases,
  }
}

const inspectConsumers = Effect.gen(function* () {
  const reader = yield* Reader
  const writer = yield* Writer
  return [reader.connectionId, writer.connectionId] as const
})

describe("C03-06 Layer graph", () => {
  it("builds Relay's current adapter leaves as one graph", async () => {
    let requests = 0
    const adapters = makeRelayAdapterLayer({
      httpClient: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          requests += 1
          return makeHttpResponse(request)
        })
      ),
    })
    const program = Effect.gen(function* () {
      const repository = yield* DeliveryRepository
      yield* repository.save(delivery)
      const stored = yield* repository.findById(delivery.id)
      const outcome = yield* sendDelivery(
        delivery.id,
        event,
        destination,
      )
      return { stored, outcome }
    }).pipe(Effect.provide(adapters))

    const result = await Effect.runPromise(program)

    expect(result.stored).toEqual(Option.some(delivery))
    expect(result.outcome._tag).toBe("Delivered")
    expect(requests).toBe(1)
  })

  it("shares one named dependency across two consumer branches", async () => {
    const graph = makeConsumerGraph(false)

    const ids = await Effect.runPromise(
      inspectConsumers.pipe(Effect.provide(graph.layer)),
    )

    expect(ids).toEqual([1, 1])
    expect(graph.acquisitionCount()).toBe(1)
    expect(graph.releaseCount()).toBe(1)
  })

  it("isolates a dependency only when the graph requests fresh instances", async () => {
    const graph = makeConsumerGraph(true)

    const ids = await Effect.runPromise(
      inspectConsumers.pipe(Effect.provide(graph.layer)),
    )

    expect(ids[0]).not.toBe(ids[1])
    expect(graph.acquisitionCount()).toBe(2)
    expect(graph.releaseCount()).toBe(2)
  })
})

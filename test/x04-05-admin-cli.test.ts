import * as BunServices from "@effect/platform-bun/BunServices"
import * as NodeServices from "@effect/platform-node/NodeServices"
import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect, Layer, Schema } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import { Command } from "effect/unstable/cli"
import {
  RelayAdminClient,
  RelayAdminClientLive,
} from "../src/adminClient.ts"
import {
  AdminPresenter,
  ConfirmationRequired,
  relayAdminCommand,
  type OutputMode,
} from "../src/adminCli.ts"
import {
  Delivery,
  DeliveryId,
  DeliveryState,
  DeliveryStatus,
  DestinationId,
  EventId,
} from "../src/model.ts"

const deadLetter = DeliveryStatus.make({
  delivery: Delivery.make({
    id: DeliveryId.make("dlv-portable"),
    eventId: EventId.make("evt-portable"),
    destinationId: DestinationId.make("dst-portable"),
    state: DeliveryState.cases.DeadLettered.make({
      reason: "RetryBudgetExhausted",
    }),
  }),
  attempts: [],
})

const pending = DeliveryStatus.make({
  ...deadLetter,
  delivery: Delivery.make({
    ...deadLetter.delivery,
    state: DeliveryState.cases.Pending.make({}),
  }),
})

const contract = (
  platform: Layer.Layer<Command.Environment>,
) => {
  const calls: Array<string> = []
  const output: Array<{
    readonly _tag: "List" | "Retry"
    readonly mode: OutputMode
  }> = []
  const client = Layer.succeed(RelayAdminClient, {
    listDeadLetters: () =>
      Effect.sync(() => {
        calls.push("list")
        return [deadLetter]
      }),
    retryDeadLetter: (deliveryId) =>
      Effect.sync(() => {
        calls.push(`retry:${deliveryId}`)
        return pending
      }),
  })
  const presenter = Layer.succeed(AdminPresenter, {
    listDeadLetters: ({ mode }) =>
      Effect.sync(() => {
        output.push({ _tag: "List", mode })
      }),
    retried: ({ mode }) =>
      Effect.sync(() => {
        output.push({ _tag: "Retry", mode })
      }),
  })
  const layer = Layer.mergeAll(platform, client, presenter)
  const run = Command.runWith(relayAdminCommand, { version: "test" })

  return Effect.gen(function* () {
    yield* run(["dead-letters", "list"])
    yield* run([
      "dead-letters",
      "retry",
      "dlv-portable",
      "--yes",
      "--output",
      "json",
    ])
    return { calls, output }
  }).pipe(Effect.provide(layer))
}

describe("X04-05 Relay administration CLI", () => {
  it("adapts configured authority to the existing typed operations API", async () => {
    const requests: Array<{
      readonly authorization: string | undefined
      readonly method: string
      readonly url: string
    }> = []
    const encodeStatus = Schema.encodeSync(DeliveryStatus)
    const httpClient = Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push({
            authorization: request.headers.authorization,
            method: request.method,
            url: request.url,
          })
          const body = request.url.endsWith("/retry")
            ? encodeStatus(pending)
            : [encodeStatus(deadLetter)]
          return HttpClientResponse.fromWeb(
            request,
            new Response(JSON.stringify(body), {
              status: 200,
              headers: { "content-type": "application/json" },
            }),
          )
        })
      ),
    )
    const provider = ConfigProvider.fromUnknown({
      RELAY_ADMIN_URL: "https://relay.example.test",
      RELAY_OPERATIONS_AUTHORIZATION: "operations-secret",
    })
    const layer = RelayAdminClientLive.pipe(Layer.provide(httpClient))

    const result = await Effect.gen(function* () {
      const client = yield* RelayAdminClient
      return {
        listed: yield* client.listDeadLetters(),
        retried: yield* client.retryDeadLetter(
          DeliveryId.make("dlv-portable"),
        ),
      }
    }).pipe(
      Effect.provide(layer),
      Effect.provideService(ConfigProvider.ConfigProvider, provider),
      Effect.runPromise,
    )

    expect(result.listed).toEqual([deadLetter])
    expect(result.retried).toEqual(pending)
    expect(requests).toEqual([
      {
        authorization: "Bearer operations-secret",
        method: "GET",
        url: "https://relay.example.test/operations/dead-letters",
      },
      {
        authorization: "Bearer operations-secret",
        method: "POST",
        url:
          "https://relay.example.test/operations/dead-letters/dlv-portable/retry",
      },
    ])
  })

  it("keeps the command contract identical across Node and Bun providers", async () => {
    const node = await Effect.runPromise(contract(NodeServices.layer))
    const bun = await Effect.runPromise(contract(BunServices.layer))

    expect(node).toEqual(bun)
    expect(node).toEqual({
      calls: ["list", "retry:dlv-portable"],
      output: [
        { _tag: "List", mode: "human" },
        { _tag: "Retry", mode: "json" },
      ],
    })
  })

  it("requires explicit confirmation before mutation", async () => {
    let calls = 0
    const layer = Layer.mergeAll(
      BunServices.layer,
      Layer.succeed(RelayAdminClient, {
        listDeadLetters: () => Effect.succeed([]),
        retryDeadLetter: () =>
          Effect.sync(() => {
            calls += 1
            return pending
          }),
      }),
      Layer.succeed(AdminPresenter, {
        listDeadLetters: () => Effect.void,
        retried: () => Effect.void,
      }),
    )
    const error = await Command.runWith(
      relayAdminCommand,
      { version: "test" },
    )(["dead-letters", "retry", "dlv-portable"]).pipe(
      Effect.provide(layer),
      Effect.flip,
      Effect.runPromise,
    )

    expect(error).toBeInstanceOf(ConfirmationRequired)
    expect(calls).toBe(0)
  })
})

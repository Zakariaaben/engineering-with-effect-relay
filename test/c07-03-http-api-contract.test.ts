import { describe, expect, it } from "bun:test"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import {
  ConfigProvider,
  Effect,
  Redacted,
} from "effect"
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient"
import * as OpenApi from "effect/unstable/httpapi/OpenApi"
import {
  deliveryAuthorizationClientLayer,
  operationsAuthorizationClientLayer,
  RelayHttpApi,
  UnauthorizedProblem,
} from "../src/httpServer.ts"
import { RelayPersistenceMemory } from "../src/adapters/memoryPersistence.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const intakeToken = Redacted.make("intake-secret")
const operationsToken = Redacted.make("operations-secret")

const configuration = () => ConfigProvider.fromUnknown({
  RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
  RELAY_DESTINATION_ID: "dst-contract",
  RELAY_DESTINATION_URL: "https://hooks.example.test/contract",
  RELAY_INTAKE_AUTHORIZATION: Redacted.value(intakeToken),
  RELAY_OPERATIONS_AUTHORIZATION: "operations-secret",
})

const callGeneratedClient = <A>(
  baseUrl: string,
  token: Redacted.Redacted,
  use: (
    client: HttpApiClient.ForApi<typeof RelayHttpApi>,
  ) => Effect.Effect<A, unknown>,
) =>
  Effect.gen(function* () {
    const client = yield* HttpApiClient.make(RelayHttpApi, {
      baseUrl,
    })
    return yield* use(client)
  }).pipe(
    Effect.provide(deliveryAuthorizationClientLayer(token)),
    Effect.provide(operationsAuthorizationClientLayer(operationsToken)),
    Effect.provide(NodeHttpClient.layerNodeHttp),
    Effect.runPromise,
  )

describe("C07-03 HttpApi contract", () => {
  it("drives a typed client and enforces the declared bearer policy", async () => {
    let outboundCalls = 0
    const application = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          outboundCalls += 1
          return makeHttpResponse(request, 202)
        })
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })

    try {
      const result = await callGeneratedClient(
        application.httpAddress,
        intakeToken,
        (client) => client.deliveries.submit({ payload: event }),
      )
      expect(String(result.deliveryId)).toMatch(/^dlv-/)
      expect(String(result.destinationId)).toBe("dst-contract")
      expect(result.outcome).toBe("Delivered")

      const unauthorized = await callGeneratedClient(
        application.httpAddress,
        Redacted.make("wrong-secret"),
        (client) =>
          client.deliveries.submit({ payload: event }).pipe(
            Effect.flip,
          ),
      )
      expect(unauthorized).toBeInstanceOf(UnauthorizedProblem)
      expect(unauthorized).toEqual(new UnauthorizedProblem({
        error: "unauthorized",
      }))
      expect(outboundCalls).toBe(1)

      const missingContentType = await fetch(
        `${application.httpAddress}/deliveries`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${Redacted.value(intakeToken)}`,
          },
          body: new TextEncoder().encode(JSON.stringify(event)),
        },
      )
      expect(missingContentType.status).toBe(415)
      expect(await missingContentType.json()).toEqual({
        error: "unsupported_media_type",
      })
      expect(outboundCalls).toBe(1)
    } finally {
      await application.shutdown()
    }
  })

  it("publishes the request, response, error, and security contract", async () => {
    const spec = OpenApi.fromApi(RelayHttpApi)
    const operation = spec.paths["/deliveries"]?.post

    expect(spec.openapi).toBe("3.1.0")
    expect(spec.info).toMatchObject({
      title: "Relay intake and operations API",
      version: "1.0.0",
    })
    expect(operation?.requestBody?.content["application/json"]).toBeDefined()
    expect(Object.keys(operation?.responses ?? {}).sort()).toEqual([
      "200",
      "400",
      "401",
      "415",
      "500",
      "503",
    ])
    expect(operation?.security).toEqual([{ bearerAuth: [] }])
    expect(spec.components.securitySchemes.bearerAuth).toEqual({
      type: "http",
      scheme: "Bearer",
    })

    const application = await startRelayApplication({
      configProvider: configuration(),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.succeed(makeHttpResponse(request, 202))
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
    })

    try {
      const response = await fetch(
        `${application.httpAddress}/openapi.json`,
      )
      expect(response.status).toBe(200)
      expect(await response.json()).toEqual(spec)
    } finally {
      await application.shutdown()
    }
  })
})

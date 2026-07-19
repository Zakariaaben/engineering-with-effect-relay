import { describe, expect, it } from "bun:test"
import {
  ConfigProvider,
  Effect,
  Layer,
  Stream,
} from "effect"
import {
  LanguageModel,
  Model,
  type Response as AiResponse,
} from "effect/unstable/ai"
import { IncidentAnalysisModelLive } from "../src/incidentAnalyst.ts"
import { RelayPersistenceMemory } from "../src/adapters/memoryPersistence.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const finishPart: AiResponse.FinishPartEncoded = {
  type: "finish",
  reason: "stop",
  usage: {
    inputTokens: {
      uncached: 12,
      total: 12,
      cacheRead: undefined,
      cacheWrite: undefined,
    },
    outputTokens: {
      total: 20,
      text: 20,
      reasoning: undefined,
    },
  },
  response: undefined,
}

const makeFixtureModel = (structuredResponse: string) => {
  const requests: Array<LanguageModel.ProviderOptions> = []
  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: (options) =>
        Effect.sync(() => {
          requests.push(options)
          return [
            { type: "text", text: structuredResponse },
            finishPart,
          ]
        }),
      streamText: () => Stream.empty,
    }),
  )
  return {
    model: Model.make("relay-fixture", "incident-analyst", layer),
    requests,
  }
}

const startApplication = (
  outboundCalls: Array<string>,
  incidentAnalysisModelLayer?: Parameters<
    typeof startRelayApplication
  >[0]["incidentAnalysisModelLayer"],
) =>
  startRelayApplication({
    configProvider: ConfigProvider.fromUnknown({
      RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
      RELAY_DESTINATION_ID: "dst-analyst",
      RELAY_DESTINATION_URL: "https://hooks.example.test/analyst",
      RELAY_INTAKE_AUTHORIZATION: "analyst-intake-token",
      RELAY_OPERATIONS_AUTHORIZATION: "analyst-operations-token",
      RELAY_RECOVERY_POLL_INTERVAL: "1 hour",
      RELAY_RETRY_MAX_ATTEMPTS: 1,
    }),
    httpClientLayer: makeHttpClientLayer((request) =>
      Effect.sync(() => {
        outboundCalls.push(request.url)
        return makeHttpResponse(request, 503)
      })
    ),
    httpServerLayer: makeTestHttpServerLayer(),
    persistenceLayer: RelayPersistenceMemory,
    registerShutdownHook: () => () => {},
    ...(incidentAnalysisModelLayer === undefined
      ? {}
      : { incidentAnalysisModelLayer }),
  })

describe("X06-08 Relay delivery analyst", () => {
  it("audits a schema-checked suggestion without changing delivery state", async () => {
    const outboundCalls: Array<string> = []
    const fixture = makeFixtureModel(JSON.stringify({
      summary: "The only recorded attempt ended in a retryable provider failure.",
      nextChecks: [
        "Confirm the destination incident is contained.",
        "Review the recovery runbook before choosing an operator action.",
      ],
    }))
    const modelLayer = IncidentAnalysisModelLive.pipe(
      Layer.provide(fixture.model),
    )
    const application = await startApplication(outboundCalls, modelLayer)

    try {
      const result = await application.deliver(event)
      expect(result._tag).toBe("Exhausted")

      const before = await application.deliveryStatus(result.deliveryId)
      const report = await application.analyzeDelivery(result.deliveryId)
      const after = await application.deliveryStatus(result.deliveryId)
      const audit = await application.incidentAnalysisAuditTrail()

      expect(report).toMatchObject({
        deliveryId: result.deliveryId,
        observedState: "DeadLettered",
        observedAttemptCount: 1,
        source: "Model",
        modelFailure: null,
      })
      expect(audit).toEqual([report])
      expect(after).toEqual(before)
      expect(outboundCalls).toHaveLength(1)

      const providerPrompt = fixture.requests[0]?.prompt.content.flatMap(
        (message) => {
          if (message.role === "system") {
            return [message.content]
          }
          if (message.role !== "user") {
            return []
          }
          return message.content.flatMap((part) =>
            part.type === "text" ? [part.text] : []
          )
        },
      ).join("\n") ?? ""
      expect(providerPrompt).toContain("Retryable")
      expect(providerPrompt).not.toContain("destination-secret")
      expect(providerPrompt).not.toContain("hooks.example.test")
      expect(providerPrompt).not.toContain("amountCents")
      expect(providerPrompt).not.toContain("inv-1")
    } finally {
      await application.shutdown()
    }
  })

  it("returns and audits a deterministic fallback when no model is configured", async () => {
    const outboundCalls: Array<string> = []
    const application = await startApplication(outboundCalls)

    try {
      const result = await application.deliver(event)
      const before = await application.deliveryStatus(result.deliveryId)
      const report = await application.analyzeDelivery(result.deliveryId)
      const audit = await application.incidentAnalysisAuditTrail()

      expect(report).toMatchObject({
        deliveryId: result.deliveryId,
        observedState: "DeadLettered",
        observedAttemptCount: 1,
        source: "DeterministicFallback",
        modelFailure: "InternalProviderError",
      })
      expect(report.suggestion.nextChecks).toEqual([
        "Inspect the ordered attempt history and its last classified outcome.",
        "Follow the recovery runbook before changing durable delivery state.",
      ])
      expect(audit).toEqual([report])
      expect(await application.deliveryStatus(result.deliveryId)).toEqual(
        before,
      )
      expect(outboundCalls).toHaveLength(1)
    } finally {
      await application.shutdown()
    }
  })
})

import { describe, expect, it } from "bun:test"
import { ConfigProvider, Effect, Tracer } from "effect"
import type { Span } from "effect/Tracer"
import { RelayPersistenceMemory } from "../src/layers.ts"
import { startRelayApplication } from "../src/runtime.ts"
import {
  event,
  makeHttpClientLayer,
  makeHttpResponse,
  makeTestHttpServerLayer,
} from "./fixtures.ts"

const traceId = "11111111111111111111111111111111"
const upstreamSpanId = "2222222222222222"

const parentSpanId = (span: Span): string | undefined =>
  span.parent._tag === "Some"
    ? span.parent.value.spanId
    : undefined

describe("C07-08 trace propagation", () => {
  it("keeps one causal trace across HTTP, persistence, queue handoff, and outbound HTTP", async () => {
    const nativeTracer = await Effect.runPromise(Effect.tracer)
    const spans: Array<Span> = []
    const tracer = Tracer.make({
      span(options) {
        const span = nativeTracer.span(options)
        spans.push(span)
        return span
      },
    })
    let outboundTraceparent: string | undefined
    const application = await startRelayApplication({
      configProvider: ConfigProvider.fromUnknown({
        RELAY_DESTINATION_AUTHORIZATION: "destination-secret",
        RELAY_DESTINATION_ID: "dst-tracing",
        RELAY_DESTINATION_URL: "https://hooks.example.test/trace",
        RELAY_INTAKE_AUTHORIZATION: "intake-secret",
      }),
      httpClientLayer: makeHttpClientLayer((request) =>
        Effect.sync(() => {
          outboundTraceparent = request.headers.traceparent
          return makeHttpResponse(request, 202)
        })
      ),
      httpServerLayer: makeTestHttpServerLayer(),
      persistenceLayer: RelayPersistenceMemory,
      registerShutdownHook: () => () => {},
      tracer,
    })

    try {
      const response = await fetch(
        `${application.httpAddress}/deliveries`,
        {
          method: "POST",
          headers: {
            authorization: "Bearer intake-secret",
            "content-type": "application/json",
            traceparent:
              `00-${traceId}-${upstreamSpanId}-01`,
          },
          body: JSON.stringify(event),
        },
      )
      expect(response.status).toBe(200)

      const requestSpans = spans.filter(
        (span) => span.traceId === traceId,
      )
      const byName = (name: string) => {
        const span = requestSpans.find(
          (candidate) => candidate.name === name,
        )
        if (span === undefined) {
          throw new Error(`missing span: ${name}`)
        }
        return span
      }
      const server = byName("http.server POST")
      const deliver = byName("DeliverySupervisor.deliver")
      const deliverTo = byName("DeliverySupervisor.deliverTo")
      const persist = byName("RelayIntakeStore.savePending")
      const submitClaimed = byName("DeliverySupervisor.submitClaimed")
      const processJob = byName("DeliverySupervisor.processJob")
      const outbound = byName("http.client POST")

      expect(parentSpanId(server)).toBe(upstreamSpanId)
      expect(parentSpanId(deliver)).toBe(server.spanId)
      expect(parentSpanId(deliverTo)).toBe(deliver.spanId)
      expect(parentSpanId(persist)).toBe(deliverTo.spanId)
      expect(parentSpanId(submitClaimed)).toBe(deliverTo.spanId)
      expect(parentSpanId(processJob)).toBe(submitClaimed.spanId)
      expect(deliverTo.attributes.get("relay.event_id")).toBe(event.id)
      expect(processJob.attributes.get("relay.delivery_id")).toBe(
        deliverTo.attributes.get("relay.delivery_id"),
      )
      expect(processJob.attributes.get("relay.destination_id")).toBe(
        "dst-tracing",
      )

      const ancestorNames: Array<string> = []
      let parent = outbound.parent
      while (parent._tag === "Some" && parent.value._tag === "Span") {
        ancestorNames.push(parent.value.name)
        parent = parent.value.parent
      }
      expect(ancestorNames).toContain("DeliverySupervisor.processJob")
      expect(outboundTraceparent).toBe(
        `00-${traceId}-${outbound.spanId}-01`,
      )

      const serializedAttributes = JSON.stringify(
        requestSpans.map((span) =>
          Object.fromEntries(span.attributes)
        ),
      )
      expect(serializedAttributes).not.toContain("destination-secret")
      expect(serializedAttributes).not.toContain("intake-secret")
      expect(serializedAttributes).not.toContain(event.invoiceId)
    } finally {
      await application.shutdown()
    }
  })
})

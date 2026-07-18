import { NodeCrypto } from "@effect/platform-node"
import { describe, expect, it } from "bun:test"
import { Effect, Layer, Logger, Redacted } from "effect"
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
import { DeliveryTransportError } from "../src/errors.ts"
import { RelayIntakeStoreMemory } from "../src/layers.ts"
import {
  AmountCents,
  Destination,
  DestinationId,
  EventId,
  InvoiceId,
  RelayEvent,
} from "../src/model.ts"

describe("C07-07 structured delivery logs", () => {
  it("correlates an attempt without leaking credentials, payload, or cause", async () => {
    const authorization = "authorization-secret-9f3a"
    const privateInvoiceId = "inv-private-payload"
    const destination = Destination.make({
      id: DestinationId.make("dst-logging"),
      endpoint: new URL("https://hooks.example.test/private-path"),
      authorization: Redacted.make(authorization),
    })
    const event = RelayEvent.make({
      id: EventId.make("evt-logging"),
      invoiceId: InvoiceId.make(privateInvoiceId),
      amountCents: AmountCents.make(987_654),
    })
    const entries: Array<{
      readonly level: string
      readonly message: unknown
      readonly annotations: Record<string, unknown>
    }> = []
    const captureLogger = Logger.formatStructured.pipe(
      Logger.map((entry): void => {
        entries.push(entry)
      }),
    )
    const dependencies = Layer.mergeAll(
      Layer.succeed(
        AppConfiguration,
        AppConfiguration.of({
          destination,
          concurrency: { global: 2, perDestination: 1 },
          flow: defaultDeliveryFlow,
          resilience: {
            ...defaultDeliveryResilience,
            maxAttempts: 1,
          },
        }),
      ),
      Layer.succeed(
        DestinationClient,
        DestinationClient.of({
          post: (request) =>
            Effect.fail(new DeliveryTransportError({
              deliveryId: request.deliveryId,
              destinationId: request.destinationId,
              cause: new Error(
                `provider echoed ${authorization} for ${request.body}`,
              ),
            })),
        }),
      ),
      NodeCrypto.layer,
      RelayIntakeStoreMemory,
    )
    const supervisor = DeliverySupervisorLive.pipe(
      Layer.provide(DeliveryEventsLive),
      Layer.provide(dependencies),
    )

    const result = await Effect.runPromise(
      Effect.flatMap(DeliverySupervisor, (service) =>
        service.deliver(event)
      ).pipe(
        Effect.provide(supervisor),
        Effect.provide(Logger.layer([captureLogger])),
      ),
    )

    expect(result._tag).toBe("Exhausted")
    expect(entries.map(({ message }) => message)).toEqual([
      "delivery.intent.persisted",
      "delivery.attempt.finished",
    ])
    expect(entries[1]).toEqual(expect.objectContaining({
      level: "WARN",
      annotations: expect.objectContaining({
        "relay.event_id": event.id,
        "relay.delivery_id": result.deliveryId,
        "relay.destination_id": destination.id,
        "relay.attempt_number": 1,
        "relay.attempt_outcome": "TransportFailure",
        "relay.attempt_decision": "Exhausted",
      }),
    }))

    const serialized = JSON.stringify(entries)
    expect(serialized).not.toContain(authorization)
    expect(serialized).not.toContain(privateInvoiceId)
    expect(serialized).not.toContain("amountCents")
    expect(serialized).not.toContain(destination.endpoint.href)
    expect(serialized).not.toContain("provider echoed")
  })
})

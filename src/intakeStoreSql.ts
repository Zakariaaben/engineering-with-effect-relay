import { Effect, Layer, Schema } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import {
  IngestionConflictError,
  RelayIntakeStoreError,
} from "./errors.ts"
import { deliveryToRow } from "./deliveryRepositorySql.ts"
import {
  AmountCents,
  ConfigurationVersion,
  Delivery,
  DeliveryId,
  DeliveryRouteSnapshot,
  DeliveryState,
  DestinationId,
  EventId,
  InvoiceId,
  RelayEvent,
  RequestFingerprint,
} from "./model.ts"
import {
  IntakeDecision,
  type IntakeRecord,
  RelayIntakeStore,
} from "./services.ts"

const intakeStoreError = (
  operation: "savePending" | "accept",
  cause: unknown,
) => new RelayIntakeStoreError({ operation, cause })

const ExistingIntakeRow = Schema.Struct({
  event_id: EventId,
  event_type: Schema.Literal("invoice.created"),
  invoice_id: InvoiceId,
  amount_cents: AmountCents,
  request_fingerprint: RequestFingerprint,
  accepted_at_ms: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  delivery_id: DeliveryId,
  destination_id: DestinationId,
  destination_url: Schema.URLFromString,
  configuration_version: ConfigurationVersion,
})

const decodeExistingIntake = Schema.decodeUnknownEffect(ExistingIntakeRow)

export const makeRelayIntakeStoreSql = (
  sql: SqlClient.SqlClient,
) => {
  const accept = Effect.fn("RelayIntakeStore.accept")(
    (record: IntakeRecord) => {
      const delivery = Delivery.make({
        id: record.deliveryId,
        eventId: record.event.id,
        destinationId: record.route.destinationId,
        state: DeliveryState.cases.Pending.make({}),
      })
      const row = deliveryToRow(delivery)

      return sql.withTransaction(
        Effect.gen(function* () {
          const inserted = yield* sql<Record<string, unknown>>`
            INSERT INTO relay_events (
              event_id,
              event_type,
              invoice_id,
              amount_cents,
              ingestion_key,
              request_fingerprint,
              accepted_at_ms
            ) VALUES (
              ${record.event.id},
              ${record.event.type},
              ${record.event.invoiceId},
              ${record.event.amountCents},
              ${record.ingestionKey},
              ${record.requestFingerprint},
              ${record.acceptedAtMillis}
            )
            ON CONFLICT (ingestion_key) DO NOTHING
            RETURNING event_id
          `

          if (inserted.length > 0) {
            yield* sql`
              INSERT INTO deliveries (
                delivery_id,
                event_id,
                destination_id,
                state,
                status,
                claimed,
                destination_url,
                configuration_version
              ) VALUES (
                ${row.delivery_id},
                ${row.event_id},
                ${row.destination_id},
                ${row.state},
                ${row.status},
                TRUE,
                ${record.route.endpoint.toString()},
                ${record.route.configurationVersion}
              )
            `

            return IntakeDecision.Accepted({
              event: record.event,
              delivery,
              route: record.route,
              acceptedAtMillis: record.acceptedAtMillis,
            })
          }

          const rows = yield* sql<Record<string, unknown>>`
            SELECT
              relay_events.event_id,
              relay_events.event_type,
              relay_events.invoice_id,
              relay_events.amount_cents,
              relay_events.request_fingerprint,
              relay_events.accepted_at_ms,
              deliveries.delivery_id,
              deliveries.destination_id,
              deliveries.destination_url,
              deliveries.configuration_version
            FROM relay_events
            INNER JOIN deliveries USING (event_id)
            WHERE relay_events.ingestion_key = ${record.ingestionKey}
            ORDER BY deliveries.delivery_id
            LIMIT 1
          `
          const candidate = rows[0]
          if (candidate === undefined) {
            return yield* Effect.fail(
              intakeStoreError(
                "accept",
                new Error("idempotency key exists without an acceptance"),
              ),
            )
          }
          const existing = yield* decodeExistingIntake(candidate)
          if (existing.request_fingerprint !== record.requestFingerprint) {
            return yield* Effect.fail(new IngestionConflictError({
              ingestionKey: record.ingestionKey,
              existingEventId: existing.event_id,
            }))
          }

          const existingEvent = RelayEvent.make({
            id: existing.event_id,
            type: existing.event_type,
            invoiceId: existing.invoice_id,
            amountCents: existing.amount_cents,
          })
          const existingDelivery = Delivery.make({
            id: existing.delivery_id,
            eventId: existing.event_id,
            destinationId: existing.destination_id,
            state: DeliveryState.cases.Pending.make({}),
          })
          const existingRoute = DeliveryRouteSnapshot.make({
            destinationId: existing.destination_id,
            endpoint: existing.destination_url,
            configurationVersion: existing.configuration_version,
          })

          return IntakeDecision.Replay({
            event: existingEvent,
            delivery: existingDelivery,
            route: existingRoute,
            acceptedAtMillis: existing.accepted_at_ms,
          })
        }),
      ).pipe(
        Effect.mapError((error) =>
          error instanceof IngestionConflictError ||
            error instanceof RelayIntakeStoreError
            ? error
            : intakeStoreError("accept", error)
        ),
      )
    },
  )

  const savePending = Effect.fn("RelayIntakeStore.savePending")(
    (
      event: RelayEvent,
      deliveryId: DeliveryId,
      destinationId: DestinationId,
    ) => {
      const delivery = Delivery.make({
        id: deliveryId,
        eventId: event.id,
        destinationId,
        state: DeliveryState.cases.Pending.make({}),
      })
      const row = deliveryToRow(delivery)

      return sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO relay_events (
              event_id,
              event_type,
              invoice_id,
              amount_cents
            ) VALUES (
              ${event.id},
              ${event.type},
              ${event.invoiceId},
              ${event.amountCents}
            )
          `

          yield* sql`
            INSERT INTO deliveries (
              delivery_id,
              event_id,
              destination_id,
              state,
              status,
              claimed
            ) VALUES (
              ${row.delivery_id},
              ${row.event_id},
              ${row.destination_id},
              ${row.state},
              ${row.status},
              TRUE
            )
          `

          return delivery
        }),
      ).pipe(
        Effect.mapError((cause) => intakeStoreError("savePending", cause)),
      )
    },
  )

  return RelayIntakeStore.of({ accept, savePending })
}

export const RelayIntakeStoreSql = Layer.effect(
  RelayIntakeStore,
  Effect.map(SqlClient.SqlClient, makeRelayIntakeStoreSql),
)

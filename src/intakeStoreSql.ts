import { Effect, Layer } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { RelayIntakeStoreError } from "./errors.ts"
import { deliveryToRow } from "./deliveryRepositorySql.ts"
import {
  Delivery,
  DeliveryState,
  type DeliveryId,
  type DestinationId,
  type RelayEvent,
} from "./model.ts"
import { RelayIntakeStore } from "./services.ts"

const intakeStoreError = (cause: unknown) =>
  new RelayIntakeStoreError({ operation: "savePending", cause })

export const makeRelayIntakeStoreSql = (
  sql: SqlClient.SqlClient,
) => {
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
        Effect.mapError(intakeStoreError),
      )
    },
  )

  return RelayIntakeStore.of({ savePending })
}

export const RelayIntakeStoreSql = Layer.effect(
  RelayIntakeStore,
  Effect.map(SqlClient.SqlClient, makeRelayIntakeStoreSql),
)

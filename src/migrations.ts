import { Effect, Layer } from "effect"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"

export const createRelayTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    CREATE TABLE relay_events (
      event_id text PRIMARY KEY,
      event_type text NOT NULL
        CONSTRAINT relay_events_type_check
        CHECK (event_type = 'invoice.created'),
      invoice_id text NOT NULL,
      amount_cents bigint NOT NULL
        CONSTRAINT relay_events_amount_check
        CHECK (amount_cents > 0)
    )
  `

  yield* sql`
    CREATE TABLE deliveries (
      delivery_id text PRIMARY KEY,
      event_id text NOT NULL REFERENCES relay_events(event_id),
      destination_id text NOT NULL,
      state text NOT NULL
        CONSTRAINT deliveries_state_check
        CHECK (state IN ('Pending', 'Delivered', 'Rejected')),
      status integer,
      CONSTRAINT deliveries_state_status_check CHECK (
        (state = 'Pending' AND status IS NULL)
        OR
        (state IN ('Delivered', 'Rejected') AND status IS NOT NULL)
      )
    )
  `
})

export const addDeliveryClaims = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    ALTER TABLE deliveries
    ADD COLUMN claimed boolean NOT NULL DEFAULT FALSE
  `

  yield* sql`
    CREATE INDEX deliveries_recovery_idx
    ON deliveries (destination_id, delivery_id)
    WHERE state = 'Pending' AND claimed = FALSE
  `
})

export const RelayMigrations = Migrator.fromRecord({
  "0001_create_relay_tables": createRelayTables,
  "0002_add_delivery_claims": addDeliveryClaims,
})

export const RelayMigrationsLive = Layer.effectDiscard(
  Migrator.make({})({
    loader: RelayMigrations,
    table: "relay_migrations",
  }),
)

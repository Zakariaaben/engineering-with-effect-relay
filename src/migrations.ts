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

export const addAtomicIntake = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    ALTER TABLE relay_events
    ADD COLUMN ingestion_key text,
    ADD COLUMN request_fingerprint text,
    ADD COLUMN accepted_at_ms bigint,
    ADD CONSTRAINT relay_events_intake_complete_check CHECK (
      (
        ingestion_key IS NULL
        AND request_fingerprint IS NULL
        AND accepted_at_ms IS NULL
      )
      OR
      (
        ingestion_key IS NOT NULL
        AND request_fingerprint IS NOT NULL
        AND accepted_at_ms IS NOT NULL
        AND request_fingerprint ~ '^[0-9a-f]{64}$'
        AND accepted_at_ms >= 0
      )
    ),
    ADD CONSTRAINT relay_events_ingestion_key_unique UNIQUE (ingestion_key)
  `

  yield* sql`
    ALTER TABLE deliveries
    ADD COLUMN destination_url text,
    ADD COLUMN configuration_version integer,
    ADD CONSTRAINT deliveries_route_snapshot_check CHECK (
      (
        destination_url IS NULL
        AND configuration_version IS NULL
      )
      OR
      (
        destination_url IS NOT NULL
        AND configuration_version IS NOT NULL
        AND configuration_version > 0
      )
    )
  `
})

export const addLeasedClaims = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`DROP INDEX deliveries_recovery_idx`

  yield* sql`
    ALTER TABLE deliveries
    ADD COLUMN claim_owner text,
    ADD COLUMN claim_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at_ms bigint,
    ADD CONSTRAINT deliveries_claim_generation_check CHECK (
      claim_generation >= 0
    ),
    ADD CONSTRAINT deliveries_claim_lease_complete_check CHECK (
      (
        claim_owner IS NULL
        AND lease_expires_at_ms IS NULL
      )
      OR
      (
        claim_owner IS NOT NULL
        AND claim_owner ~ '^wrk-[a-z0-9]+(-[a-z0-9]+)*$'
        AND claim_generation > 0
        AND lease_expires_at_ms IS NOT NULL
        AND lease_expires_at_ms >= 0
      )
      AND (claim_owner IS NULL OR state = 'Pending')
    )
  `

  yield* sql`
    ALTER TABLE deliveries
    DROP COLUMN claimed
  `

  yield* sql`
    CREATE INDEX deliveries_recovery_idx
    ON deliveries (destination_id, delivery_id)
    WHERE state = 'Pending'
  `
})

export const RelayMigrations = Migrator.fromRecord({
  "0001_create_relay_tables": createRelayTables,
  "0002_add_delivery_claims": addDeliveryClaims,
  "0003_atomic_intake": addAtomicIntake,
  "0004_leased_claims": addLeasedClaims,
})

export const RelayMigrationsLive = Layer.effectDiscard(
  Migrator.make({})({
    loader: RelayMigrations,
    table: "relay_migrations",
  }),
)

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

export const addAttemptsAndDeadLetters = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_state_check,
    DROP CONSTRAINT deliveries_state_status_check,
    ADD COLUMN next_eligible_at_ms bigint NOT NULL DEFAULT (
      floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
    ),
    ADD COLUMN dead_letter_reason text,
    ADD CONSTRAINT deliveries_next_eligible_check CHECK (
      next_eligible_at_ms >= 0
    ),
    ADD CONSTRAINT deliveries_state_check CHECK (
      state IN ('Pending', 'Delivered', 'Rejected', 'DeadLettered')
    ),
    ADD CONSTRAINT deliveries_state_status_check CHECK (
      (
        state = 'Pending'
        AND status IS NULL
        AND dead_letter_reason IS NULL
      )
      OR
      (
        state IN ('Delivered', 'Rejected')
        AND status IS NOT NULL
        AND dead_letter_reason IS NULL
      )
      OR
      (
        state = 'DeadLettered'
        AND status IS NULL
        AND dead_letter_reason IN (
          'ProviderProtocolFailure',
          'RetryBudgetExhausted'
        )
      )
    )
  `

  yield* sql`
    CREATE TABLE delivery_attempts (
      delivery_id text NOT NULL REFERENCES deliveries(delivery_id),
      ordinal integer NOT NULL CHECK (ordinal > 0),
      claim_owner text NOT NULL,
      claim_generation bigint NOT NULL CHECK (claim_generation > 0),
      started_at_ms bigint NOT NULL CHECK (started_at_ms >= 0),
      completed_at_ms bigint NOT NULL,
      outcome text NOT NULL CHECK (
        outcome IN (
          'Delivered',
          'Rejected',
          'Retryable',
          'ProtocolFailure',
          'TransportFailure',
          'TimedOut'
        )
      ),
      decision text NOT NULL CHECK (
        decision IN ('Terminal', 'RetryScheduled', 'Exhausted')
      ),
      status integer,
      retry_delay_ms bigint,
      trace_id text,
      span_id text,
      PRIMARY KEY (delivery_id, ordinal),
      CONSTRAINT delivery_attempts_time_check CHECK (
        completed_at_ms >= started_at_ms
      ),
      CONSTRAINT delivery_attempts_retry_delay_check CHECK (
        (
          decision = 'RetryScheduled'
          AND retry_delay_ms IS NOT NULL
          AND retry_delay_ms >= 0
        )
        OR
        (decision <> 'RetryScheduled' AND retry_delay_ms IS NULL)
      ),
      CONSTRAINT delivery_attempts_status_check CHECK (
        (
          outcome IN (
            'Delivered',
            'Rejected',
            'Retryable',
            'ProtocolFailure'
          )
          AND status IS NOT NULL
        )
        OR
        (
          outcome IN ('TransportFailure', 'TimedOut')
          AND status IS NULL
        )
      ),
      CONSTRAINT delivery_attempts_decision_check CHECK (
        (
          decision = 'Terminal'
          AND outcome IN ('Delivered', 'Rejected', 'ProtocolFailure')
        )
        OR
        (
          decision IN ('RetryScheduled', 'Exhausted')
          AND outcome IN ('Retryable', 'TransportFailure', 'TimedOut')
        )
      ),
      CONSTRAINT delivery_attempts_trace_check CHECK (
        (trace_id IS NULL AND span_id IS NULL)
        OR
        (
          trace_id ~ '^[0-9a-f]{32}$'
          AND trace_id <> repeat('0', 32)
          AND span_id ~ '^[0-9a-f]{16}$'
          AND span_id <> repeat('0', 16)
        )
      )
    )
  `

  yield* sql`
    CREATE INDEX deliveries_dead_letter_idx
    ON deliveries (delivery_id)
    WHERE state = 'DeadLettered'
  `
})

export const addOperationalTermination = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_state_check,
    DROP CONSTRAINT deliveries_state_status_check,
    ADD CONSTRAINT deliveries_state_check CHECK (
      state IN (
        'Pending',
        'Delivered',
        'Rejected',
        'DeadLettered',
        'Terminated'
      )
    ),
    ADD CONSTRAINT deliveries_state_status_check CHECK (
      (
        state IN ('Pending', 'Terminated')
        AND status IS NULL
        AND dead_letter_reason IS NULL
      )
      OR
      (
        state IN ('Delivered', 'Rejected')
        AND status IS NOT NULL
        AND dead_letter_reason IS NULL
      )
      OR
      (
        state = 'DeadLettered'
        AND status IS NULL
        AND dead_letter_reason IN (
          'ProviderProtocolFailure',
          'RetryBudgetExhausted'
        )
      )
    )
  `
})

export const RelayMigrations = Migrator.fromRecord({
  "0001_create_relay_tables": createRelayTables,
  "0002_add_delivery_claims": addDeliveryClaims,
  "0003_atomic_intake": addAtomicIntake,
  "0004_leased_claims": addLeasedClaims,
  "0005_attempts_and_dead_letters": addAttemptsAndDeadLetters,
  "0006_operational_termination": addOperationalTermination,
})

export const PostgresMigrationsLive = Layer.effectDiscard(
  Migrator.make({})({
    loader: RelayMigrations,
    table: "relay_migrations",
  }),
)

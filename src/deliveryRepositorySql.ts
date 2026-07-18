import * as PgClient from "@effect/sql-pg/PgClient"
import { Config, Effect, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { DeliveryRepositoryError } from "./errors.ts"
import {
  Delivery,
  DeliveryId,
  DeliveryResult,
  DeliveryState,
  DestinationId,
  EventId,
  InvoiceId,
  AmountCents,
  RelayEvent,
  type Delivery as DeliveryValue,
  type DeliveryResult as DeliveryResultValue,
} from "./model.ts"
import {
  DeliveryRepository,
  type ClaimedDelivery,
} from "./services.ts"

const DeliveryRowFields = {
  delivery_id: DeliveryId,
  event_id: EventId,
  destination_id: DestinationId,
}

export const DeliveryRow = Schema.Union([
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Pending"),
    status: Schema.Null,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Delivered"),
    status: Schema.Int,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Rejected"),
    status: Schema.Int,
  }),
])
export type DeliveryRow = Schema.Schema.Type<typeof DeliveryRow>
export type DeliveryRowEncoded = Schema.Codec.Encoded<typeof DeliveryRow>

const PositiveInteger = Schema.Int.check(Schema.isGreaterThan(0))

export const DeliveryClaimRequest = Schema.Struct({
  destination_id: DestinationId,
  limit: PositiveInteger,
})
export type DeliveryClaimRequestEncoded = Schema.Codec.Encoded<
  typeof DeliveryClaimRequest
>

export const ClaimedDeliveryRow = Schema.Struct({
  delivery_id: DeliveryId,
  event_id: EventId,
  destination_id: DestinationId,
  event_type: Schema.Literal("invoice.created"),
  invoice_id: InvoiceId,
  amount_cents: AmountCents,
})
export type ClaimedDeliveryRow = Schema.Schema.Type<
  typeof ClaimedDeliveryRow
>

export const deliveryToRow = (delivery: DeliveryValue): DeliveryRow =>
  DeliveryState.match<DeliveryRow>(delivery.state, {
    Pending: () => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Pending",
      status: null,
    }),
    Delivered: ({ status }) => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Delivered",
      status,
    }),
    Rejected: ({ status }) => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Rejected",
      status,
    }),
  })

export const rowToDelivery = (row: DeliveryRow): DeliveryValue => {
  const state = row.state === "Pending"
    ? DeliveryState.cases.Pending.make({})
    : row.state === "Delivered"
    ? DeliveryState.cases.Delivered.make({ status: row.status })
    : DeliveryState.cases.Rejected.make({ status: row.status })

  return Delivery.make({
    id: row.delivery_id,
    eventId: row.event_id,
    destinationId: row.destination_id,
    state,
  })
}

export const rowToClaimedDelivery = (
  row: ClaimedDeliveryRow,
): ClaimedDelivery => ({
  delivery: Delivery.make({
    id: row.delivery_id,
    eventId: row.event_id,
    destinationId: row.destination_id,
    state: DeliveryState.cases.Pending.make({}),
  }),
  event: RelayEvent.make({
    id: row.event_id,
    type: row.event_type,
    invoiceId: row.invoice_id,
    amountCents: row.amount_cents,
  }),
})

export interface DeliveryCompletionRow {
  readonly delivery_id: string
  readonly state: "Pending" | "Delivered" | "Rejected"
  readonly status: number | null
}

export const deliveryCompletionRow = (
  deliveryId: DeliveryId,
  result: DeliveryResultValue,
): DeliveryCompletionRow =>
  DeliveryResult.$match(result, {
    Delivered: ({ status }): DeliveryCompletionRow => ({
      delivery_id: deliveryId,
      state: "Delivered",
      status,
    }),
    Rejected: ({ status }): DeliveryCompletionRow => ({
      delivery_id: deliveryId,
      state: "Rejected",
      status,
    }),
    ProtocolFailure: (): DeliveryCompletionRow => ({
      delivery_id: deliveryId,
      state: "Pending",
      status: null,
    }),
    Exhausted: (): DeliveryCompletionRow => ({
      delivery_id: deliveryId,
      state: "Pending",
      status: null,
    }),
  })

export interface DeliverySqlStatements<E = never> {
  readonly save: (row: DeliveryRowEncoded) => Effect.Effect<unknown, E>
  readonly findById: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly resetClaims: () => Effect.Effect<unknown, E>
  readonly claimPending: (
    request: DeliveryClaimRequestEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly completeClaim: (
    row: DeliveryCompletionRow,
  ) => Effect.Effect<unknown, E>
  readonly releaseClaim: (
    id: string,
  ) => Effect.Effect<unknown, E>
}

const repositoryError = (
  operation:
    | "save"
    | "findById"
    | "resetClaims"
    | "claimPending"
    | "completeClaim"
    | "releaseClaim",
  cause: unknown,
) => new DeliveryRepositoryError({ operation, cause })

export const makeDeliveryRepositorySql = <E>(
  statements: DeliverySqlStatements<E>,
) => {
  const saveRow = SqlSchema.void({
    Request: DeliveryRow,
    execute: statements.save,
  })
  const findRowById = SqlSchema.findOneOption({
    Request: DeliveryId,
    Result: DeliveryRow,
    execute: statements.findById,
  })
  const claimRows = SqlSchema.findAll({
    Request: DeliveryClaimRequest,
    Result: ClaimedDeliveryRow,
    execute: statements.claimPending,
  })

  const save = Effect.fn("DeliveryRepositorySql.save")(
    (delivery: DeliveryValue) =>
      saveRow(deliveryToRow(delivery)).pipe(
        Effect.mapError((cause) => repositoryError("save", cause)),
      ),
  )
  const findById = Effect.fn("DeliveryRepositorySql.findById")(
    (id: DeliveryId) =>
      findRowById(id).pipe(
        Effect.map(Option.map(rowToDelivery)),
        Effect.mapError((cause) => repositoryError("findById", cause)),
      ),
  )

  const resetClaims = Effect.fn("DeliveryRepositorySql.resetClaims")(
    () =>
      statements.resetClaims().pipe(
        Effect.asVoid,
        Effect.mapError((cause) => repositoryError("resetClaims", cause)),
      ),
  )
  const claimPending = Effect.fn("DeliveryRepositorySql.claimPending")(
    (destinationId: DestinationId, limit: number) =>
      claimRows({ destination_id: destinationId, limit }).pipe(
        Effect.map((rows) => rows.map(rowToClaimedDelivery)),
        Effect.mapError((cause) => repositoryError("claimPending", cause)),
      ),
  )
  const completeClaim = Effect.fn("DeliveryRepositorySql.completeClaim")(
    (deliveryId: DeliveryId, result: DeliveryResultValue) =>
      statements.completeClaim(
        deliveryCompletionRow(deliveryId, result),
      ).pipe(
        Effect.asVoid,
        Effect.mapError((cause) => repositoryError("completeClaim", cause)),
      ),
  )
  const releaseClaim = Effect.fn("DeliveryRepositorySql.releaseClaim")(
    (deliveryId: DeliveryId) =>
      statements.releaseClaim(deliveryId).pipe(
        Effect.asVoid,
        Effect.mapError((cause) => repositoryError("releaseClaim", cause)),
      ),
  )

  return DeliveryRepository.of({
    save,
    findById,
    resetClaims,
    claimPending,
    completeClaim,
    releaseClaim,
  })
}

export const DeliveryRepositorySql = Layer.effect(
  DeliveryRepository,
  Effect.map(SqlClient.SqlClient, (sql) =>
    makeDeliveryRepositorySql({
      save: (row) =>
        sql`
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
            FALSE
          )
          ON CONFLICT (delivery_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            destination_id = EXCLUDED.destination_id,
            state = EXCLUDED.state,
            status = EXCLUDED.status,
            claimed = FALSE
        `.raw,
      findById: (id) =>
        sql<Record<string, unknown>>`
          SELECT
            delivery_id,
            event_id,
            destination_id,
            state,
            status
          FROM deliveries
          WHERE delivery_id = ${id}
        `,
      resetClaims: () =>
        sql`
          UPDATE deliveries
          SET claimed = FALSE
          WHERE state = 'Pending' AND claimed = TRUE
        `,
      claimPending: ({ destination_id, limit }) =>
        sql.withTransaction(
          sql<Record<string, unknown>>`
            WITH candidates AS (
              SELECT delivery_id
              FROM deliveries
              WHERE
                state = 'Pending'
                AND claimed = FALSE
                AND destination_id = ${destination_id}
              ORDER BY delivery_id
              LIMIT ${limit}
              FOR UPDATE SKIP LOCKED
            ), claimed_deliveries AS (
              UPDATE deliveries AS delivery
              SET claimed = TRUE
              FROM candidates
              WHERE delivery.delivery_id = candidates.delivery_id
              RETURNING
                delivery.delivery_id,
                delivery.event_id,
                delivery.destination_id
            )
            SELECT
              claimed_deliveries.delivery_id,
              claimed_deliveries.event_id,
              claimed_deliveries.destination_id,
              relay_events.event_type,
              relay_events.invoice_id,
              relay_events.amount_cents
            FROM claimed_deliveries
            INNER JOIN relay_events USING (event_id)
            ORDER BY claimed_deliveries.delivery_id
          `,
        ),
      completeClaim: (row) =>
        sql`
          UPDATE deliveries
          SET
            state = ${row.state},
            status = ${row.status},
            claimed = FALSE
          WHERE
            delivery_id = ${row.delivery_id}
            AND state = 'Pending'
            AND claimed = TRUE
        `,
      releaseClaim: (id) =>
        sql`
          UPDATE deliveries
          SET claimed = FALSE
          WHERE
            delivery_id = ${id}
            AND state = 'Pending'
            AND claimed = TRUE
        `,
    })
  ),
)

export const PostgresLive = PgClient.layerConfig({
  url: Config.redacted("RELAY_DATABASE_URL"),
})

import * as PgClient from "@effect/sql-pg/PgClient"
import { Config, Effect, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { DeliveryRepositoryError } from "./errors.ts"
import {
  Delivery,
  DeliveryId,
  DeliveryState,
  DestinationId,
  EventId,
  type Delivery as DeliveryValue,
} from "./model.ts"
import { DeliveryRepository } from "./services.ts"

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

export interface DeliverySqlStatements<E = never> {
  readonly save: (row: DeliveryRowEncoded) => Effect.Effect<unknown, E>
  readonly findById: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
}

const repositoryError = (
  operation: "save" | "findById",
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

  return DeliveryRepository.of({ save, findById })
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
            status
          ) VALUES (
            ${row.delivery_id},
            ${row.event_id},
            ${row.destination_id},
            ${row.state},
            ${row.status}
          )
          ON CONFLICT (delivery_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            destination_id = EXCLUDED.destination_id,
            state = EXCLUDED.state,
            status = EXCLUDED.status
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
    })
  ),
)

export const PostgresLive = PgClient.layerConfig({
  url: Config.redacted("RELAY_DATABASE_URL"),
})

import * as PgClient from "@effect/sql-pg/PgClient"
import { Config, Effect, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import {
  ClaimLostError,
  DeliveryRepositoryError,
} from "./errors.ts"
import {
  AmountCents,
  ClaimGeneration,
  ConfigurationVersion,
  Delivery,
  DeliveryClaim,
  DeliveryId,
  DeliveryResult,
  DeliveryRouteSnapshot,
  DeliveryState,
  DestinationId,
  EventId,
  InvoiceId,
  RelayEvent,
  WorkerId,
  type Delivery as DeliveryValue,
  type DeliveryClaim as DeliveryClaimValue,
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
  owner_id: WorkerId,
  destination_id: DestinationId,
  limit: PositiveInteger,
  lease_duration_ms: PositiveInteger,
})
export type DeliveryClaimRequestEncoded = Schema.Codec.Encoded<
  typeof DeliveryClaimRequest
>

const DeliveryClaimMutation = Schema.Struct({
  delivery_id: DeliveryId,
  claim_owner: WorkerId,
  claim_generation: ClaimGeneration,
})
type DeliveryClaimMutation = Schema.Schema.Type<
  typeof DeliveryClaimMutation
>
type DeliveryClaimMutationEncoded = Schema.Codec.Encoded<
  typeof DeliveryClaimMutation
>

const DeliveryRenewRequest = Schema.Struct({
  ...DeliveryClaimMutation.fields,
  lease_duration_ms: PositiveInteger,
})
type DeliveryRenewRequestEncoded = Schema.Codec.Encoded<
  typeof DeliveryRenewRequest
>

const DeliveryClaimRow = Schema.Struct({
  claim_owner: WorkerId,
  claim_generation: ClaimGeneration,
  lease_expires_at_ms: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
  ),
})

export const ClaimedDeliveryRow = Schema.Struct({
  delivery_id: DeliveryId,
  event_id: EventId,
  destination_id: DestinationId,
  event_type: Schema.Literal("invoice.created"),
  invoice_id: InvoiceId,
  amount_cents: AmountCents,
  destination_url: Schema.NullOr(Schema.URLFromString),
  configuration_version: Schema.NullOr(ConfigurationVersion),
  ...DeliveryClaimRow.fields,
})
export type ClaimedDeliveryRow = Schema.Schema.Type<
  typeof ClaimedDeliveryRow
>

const DeliveryMutationResult = Schema.Struct({
  delivery_id: DeliveryId,
})

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

const rowToClaim = (
  row: Schema.Schema.Type<typeof DeliveryClaimRow>,
): DeliveryClaimValue => DeliveryClaim.make({
  ownerId: row.claim_owner,
  generation: row.claim_generation,
  leaseExpiresAtMillis: row.lease_expires_at_ms,
})

export const rowToClaimedDelivery = (
  row: ClaimedDeliveryRow,
): ClaimedDelivery => ({
  claim: rowToClaim(row),
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
  route:
    row.destination_url !== null && row.configuration_version !== null
      ? Option.some(DeliveryRouteSnapshot.make({
          destinationId: row.destination_id,
          endpoint: row.destination_url,
          configurationVersion: row.configuration_version,
        }))
      : Option.none(),
})

const DeliveryCompletionRequest = Schema.Struct({
  ...DeliveryClaimMutation.fields,
  state: Schema.Literals(["Pending", "Delivered", "Rejected"]),
  status: Schema.NullOr(Schema.Int),
})
export type DeliveryCompletionRow = Schema.Schema.Type<
  typeof DeliveryCompletionRequest
>
type DeliveryCompletionRowEncoded = Schema.Codec.Encoded<
  typeof DeliveryCompletionRequest
>

export const deliveryCompletionRow = (
  deliveryId: DeliveryId,
  claim: DeliveryClaimValue,
  result: DeliveryResultValue,
): DeliveryCompletionRow => {
  const base = {
    delivery_id: deliveryId,
    claim_owner: claim.ownerId,
    claim_generation: claim.generation,
  }
  return DeliveryResult.$match(result, {
    Delivered: ({ status }): DeliveryCompletionRow => ({
      ...base,
      state: "Delivered",
      status,
    }),
    Rejected: ({ status }): DeliveryCompletionRow => ({
      ...base,
      state: "Rejected",
      status,
    }),
    ProtocolFailure: (): DeliveryCompletionRow => ({
      ...base,
      state: "Pending",
      status: null,
    }),
    Exhausted: (): DeliveryCompletionRow => ({
      ...base,
      state: "Pending",
      status: null,
    }),
  })
}

export interface DeliverySqlStatements<E = never> {
  readonly save: (row: DeliveryRowEncoded) => Effect.Effect<unknown, E>
  readonly findById: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly claimPending: (
    request: DeliveryClaimRequestEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly renewClaim: (
    request: DeliveryRenewRequestEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly completeClaim: (
    row: DeliveryCompletionRowEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly releaseClaim: (
    request: DeliveryClaimMutationEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
}

const repositoryError = (
  operation:
    | "save"
    | "findById"
    | "claimPending"
    | "renewClaim"
    | "completeClaim"
    | "releaseClaim",
  cause: unknown,
) => new DeliveryRepositoryError({ operation, cause })

const claimLost = (
  operation: "renew" | "complete" | "release",
  deliveryId: DeliveryId,
  claim: DeliveryClaimValue,
) => new ClaimLostError({
  deliveryId,
  ownerId: claim.ownerId,
  generation: claim.generation,
  operation,
})

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
  const renewRow = SqlSchema.findOneOption({
    Request: DeliveryRenewRequest,
    Result: DeliveryClaimRow,
    execute: statements.renewClaim,
  })
  const completeRow = SqlSchema.findOneOption({
    Request: DeliveryCompletionRequest,
    Result: DeliveryMutationResult,
    execute: statements.completeClaim,
  })
  const releaseRow = SqlSchema.findOneOption({
    Request: DeliveryClaimMutation,
    Result: DeliveryMutationResult,
    execute: statements.releaseClaim,
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
  const claimPending = Effect.fn("DeliveryRepositorySql.claimPending")(
    (
      ownerId: WorkerId,
      destinationId: DestinationId,
      limit: number,
      leaseDurationMillis: number,
    ) =>
      claimRows({
        owner_id: ownerId,
        destination_id: destinationId,
        limit,
        lease_duration_ms: leaseDurationMillis,
      }).pipe(
        Effect.map((rows) => rows.map(rowToClaimedDelivery)),
        Effect.mapError((cause) => repositoryError("claimPending", cause)),
      ),
  )
  const renewClaim = Effect.fn("DeliveryRepositorySql.renewClaim")(
    (
      deliveryId: DeliveryId,
      claim: DeliveryClaimValue,
      leaseDurationMillis: number,
    ) =>
      renewRow({
        delivery_id: deliveryId,
        claim_owner: claim.ownerId,
        claim_generation: claim.generation,
        lease_duration_ms: leaseDurationMillis,
      }).pipe(
        Effect.mapError((cause) => repositoryError("renewClaim", cause)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(
              claimLost("renew", deliveryId, claim),
            ),
            onSome: (row) => Effect.succeed(rowToClaim(row)),
          }),
        ),
      ),
  )
  const completeClaim = Effect.fn("DeliveryRepositorySql.completeClaim")(
    (
      deliveryId: DeliveryId,
      claim: DeliveryClaimValue,
      result: DeliveryResultValue,
    ) =>
      completeRow(deliveryCompletionRow(deliveryId, claim, result)).pipe(
        Effect.mapError((cause) => repositoryError("completeClaim", cause)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(
              claimLost("complete", deliveryId, claim),
            ),
            onSome: () => Effect.void,
          }),
        ),
      ),
  )
  const releaseClaim = Effect.fn("DeliveryRepositorySql.releaseClaim")(
    (deliveryId: DeliveryId, claim: DeliveryClaimValue) =>
      releaseRow({
        delivery_id: deliveryId,
        claim_owner: claim.ownerId,
        claim_generation: claim.generation,
      }).pipe(
        Effect.mapError((cause) => repositoryError("releaseClaim", cause)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(
              claimLost("release", deliveryId, claim),
            ),
            onSome: () => Effect.void,
          }),
        ),
      ),
  )

  return DeliveryRepository.of({
    save,
    findById,
    claimPending,
    renewClaim,
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
            status
          ) VALUES (
            ${row.delivery_id},
            ${row.event_id},
            ${row.destination_id},
            ${row.state},
            ${row.status}
          )
          ON CONFLICT (delivery_id) DO NOTHING
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
      claimPending: ({
        owner_id,
        destination_id,
        limit,
        lease_duration_ms,
      }) =>
        sql.withTransaction(
          sql<Record<string, unknown>>`
            WITH claim_clock AS (
              SELECT floor(
                extract(epoch FROM clock_timestamp()) * 1000
              )::bigint AS now_ms
            ), candidates AS (
              SELECT delivery.delivery_id
              FROM deliveries AS delivery
              CROSS JOIN claim_clock
              WHERE
                delivery.state = 'Pending'
                AND delivery.destination_id = ${destination_id}
                AND (
                  delivery.claim_owner IS NULL
                  OR delivery.lease_expires_at_ms <= claim_clock.now_ms
                )
              ORDER BY delivery.delivery_id
              LIMIT ${limit}
              FOR UPDATE OF delivery SKIP LOCKED
            ), claimed_deliveries AS (
              UPDATE deliveries AS delivery
              SET
                claim_owner = ${owner_id},
                claim_generation = delivery.claim_generation + 1,
                lease_expires_at_ms =
                  claim_clock.now_ms + ${lease_duration_ms}
              FROM candidates, claim_clock
              WHERE delivery.delivery_id = candidates.delivery_id
              RETURNING
                delivery.delivery_id,
                delivery.event_id,
                delivery.destination_id,
                delivery.claim_owner,
                delivery.claim_generation,
                delivery.lease_expires_at_ms
            )
            SELECT
              claimed_deliveries.delivery_id,
              claimed_deliveries.event_id,
              claimed_deliveries.destination_id,
              relay_events.event_type,
              relay_events.invoice_id,
              relay_events.amount_cents,
              delivery.destination_url,
              delivery.configuration_version,
              claimed_deliveries.claim_owner,
              claimed_deliveries.claim_generation,
              claimed_deliveries.lease_expires_at_ms
            FROM claimed_deliveries
            INNER JOIN deliveries AS delivery USING (delivery_id)
            INNER JOIN relay_events USING (event_id)
            ORDER BY claimed_deliveries.delivery_id
          `,
        ),
      renewClaim: ({
        delivery_id,
        claim_owner,
        claim_generation,
        lease_duration_ms,
      }) =>
        sql<Record<string, unknown>>`
          WITH claim_clock AS (
            SELECT floor(
              extract(epoch FROM clock_timestamp()) * 1000
            )::bigint AS now_ms
          )
          UPDATE deliveries AS delivery
          SET lease_expires_at_ms =
            claim_clock.now_ms + ${lease_duration_ms}
          FROM claim_clock
          WHERE
            delivery.delivery_id = ${delivery_id}
            AND delivery.state = 'Pending'
            AND delivery.claim_owner = ${claim_owner}
            AND delivery.claim_generation = ${claim_generation}
            AND delivery.lease_expires_at_ms > claim_clock.now_ms
          RETURNING
            delivery.claim_owner,
            delivery.claim_generation,
            delivery.lease_expires_at_ms
        `,
      completeClaim: (row) =>
        sql<Record<string, unknown>>`
          WITH claim_clock AS (
            SELECT floor(
              extract(epoch FROM clock_timestamp()) * 1000
            )::bigint AS now_ms
          )
          UPDATE deliveries AS delivery
          SET
            state = ${row.state},
            status = ${row.status},
            claim_owner = NULL,
            lease_expires_at_ms = NULL
          FROM claim_clock
          WHERE
            delivery.delivery_id = ${row.delivery_id}
            AND delivery.state = 'Pending'
            AND delivery.claim_owner = ${row.claim_owner}
            AND delivery.claim_generation = ${row.claim_generation}
            AND delivery.lease_expires_at_ms > claim_clock.now_ms
          RETURNING delivery.delivery_id
        `,
      releaseClaim: ({
        delivery_id,
        claim_owner,
        claim_generation,
      }) =>
        sql<Record<string, unknown>>`
          UPDATE deliveries AS delivery
          SET
            claim_owner = NULL,
            lease_expires_at_ms = NULL
          WHERE
            delivery.delivery_id = ${delivery_id}
            AND delivery.state = 'Pending'
            AND delivery.claim_owner = ${claim_owner}
            AND delivery.claim_generation = ${claim_generation}
          RETURNING delivery.delivery_id
        `,
    })
  ),
)

export const PostgresLive = PgClient.layerConfig({
  url: Config.redacted("RELAY_DATABASE_URL"),
})

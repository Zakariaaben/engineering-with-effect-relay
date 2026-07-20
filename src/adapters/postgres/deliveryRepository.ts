import * as PgClient from "@effect/sql-pg/PgClient"
import { Config, Effect, Layer, Option, Schema } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import {
  ClaimLostError,
  DeadLetterDestinationMismatchError,
  DeadLetterRecoveryError,
  DeliveryRepositoryError,
} from "../../errors.ts"
import { deliveryStateFromResult } from "../../deliveryPolicy.ts"
import {
  DeadLetterReason,
  Delivery,
  DeliveryAttemptRecord,
  DeliveryClaim,
  DeliveryResult,
  DeliveryState,
  DeliveryStatus,
  type Delivery as DeliveryValue,
  type DeliveryAttemptRecord as DeliveryAttemptRecordValue,
  type DeliveryClaim as DeliveryClaimValue,
  type DeliveryResult as DeliveryResultValue,
} from "../../delivery.ts"
import { RelayEvent } from "../../command.ts"
import { DeliveryRouteSnapshot } from "../../destination.ts"
import {
  AmountCents,
  ClaimGeneration,
  ConfigurationVersion,
  DeliveryId,
  DestinationId,
  EventId,
  InvoiceId,
  WorkerId,
} from "../../identifiers.ts"
import {
  DeliveryRepository,
  type ClaimedDelivery,
} from "../../deliveryRepository.ts"

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
    dead_letter_reason: Schema.Null,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Delivered"),
    status: Schema.Int,
    dead_letter_reason: Schema.Null,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Rejected"),
    status: Schema.Int,
    dead_letter_reason: Schema.Null,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("DeadLettered"),
    status: Schema.Null,
    dead_letter_reason: DeadLetterReason,
  }),
  Schema.Struct({
    ...DeliveryRowFields,
    state: Schema.Literal("Terminated"),
    status: Schema.Null,
    dead_letter_reason: Schema.Null,
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
  claim_lag_ms: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  next_attempt_ordinal: PositiveInteger,
  ...DeliveryClaimRow.fields,
})
export type ClaimedDeliveryRow = Schema.Schema.Type<
  typeof ClaimedDeliveryRow
>

const DeliveryMutationResult = Schema.Struct({
  delivery_id: DeliveryId,
})

const DeadLetterRepairRequest = Schema.Struct({
  delivery_id: DeliveryId,
  destination_id: DestinationId,
  destination_url: Schema.String,
  configuration_version: ConfigurationVersion,
})
type DeadLetterRepairRequestEncoded = Schema.Codec.Encoded<
  typeof DeadLetterRepairRequest
>

export const deliveryToRow = (delivery: DeliveryValue): DeliveryRow =>
  DeliveryState.match<DeliveryRow>(delivery.state, {
    Pending: () => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Pending",
      status: null,
      dead_letter_reason: null,
    }),
    Delivered: ({ status }) => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Delivered",
      status,
      dead_letter_reason: null,
    }),
    Rejected: ({ status }) => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Rejected",
      status,
      dead_letter_reason: null,
    }),
    DeadLettered: ({ reason }) => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "DeadLettered",
      status: null,
      dead_letter_reason: reason,
    }),
    Terminated: () => ({
      delivery_id: delivery.id,
      event_id: delivery.eventId,
      destination_id: delivery.destinationId,
      state: "Terminated",
      status: null,
      dead_letter_reason: null,
    }),
  })

export const rowToDelivery = (row: DeliveryRow): DeliveryValue => {
  const state = row.state === "Pending"
    ? DeliveryState.cases.Pending.make({})
    : row.state === "Delivered"
    ? DeliveryState.cases.Delivered.make({ status: row.status })
    : row.state === "Rejected"
    ? DeliveryState.cases.Rejected.make({ status: row.status })
    : row.state === "DeadLettered"
    ? DeliveryState.cases.DeadLettered.make({
        reason: row.dead_letter_reason,
      })
    : DeliveryState.cases.Terminated.make({
        reason: "OperatorTerminated",
      })

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
  claimLagMillis: row.claim_lag_ms,
  nextAttemptOrdinal: row.next_attempt_ordinal,
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
  state: Schema.Literals([
    "Pending",
    "Delivered",
    "Rejected",
    "DeadLettered",
  ]),
  status: Schema.NullOr(Schema.Int),
  dead_letter_reason: Schema.NullOr(DeadLetterReason),
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
  const state = deliveryStateFromResult(result)
  return DeliveryState.match<DeliveryCompletionRow>(state, {
    Pending: () => ({
      ...base,
      state: "Pending",
      status: null,
      dead_letter_reason: null,
    }),
    Delivered: ({ status }) => ({
      ...base,
      state: "Delivered",
      status,
      dead_letter_reason: null,
    }),
    Rejected: ({ status }) => ({
      ...base,
      state: "Rejected",
      status,
      dead_letter_reason: null,
    }),
    DeadLettered: ({ reason }) => ({
      ...base,
      state: "DeadLettered",
      status: null,
      dead_letter_reason: reason,
    }),
    Terminated: () => ({
      ...base,
      state: "Pending",
      status: null,
      dead_letter_reason: null,
    }),
  })
}

const DeliveryAttemptRow = Schema.Struct({
  delivery_id: DeliveryId,
  ordinal: PositiveInteger,
  claim_owner: WorkerId,
  claim_generation: ClaimGeneration,
  started_at_ms: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  completed_at_ms: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  outcome: DeliveryAttemptRecord.fields.outcome,
  decision: DeliveryAttemptRecord.fields.decision,
  status: Schema.NullOr(Schema.Int),
  retry_delay_ms: Schema.NullOr(
    Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  trace_id: DeliveryAttemptRecord.fields.traceId,
  span_id: DeliveryAttemptRecord.fields.spanId,
})
type DeliveryAttemptRow = Schema.Schema.Type<typeof DeliveryAttemptRow>
type DeliveryAttemptRowEncoded = Schema.Codec.Encoded<
  typeof DeliveryAttemptRow
>

const attemptToRow = (
  attempt: DeliveryAttemptRecordValue,
): DeliveryAttemptRow => ({
  delivery_id: attempt.deliveryId,
  ordinal: attempt.ordinal,
  claim_owner: attempt.workerId,
  claim_generation: attempt.claimGeneration,
  started_at_ms: attempt.startedAtMillis,
  completed_at_ms: attempt.completedAtMillis,
  outcome: attempt.outcome,
  decision: attempt.decision,
  status: attempt.status,
  retry_delay_ms: attempt.retryDelayMillis,
  trace_id: attempt.traceId,
  span_id: attempt.spanId,
})

const rowToAttempt = (
  row: DeliveryAttemptRow,
): DeliveryAttemptRecordValue => DeliveryAttemptRecord.make({
  deliveryId: row.delivery_id,
  ordinal: row.ordinal,
  workerId: row.claim_owner,
  claimGeneration: row.claim_generation,
  startedAtMillis: row.started_at_ms,
  completedAtMillis: row.completed_at_ms,
  outcome: row.outcome,
  decision: row.decision,
  status: row.status,
  retryDelayMillis: row.retry_delay_ms,
  traceId: row.trace_id,
  spanId: row.span_id,
})

export interface DeliverySqlStatements<E = never> {
  readonly save: (row: DeliveryRowEncoded) => Effect.Effect<unknown, E>
  readonly findById: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly findAttempts: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly recordAttempt: (
    row: DeliveryAttemptRowEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly listDeadLetters: (
    limit: number,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly retryDeadLetter: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly repairDeadLetter: (
    request: DeadLetterRepairRequestEncoded,
  ) => Effect.Effect<ReadonlyArray<unknown>, E>
  readonly terminateDeadLetter: (
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
    | "findStatus"
    | "recordAttempt"
    | "listDeadLetters"
    | "retryDeadLetter"
    | "repairDeadLetter"
    | "terminateDeadLetter"
    | "claimPending"
    | "renewClaim"
    | "completeClaim"
    | "releaseClaim",
  cause: unknown,
) => new DeliveryRepositoryError({ operation, cause })

const claimLost = (
  operation: "renew" | "recordAttempt" | "complete" | "release",
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
  const findAttemptRows = SqlSchema.findAll({
    Request: DeliveryId,
    Result: DeliveryAttemptRow,
    execute: statements.findAttempts,
  })
  const recordAttemptRow = SqlSchema.findOneOption({
    Request: DeliveryAttemptRow,
    Result: DeliveryMutationResult,
    execute: statements.recordAttempt,
  })
  const DeadLetterListRequest = Schema.Int.check(Schema.isGreaterThan(0))
  const listDeadLetterRows = SqlSchema.findAll({
    Request: DeadLetterListRequest,
    Result: DeliveryRow,
    execute: statements.listDeadLetters,
  })
  const retryDeadLetterRow = SqlSchema.findOneOption({
    Request: DeliveryId,
    Result: DeliveryMutationResult,
    execute: statements.retryDeadLetter,
  })
  const repairDeadLetterRow = SqlSchema.findOneOption({
    Request: DeadLetterRepairRequest,
    Result: DeliveryMutationResult,
    execute: statements.repairDeadLetter,
  })
  const terminateDeadLetterRow = SqlSchema.findOneOption({
    Request: DeliveryId,
    Result: DeliveryMutationResult,
    execute: statements.terminateDeadLetter,
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
  const findStatus = Effect.fn("DeliveryRepositorySql.findStatus")(
    (id: DeliveryId) =>
      findRowById(id).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed(Option.none()),
            onSome: (row) =>
              findAttemptRows(id).pipe(
                Effect.map((attempts) => Option.some(DeliveryStatus.make({
                  delivery: rowToDelivery(row),
                  attempts: attempts.map(rowToAttempt),
                }))),
              ),
          }),
        ),
        Effect.mapError((cause) => repositoryError("findStatus", cause)),
      ),
  )
  const recordAttempt = Effect.fn("DeliveryRepositorySql.recordAttempt")(
    (attempt: DeliveryAttemptRecordValue) =>
      recordAttemptRow(attemptToRow(attempt)).pipe(
        Effect.mapError((cause) => repositoryError("recordAttempt", cause)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(
              claimLost(
                "recordAttempt",
                attempt.deliveryId,
                DeliveryClaim.make({
                  ownerId: attempt.workerId,
                  generation: attempt.claimGeneration,
                  leaseExpiresAtMillis: 0,
                }),
              ),
            ),
            onSome: () => Effect.void,
          }),
        ),
      ),
  )
  const listDeadLetters = Effect.fn(
    "DeliveryRepositorySql.listDeadLetters",
  )((limit: number) =>
    listDeadLetterRows(limit).pipe(
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          findAttemptRows(row.delivery_id).pipe(
            Effect.map((attempts) => DeliveryStatus.make({
              delivery: rowToDelivery(row),
              attempts: attempts.map(rowToAttempt),
            }))))
      ),
      Effect.mapError((cause) => repositoryError("listDeadLetters", cause)),
    ))
  const requireDeadLetter = (
    id: DeliveryId,
    operation:
      | "retryDeadLetter"
      | "repairDeadLetter"
      | "terminateDeadLetter",
  ) =>
    findRowById(id).pipe(
      Effect.mapError((cause) => repositoryError(operation, cause)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(new DeadLetterRecoveryError({
            deliveryId: id,
            reason: "NotFound",
          })),
          onSome: (row) =>
            row.state === "DeadLettered"
              ? Effect.succeed(row)
              : Effect.fail(new DeadLetterRecoveryError({
                deliveryId: id,
                reason: "NotDeadLettered",
              })),
        }),
      ),
    )
  const retryDeadLetter = Effect.fn(
    "DeliveryRepositorySql.retryDeadLetter",
  )(function* (id: DeliveryId) {
    yield* requireDeadLetter(id, "retryDeadLetter")
    const result = yield* retryDeadLetterRow(id).pipe(
      Effect.mapError((cause) =>
        repositoryError("retryDeadLetter", cause)
      ),
    )
    if (Option.isNone(result)) {
      return yield* Effect.fail(new DeadLetterRecoveryError({
        deliveryId: id,
        reason: "NotDeadLettered",
      }))
    }
  })
  const repairDeadLetter = Effect.fn(
    "DeliveryRepositorySql.repairDeadLetter",
  )(function* (id: DeliveryId, route: DeliveryRouteSnapshot) {
    const row = yield* requireDeadLetter(id, "repairDeadLetter")
    if (row.destination_id !== route.destinationId) {
      return yield* Effect.fail(new DeadLetterDestinationMismatchError({
        deliveryId: id,
        deliveryDestinationId: row.destination_id,
        repairDestinationId: route.destinationId,
      }))
    }
    const result = yield* repairDeadLetterRow({
      delivery_id: id,
      destination_id: route.destinationId,
      destination_url: route.endpoint.href,
      configuration_version: route.configurationVersion,
    }).pipe(
      Effect.mapError((cause) =>
        repositoryError("repairDeadLetter", cause)
      ),
    )
    if (Option.isNone(result)) {
      return yield* Effect.fail(new DeadLetterRecoveryError({
        deliveryId: id,
        reason: "NotDeadLettered",
      }))
    }
  })
  const terminateDeadLetter = Effect.fn(
    "DeliveryRepositorySql.terminateDeadLetter",
  )(function* (id: DeliveryId) {
    yield* requireDeadLetter(id, "terminateDeadLetter")
    const result = yield* terminateDeadLetterRow(id).pipe(
      Effect.mapError((cause) =>
        repositoryError("terminateDeadLetter", cause)
      ),
    )
    if (Option.isNone(result)) {
      return yield* Effect.fail(new DeadLetterRecoveryError({
        deliveryId: id,
        reason: "NotDeadLettered",
      }))
    }
  })
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
    findStatus,
    recordAttempt,
    listDeadLetters,
    retryDeadLetter,
    repairDeadLetter,
    terminateDeadLetter,
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
            status,
            dead_letter_reason
          ) VALUES (
            ${row.delivery_id},
            ${row.event_id},
            ${row.destination_id},
            ${row.state},
            ${row.status},
            ${row.dead_letter_reason}
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
            status,
            dead_letter_reason
          FROM deliveries
          WHERE delivery_id = ${id}
        `,
      findAttempts: (id) =>
        sql<Record<string, unknown>>`
          SELECT
            delivery_id,
            ordinal,
            claim_owner,
            claim_generation,
            started_at_ms,
            completed_at_ms,
            outcome,
            decision,
            status,
            retry_delay_ms,
            trace_id,
            span_id
          FROM delivery_attempts
          WHERE delivery_id = ${id}
          ORDER BY ordinal
        `,
      recordAttempt: (row) =>
        sql<Record<string, unknown>>`
          WITH claim_clock AS (
            SELECT floor(
              extract(epoch FROM clock_timestamp()) * 1000
            )::bigint AS now_ms
          ), owned_delivery AS (
            UPDATE deliveries AS delivery
            SET
              state = CASE
                WHEN ${row.decision} = 'Terminal'
                  AND ${row.outcome} = 'Delivered'
                  THEN 'Delivered'
                WHEN ${row.decision} = 'Terminal'
                  AND ${row.outcome} = 'Rejected'
                  THEN 'Rejected'
                WHEN ${row.decision} = 'Terminal'
                  AND ${row.outcome} = 'ProtocolFailure'
                  THEN 'DeadLettered'
                WHEN ${row.decision} = 'Exhausted'
                  THEN 'DeadLettered'
                ELSE delivery.state
              END,
              status = CASE
                WHEN ${row.decision} = 'Terminal'
                  AND ${row.outcome} IN ('Delivered', 'Rejected')
                  THEN ${row.status}
                ELSE delivery.status
              END,
              dead_letter_reason = CASE
                WHEN ${row.decision} = 'Terminal'
                  AND ${row.outcome} = 'ProtocolFailure'
                  THEN 'ProviderProtocolFailure'
                WHEN ${row.decision} = 'Exhausted'
                  THEN 'RetryBudgetExhausted'
                ELSE delivery.dead_letter_reason
              END,
              next_eligible_at_ms = CASE
                WHEN ${row.decision} = 'RetryScheduled'
                  THEN ${row.completed_at_ms} + ${row.retry_delay_ms}
                ELSE delivery.next_eligible_at_ms
              END,
              claim_owner = CASE
                WHEN ${row.decision} IN ('Terminal', 'Exhausted')
                  THEN NULL
                ELSE delivery.claim_owner
              END,
              lease_expires_at_ms = CASE
                WHEN ${row.decision} IN ('Terminal', 'Exhausted')
                  THEN NULL
                ELSE delivery.lease_expires_at_ms
              END
            FROM claim_clock
            WHERE
              delivery.delivery_id = ${row.delivery_id}
              AND delivery.state = 'Pending'
              AND delivery.claim_owner = ${row.claim_owner}
              AND delivery.claim_generation = ${row.claim_generation}
              AND delivery.lease_expires_at_ms > claim_clock.now_ms
            RETURNING delivery.delivery_id
          )
          INSERT INTO delivery_attempts (
            delivery_id,
            ordinal,
            claim_owner,
            claim_generation,
            started_at_ms,
            completed_at_ms,
            outcome,
            decision,
            status,
            retry_delay_ms,
            trace_id,
            span_id
          )
          SELECT
            ${row.delivery_id},
            ${row.ordinal},
            ${row.claim_owner},
            ${row.claim_generation},
            ${row.started_at_ms},
            ${row.completed_at_ms},
            ${row.outcome},
            ${row.decision},
            ${row.status},
            ${row.retry_delay_ms},
            ${row.trace_id},
            ${row.span_id}
          FROM owned_delivery
          RETURNING delivery_id
        `,
      listDeadLetters: (limit) =>
        sql<Record<string, unknown>>`
          SELECT
            delivery_id,
            event_id,
            destination_id,
            state,
            status,
            dead_letter_reason
          FROM deliveries
          WHERE state = 'DeadLettered'
          ORDER BY delivery_id
          LIMIT ${limit}
        `,
      retryDeadLetter: (id) =>
        sql<Record<string, unknown>>`
          WITH claim_clock AS (
            SELECT floor(
              extract(epoch FROM clock_timestamp()) * 1000
            )::bigint AS now_ms
          )
          UPDATE deliveries AS delivery
          SET
            state = 'Pending',
            status = NULL,
            dead_letter_reason = NULL,
            claim_owner = NULL,
            lease_expires_at_ms = NULL,
            next_eligible_at_ms = claim_clock.now_ms
          FROM claim_clock
          WHERE
            delivery.delivery_id = ${id}
            AND delivery.state = 'DeadLettered'
          RETURNING delivery.delivery_id
        `,
      repairDeadLetter: ({
        delivery_id,
        destination_id,
        destination_url,
        configuration_version,
      }) =>
        sql<Record<string, unknown>>`
          WITH claim_clock AS (
            SELECT floor(
              extract(epoch FROM clock_timestamp()) * 1000
            )::bigint AS now_ms
          )
          UPDATE deliveries AS delivery
          SET
            state = 'Pending',
            status = NULL,
            dead_letter_reason = NULL,
            destination_url = ${destination_url},
            configuration_version = ${configuration_version},
            claim_owner = NULL,
            lease_expires_at_ms = NULL,
            next_eligible_at_ms = claim_clock.now_ms
          FROM claim_clock
          WHERE
            delivery.delivery_id = ${delivery_id}
            AND delivery.destination_id = ${destination_id}
            AND delivery.state = 'DeadLettered'
          RETURNING delivery.delivery_id
        `,
      terminateDeadLetter: (id) =>
        sql<Record<string, unknown>>`
          UPDATE deliveries AS delivery
          SET
            state = 'Terminated',
            status = NULL,
            dead_letter_reason = NULL,
            claim_owner = NULL,
            lease_expires_at_ms = NULL
          WHERE
            delivery.delivery_id = ${id}
            AND delivery.state = 'DeadLettered'
          RETURNING delivery.delivery_id
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
                AND delivery.next_eligible_at_ms <= claim_clock.now_ms
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
                delivery.lease_expires_at_ms,
                delivery.next_eligible_at_ms,
                claim_clock.now_ms AS claimed_at_ms
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
              claimed_deliveries.claimed_at_ms -
                claimed_deliveries.next_eligible_at_ms AS claim_lag_ms,
              COALESCE(attempts.next_attempt_ordinal, 1)
                AS next_attempt_ordinal,
              claimed_deliveries.claim_owner,
              claimed_deliveries.claim_generation,
              claimed_deliveries.lease_expires_at_ms
            FROM claimed_deliveries
            INNER JOIN deliveries AS delivery USING (delivery_id)
            INNER JOIN relay_events USING (event_id)
            LEFT JOIN LATERAL (
              SELECT MAX(ordinal) + 1 AS next_attempt_ordinal
              FROM delivery_attempts
              WHERE delivery_id = claimed_deliveries.delivery_id
            ) AS attempts ON TRUE
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
            dead_letter_reason = ${row.dead_letter_reason},
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

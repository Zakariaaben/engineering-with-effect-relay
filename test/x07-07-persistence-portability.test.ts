import { describe, expect, it } from "bun:test"
import * as PgliteClient from "@effect/sql-pglite/PgliteClient"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Effect, Layer, Option } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import {
  DeliveryRepositorySql,
} from "../src/adapters/postgres/deliveryRepository.ts"
import {
  Delivery,
  DeliveryId,
  DeliveryState,
  EventId,
} from "../src/model.ts"
import { DeliveryRepository } from "../src/services.ts"
import { delivery, event } from "./fixtures.ts"

const createPortableSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient

  yield* sql`
    CREATE TABLE relay_events (
      event_id text PRIMARY KEY,
      event_type text NOT NULL,
      invoice_id text NOT NULL,
      amount_cents bigint NOT NULL
    )
  `

  yield* sql`
    CREATE TABLE deliveries (
      delivery_id text PRIMARY KEY,
      event_id text NOT NULL REFERENCES relay_events(event_id),
      destination_id text NOT NULL,
      state text NOT NULL,
      status integer,
      dead_letter_reason text
    )
  `
})

const insertEvent = (id: EventId = event.id) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`
      INSERT INTO relay_events (
        event_id,
        event_type,
        invoice_id,
        amount_cents
      ) VALUES (
        ${id},
        ${event.type},
        ${event.invoiceId},
        ${event.amountCents}
      )
    `
  })

const roundTrip = Effect.gen(function* () {
  yield* createPortableSchema
  yield* insertEvent()

  const repository = yield* DeliveryRepository
  yield* repository.save(delivery)

  const stored = yield* repository.findById(delivery.id)
  expect(stored).toEqual(Option.some(delivery))
})

const duplicateDoesNotRewriteState = Effect.gen(function* () {
  yield* createPortableSchema
  yield* insertEvent()

  const repository = yield* DeliveryRepository
  yield* repository.save(delivery)
  yield* repository.save(Delivery.make({
    ...delivery,
    state: DeliveryState.cases.Delivered.make({ status: 202 }),
  }))

  const stored = yield* repository.findById(delivery.id)
  expect(Option.getOrThrow(stored).state._tag).toBe("Pending")
})

const transactionRollsBack = Effect.gen(function* () {
  yield* createPortableSchema

  const sql = yield* SqlClient.SqlClient
  const repository = yield* DeliveryRepository
  const rolledBackEventId = EventId.make("evt-rollback")
  const rolledBackDelivery = Delivery.make({
    ...delivery,
    id: DeliveryId.make("dlv-rollback"),
    eventId: rolledBackEventId,
  })

  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* insertEvent(rolledBackEventId)
      yield* repository.save(rolledBackDelivery)
      return yield* Effect.fail("injected failure")
    }),
  ).pipe(Effect.catch(() => Effect.void))

  const stored = yield* repository.findById(rolledBackDelivery.id)
  expect(Option.isNone(stored)).toBe(true)
})

const makePgliteLayer = () => {
  const driver = PgliteClient.layer()
  return Layer.merge(
    driver,
    DeliveryRepositorySql.pipe(Layer.provide(driver)),
  )
}

const makeBunSqliteLayer = () => {
  const driver = SqliteClient.layer({ filename: ":memory:" })
  return Layer.merge(
    driver,
    DeliveryRepositorySql.pipe(Layer.provide(driver)),
  )
}

const runPglite = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DeliveryRepository | SqlClient.SqlClient
  >,
) =>
  Effect.runPromise(
    program.pipe(Effect.provide(makePgliteLayer()), Effect.scoped),
  )

const runBunSqlite = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DeliveryRepository | SqlClient.SqlClient
  >,
) =>
  Effect.runPromise(
    program.pipe(Effect.provide(makeBunSqliteLayer()), Effect.scoped),
  )

const persistenceContract = (
  driver: string,
  run: typeof runPglite,
) => {
  describe(driver, () => {
    it("round-trips a decoded delivery", () => run(roundTrip))

    it("does not let a duplicate save rewrite state", () =>
      run(duplicateDoesNotRewriteState))

    it("rolls back the event and delivery together", () =>
      run(transactionRollsBack))
  })
}

describe("X07-07 portable persistence contract", () => {
  persistenceContract("PGlite (PostgreSQL semantics)", runPglite)
  persistenceContract("Bun SQLite", runBunSqlite)
})

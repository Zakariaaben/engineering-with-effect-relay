import * as PgClient from "@effect/sql-pg/PgClient"
import { describe, expect, it } from "bun:test"
import { Effect, Stream } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import { makeRelayIntakeStoreSql } from "../src/intakeStoreSql.ts"
import { DeliveryId } from "../src/model.ts"
import { destination, event } from "./fixtures.ts"

interface DatabaseState {
  readonly events: Set<string>
  readonly deliveries: Set<string>
}

const copyState = (state: DatabaseState): DatabaseState => ({
  events: new Set(state.events),
  deliveries: new Set(state.deliveries),
})

const makeDatabase = (failDeliveryInsert: boolean) =>
  Effect.gen(function* () {
    let committed: DatabaseState = {
      events: new Set(),
      deliveries: new Set(),
    }
    let staged: DatabaseState | undefined
    const commands: Array<string> = []

    const execute = (
      statement: string,
      params: ReadonlyArray<unknown>,
    ): Effect.Effect<ReadonlyArray<Record<string, unknown>>, SqlError.SqlError> =>
      Effect.suspend(() => {
        const normalized = statement.replace(/\s+/g, " ").trim()
        commands.push(normalized)

        if (normalized === "BEGIN") {
          staged = copyState(committed)
          return Effect.succeed([])
        }
        if (normalized === "COMMIT") {
          if (staged !== undefined) {
            committed = staged
            staged = undefined
          }
          return Effect.succeed([])
        }
        if (normalized === "ROLLBACK") {
          staged = undefined
          return Effect.succeed([])
        }

        const target = staged ?? committed
        if (normalized.startsWith("INSERT INTO relay_events")) {
          target.events.add(String(params[0]))
          return Effect.succeed([])
        }
        if (normalized.startsWith("INSERT INTO deliveries")) {
          if (failDeliveryInsert) {
            return Effect.fail(
              new SqlError.SqlError({
                reason: new SqlError.UnknownError({
                  cause: new Error("forced delivery insert failure"),
                  message: "forced delivery insert failure",
                  operation: "insertDelivery",
                }),
              }),
            )
          }
          target.deliveries.add(String(params[0]))
          return Effect.succeed([])
        }

        return Effect.succeed([])
      })

    const connection = {
      execute: (statement, params) => execute(statement, params),
      executeRaw: (statement, params) => execute(statement, params),
      executeStream: () => Stream.empty,
      executeValues: () => Effect.succeed([]),
      executeValuesUnprepared: () => Effect.succeed([]),
      executeUnprepared: (statement, params) => execute(statement, params),
    } satisfies SqlConnection.Connection

    const sql = yield* SqlClient.make({
      acquirer: Effect.succeed(connection),
      compiler: PgClient.makeCompiler(),
      spanAttributes: [],
      transactionAcquirer: Effect.succeed(connection),
    })

    return {
      sql,
      commands,
      state: () => committed,
    }
  }).pipe(Effect.provide(Reactivity.layer))

describe("C07-05 intake transaction", () => {
  it("commits the event and pending delivery together", async () => {
    const database = await Effect.runPromise(makeDatabase(false))
    const store = makeRelayIntakeStoreSql(database.sql)
    const deliveryId = DeliveryId.make("dlv-transaction-commit")

    const saved = await Effect.runPromise(
      store.savePending(event, deliveryId, destination.id),
    )

    expect(saved.id).toBe(deliveryId)
    expect(database.state()).toEqual({
      events: new Set([String(event.id)]),
      deliveries: new Set([String(deliveryId)]),
    })
    expect(database.commands.at(-1)).toBe("COMMIT")
  })

  it("rolls back the event when the delivery insert fails", async () => {
    const database = await Effect.runPromise(makeDatabase(true))
    const store = makeRelayIntakeStoreSql(database.sql)
    const deliveryId = DeliveryId.make("dlv-transaction-rollback")

    const error = await Effect.runPromise(
      Effect.flip(store.savePending(event, deliveryId, destination.id)),
    )

    expect(error._tag).toBe("RelayIntakeStoreError")
    expect(SqlError.isSqlError(error.cause)).toBe(true)
    expect(database.state()).toEqual({
      events: new Set(),
      deliveries: new Set(),
    })
    expect(database.commands.some((command) =>
      command.startsWith("INSERT INTO relay_events")
    )).toBe(true)
    expect(database.commands.at(-1)).toBe("ROLLBACK")
  })
})

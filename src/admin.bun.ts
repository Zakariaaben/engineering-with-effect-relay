import * as BunHttpClient from "@effect/platform-bun/BunHttpClient"
import * as BunRuntime from "@effect/platform-bun/BunRuntime"
import * as BunServices from "@effect/platform-bun/BunServices"
import { Effect, Layer } from "effect"
import { Command } from "effect/unstable/cli"
import {
  RelayAdminClientLive,
} from "./adminClient.ts"
import {
  AdminPresenterLive,
  relayAdminCommand,
} from "./adminCli.ts"

const AdminClientLive = RelayAdminClientLive.pipe(
  Layer.provide(BunHttpClient.layer),
)

const MainLayer = Layer.mergeAll(
  BunServices.layer,
  AdminClientLive,
  AdminPresenterLive,
)

Command.run(relayAdminCommand, { version: "1.0.0" }).pipe(
  Effect.provide(MainLayer),
  BunRuntime.runMain,
)

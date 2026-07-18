import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as NodeServices from "@effect/platform-node/NodeServices"
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
  Layer.provide(NodeHttpClient.layerNodeHttp),
)

const MainLayer = Layer.mergeAll(
  NodeServices.layer,
  AdminClientLive,
  AdminPresenterLive,
)

Command.run(relayAdminCommand, { version: "1.0.0" }).pipe(
  Effect.provide(MainLayer),
  NodeRuntime.runMain,
)

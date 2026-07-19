import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Config, ConfigProvider, Layer } from "effect"
import { createServer } from "node:http"
import {
  makeRelayHttpApplicationLayer,
  RelayPersistenceLive,
} from "./app/layer.ts"

const HttpServerLive = NodeHttpServer.layerConfig(
  createServer,
  Config.all({
    host: Config.string("RELAY_HOST").pipe(
      Config.withDefault("127.0.0.1"),
    ),
    port: Config.number("RELAY_PORT").pipe(Config.withDefault(3_000)),
  }),
)

const RelayLive = makeRelayHttpApplicationLayer({
  configProvider: ConfigProvider.fromEnv(),
  httpClient: NodeHttpClient.layerNodeHttp,
  httpServer: HttpServerLive,
  persistence: RelayPersistenceLive,
})

Layer.launch(RelayLive).pipe(NodeRuntime.runMain)

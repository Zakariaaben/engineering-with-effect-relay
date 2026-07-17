import { Config, Context, Layer } from "effect"
import {
  Destination,
  DestinationId,
} from "./model.ts"

export class AppConfiguration extends Context.Service<AppConfiguration, {
  readonly destination: Destination
}>()("Relay/AppConfiguration") {}

const destination = Config.all({
  id: Config.schema(
    DestinationId,
    "RELAY_DESTINATION_ID",
  ).pipe(Config.withDefault(DestinationId.make("dst-primary"))),
  endpoint: Config.url("RELAY_DESTINATION_URL"),
  authorization: Config.redacted("RELAY_DESTINATION_AUTHORIZATION"),
})

export const AppConfigurationLive = Layer.effect(
  AppConfiguration,
  destination.pipe(
    Config.map((destination) =>
      AppConfiguration.of({
        destination: Destination.make(destination),
      })
    ),
  ),
)

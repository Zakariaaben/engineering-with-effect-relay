import { Config, Context, Layer, Schema } from "effect"
import {
  Destination,
  DestinationId,
} from "./model.ts"

export interface DeliveryConcurrency {
  readonly global: number
  readonly perDestination: number
}

export class AppConfiguration extends Context.Service<AppConfiguration, {
  readonly destination: Destination
  readonly concurrency: DeliveryConcurrency
}>()("Relay/AppConfiguration") {}

const destination = Config.all({
  id: Config.schema(
    DestinationId,
    "RELAY_DESTINATION_ID",
  ).pipe(Config.withDefault(DestinationId.make("dst-primary"))),
  endpoint: Config.url("RELAY_DESTINATION_URL"),
  authorization: Config.redacted("RELAY_DESTINATION_AUTHORIZATION"),
})

const ConcurrencyLimit = Schema.Int.check(Schema.isGreaterThan(0))

const concurrency = Config.all({
  global: Config.schema(
    ConcurrencyLimit,
    "RELAY_GLOBAL_CONCURRENCY",
  ).pipe(Config.withDefault(64)),
  perDestination: Config.schema(
    ConcurrencyLimit,
    "RELAY_DESTINATION_CONCURRENCY",
  ).pipe(Config.withDefault(4)),
})

export const AppConfigurationLive = Layer.effect(
  AppConfiguration,
  Config.all({ destination, concurrency }).pipe(
    Config.map(({ concurrency, destination }) =>
      AppConfiguration.of({
        concurrency,
        destination: Destination.make(destination),
      })
    ),
  ),
)

import {
  Config,
  Context,
  Duration,
  Layer,
  Schema,
} from "effect"
import {
  Destination,
  DestinationId,
} from "./model.ts"

export interface DeliveryConcurrency {
  readonly global: number
  readonly perDestination: number
}

export interface DeliveryResilience {
  readonly attemptTimeout: Duration.Duration
  readonly maxAttempts: number
  readonly maxElapsed: Duration.Duration
  readonly baseDelay: Duration.Duration
  readonly maxDelay: Duration.Duration
}

export interface DeliveryFlow {
  readonly deliveryRequestsCapacity: number
  readonly deliveryEventsCapacity: number
}

export const defaultDeliveryResilience: DeliveryResilience = {
  attemptTimeout: Duration.seconds(30),
  maxAttempts: 8,
  maxElapsed: Duration.hours(24),
  baseDelay: Duration.seconds(1),
  maxDelay: Duration.minutes(15),
}

export const defaultDeliveryFlow: DeliveryFlow = {
  deliveryRequestsCapacity: 1_024,
  deliveryEventsCapacity: 64,
}

export class AppConfiguration extends Context.Service<AppConfiguration, {
  readonly destination: Destination
  readonly concurrency: DeliveryConcurrency
  readonly flow: DeliveryFlow
  readonly resilience: DeliveryResilience
}>()("Relay/AppConfiguration") {}

const destination = Config.all({
  id: Config.schema(
    DestinationId,
    "RELAY_DESTINATION_ID",
  ).pipe(Config.withDefault(DestinationId.make("dst-primary"))),
  endpoint: Config.url("RELAY_DESTINATION_URL"),
  authorization: Config.redacted("RELAY_DESTINATION_AUTHORIZATION"),
})

const PositiveInteger = Schema.Int.check(Schema.isGreaterThan(0))
const PositiveFiniteDuration = Schema.DurationFromString.check(
  Schema.makeFilter(
    (duration) =>
      Duration.isFinite(duration) &&
      Duration.isPositive(duration),
    { expected: "a positive finite duration" },
  ),
)

const concurrency = Config.all({
  global: Config.schema(
    PositiveInteger,
    "RELAY_GLOBAL_CONCURRENCY",
  ).pipe(Config.withDefault(64)),
  perDestination: Config.schema(
    PositiveInteger,
    "RELAY_DESTINATION_CONCURRENCY",
  ).pipe(Config.withDefault(4)),
})

const flow = Config.all({
  deliveryRequestsCapacity: Config.schema(
    PositiveInteger,
    "RELAY_DELIVERY_REQUESTS_CAPACITY",
  ).pipe(Config.withDefault(defaultDeliveryFlow.deliveryRequestsCapacity)),
  deliveryEventsCapacity: Config.schema(
    PositiveInteger,
    "RELAY_DELIVERY_EVENTS_CAPACITY",
  ).pipe(Config.withDefault(defaultDeliveryFlow.deliveryEventsCapacity)),
})

const resilience = Config.all({
  attemptTimeout: Config.schema(
    PositiveFiniteDuration,
    "RELAY_ATTEMPT_TIMEOUT",
  ).pipe(Config.withDefault(defaultDeliveryResilience.attemptTimeout)),
  maxAttempts: Config.schema(
    PositiveInteger,
    "RELAY_RETRY_MAX_ATTEMPTS",
  ).pipe(Config.withDefault(defaultDeliveryResilience.maxAttempts)),
  maxElapsed: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RETRY_MAX_ELAPSED",
  ).pipe(Config.withDefault(defaultDeliveryResilience.maxElapsed)),
  baseDelay: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RETRY_BASE_DELAY",
  ).pipe(Config.withDefault(defaultDeliveryResilience.baseDelay)),
  maxDelay: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RETRY_MAX_DELAY",
  ).pipe(Config.withDefault(defaultDeliveryResilience.maxDelay)),
})

export const AppConfigurationLive = Layer.effect(
  AppConfiguration,
  Config.all({ destination, concurrency, flow, resilience }).pipe(
    Config.map(({ concurrency, destination, flow, resilience }) =>
      AppConfiguration.of({
        concurrency,
        destination: Destination.make(destination),
        flow,
        resilience,
      })
    ),
  ),
)

import {
  Config,
  Context,
  Duration,
  Effect,
  Layer,
  Option,
  Schema,
} from "effect"
import {
  ConfigurationVersion,
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
  readonly deliveryRequestsPerDestinationCapacity: number
  readonly deliveryEventsCapacity: number
}

export interface DeliveryRecovery {
  readonly claimBatchSize: number
  readonly claimLeaseDuration: Duration.Duration
  readonly claimRenewInterval: Duration.Duration
  readonly pollInterval: Duration.Duration
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
  deliveryRequestsPerDestinationCapacity: 256,
  deliveryEventsCapacity: 64,
}

export const defaultDeliveryRecovery: DeliveryRecovery = {
  claimBatchSize: 64,
  claimLeaseDuration: Duration.seconds(30),
  claimRenewInterval: Duration.seconds(10),
  pollInterval: Duration.seconds(1),
}

export const defaultDestinationConfigurationVersion =
  ConfigurationVersion.make(1)

export class AppConfiguration extends Context.Service<AppConfiguration, {
  readonly destination: Destination
  readonly destinationConfigurationVersion: ConfigurationVersion
  readonly concurrency: DeliveryConcurrency
  readonly flow: DeliveryFlow
  readonly recovery: DeliveryRecovery
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

const destinationConfigurationVersion = Config.schema(
  ConfigurationVersion,
  "RELAY_DESTINATION_CONFIGURATION_VERSION",
).pipe(Config.withDefault(defaultDestinationConfigurationVersion))

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

const DeliveryFlowSchema = Schema.Struct({
  deliveryRequestsCapacity: PositiveInteger,
  deliveryRequestsPerDestinationCapacity: PositiveInteger,
  deliveryEventsCapacity: PositiveInteger,
}).check(
  Schema.makeFilter(
    (policy) =>
      policy.deliveryRequestsPerDestinationCapacity <=
        policy.deliveryRequestsCapacity,
    {
      expected:
        "a per-destination admission capacity no greater than the global admission capacity",
    },
  ),
)

const decodeFlow = Schema.decodeUnknownEffect(DeliveryFlowSchema)

const flow = Config.all({
  deliveryRequestsCapacity: Config.schema(
    PositiveInteger,
    "RELAY_DELIVERY_REQUESTS_CAPACITY",
  ).pipe(Config.withDefault(defaultDeliveryFlow.deliveryRequestsCapacity)),
  deliveryRequestsPerDestinationCapacity: Config.schema(
    PositiveInteger,
    "RELAY_DELIVERY_REQUESTS_PER_DESTINATION_CAPACITY",
  ).pipe(Config.option),
  deliveryEventsCapacity: Config.schema(
    PositiveInteger,
    "RELAY_DELIVERY_EVENTS_CAPACITY",
  ).pipe(Config.withDefault(defaultDeliveryFlow.deliveryEventsCapacity)),
}).pipe(
  Config.mapOrFail((policy) =>
    decodeFlow({
      ...policy,
      deliveryRequestsPerDestinationCapacity: Option.getOrElse(
        policy.deliveryRequestsPerDestinationCapacity,
        () =>
          Math.min(
            defaultDeliveryFlow
              .deliveryRequestsPerDestinationCapacity,
            policy.deliveryRequestsCapacity,
          ),
      ),
    }).pipe(
      Effect.mapError((error) => new Config.ConfigError(error)),
    )
  ),
)

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

const DeliveryRecoverySchema = Schema.Struct({
  claimBatchSize: PositiveInteger,
  claimLeaseDuration: Schema.Duration,
  claimRenewInterval: Schema.Duration,
  pollInterval: Schema.Duration,
}).check(
  Schema.makeFilter(
    (policy) =>
      Duration.toMillis(policy.claimRenewInterval) <
        Duration.toMillis(policy.claimLeaseDuration),
    {
      expected:
        "a recovery policy whose claim renewal interval is shorter than its lease",
    },
  ),
)

const decodeRecovery = Schema.decodeUnknownEffect(DeliveryRecoverySchema)

const recovery = Config.all({
  claimBatchSize: Config.schema(
    PositiveInteger,
    "RELAY_RECOVERY_CLAIM_BATCH_SIZE",
  ).pipe(Config.withDefault(defaultDeliveryRecovery.claimBatchSize)),
  claimLeaseDuration: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RECOVERY_CLAIM_LEASE",
  ).pipe(Config.withDefault(defaultDeliveryRecovery.claimLeaseDuration)),
  claimRenewInterval: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RECOVERY_CLAIM_RENEW_INTERVAL",
  ).pipe(Config.withDefault(defaultDeliveryRecovery.claimRenewInterval)),
  pollInterval: Config.schema(
    PositiveFiniteDuration,
    "RELAY_RECOVERY_POLL_INTERVAL",
  ).pipe(Config.withDefault(defaultDeliveryRecovery.pollInterval)),
}).pipe(
  Config.mapOrFail((policy) =>
    decodeRecovery(policy).pipe(
      Effect.mapError((error) => new Config.ConfigError(error)),
    )
  ),
)

export const AppConfigurationLive = Layer.effect(
  AppConfiguration,
  Config.all({
    destination,
    destinationConfigurationVersion,
    concurrency,
    flow,
    recovery,
    resilience,
  }).pipe(
    Config.map(({
      concurrency,
      destination,
      destinationConfigurationVersion,
      flow,
      recovery,
      resilience,
    }) =>
      AppConfiguration.of({
        concurrency,
        destination: Destination.make(destination),
        destinationConfigurationVersion,
        flow,
        recovery,
        resilience,
      })
    ),
  ),
)

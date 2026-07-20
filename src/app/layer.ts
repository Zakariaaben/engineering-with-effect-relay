import { NodeCrypto } from "@effect/platform-node"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import { ConfigProvider, Layer } from "effect"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import { RelayPersistenceMemory } from "../adapters/memoryPersistence.ts"
import { AppConfigurationLive } from "../configuration.ts"
import { DeliveryEventsLive } from "../deliveryEvents.ts"
import { DeliveryOperationsLive } from "../deliveryOperations.ts"
import {
  DeliveryRepositorySql,
  PostgresLive,
} from "../adapters/postgres/deliveryRepository.ts"
import { DeliverySupervisorLive } from "../deliverySupervisor.ts"
import { DestinationClientLive } from "../http/destination-live.ts"
import { EventIntakeLive } from "../eventIntake.ts"
import {
  DeliveryHttpRoutes,
  IntakeAuthorizationLive,
  OperationsAuthorizationLive,
} from "../httpServer.ts"
import {
  DeliveryAnalystLive,
  IncidentAnalysisAudit,
  IncidentAnalysisAuditMemory,
  IncidentAnalysisModel,
  IncidentAnalysisModelUnavailable,
} from "../incidentAnalyst.ts"
import { RelayIntakeStoreSql } from "../adapters/postgres/intakeStore.ts"
import { PostgresMigrationsLive } from "../adapters/postgres/migrations.ts"
import { RelayReadiness, RelayReadinessLive } from "../readiness.ts"
import { ReconcilerLive } from "../reconciler.ts"
import { DeliveryRepository } from "../deliveryRepository.ts"
import { RelayIntakeStore } from "../intakeStore.ts"
import { WorkerIdentity, WorkerIdentityLive } from "../workerIdentity.ts"

export const RelayPersistenceLive = Layer.mergeAll(
  DeliveryRepositorySql,
  RelayIntakeStoreSql,
  PostgresMigrationsLive,
).pipe(Layer.provide(PostgresLive))

export type RelayPersistenceLayer = Layer.Layer<
  DeliveryRepository | RelayIntakeStore,
  unknown
>

export interface RelayApplicationLayerOptions {
  readonly httpClient: Layer.Layer<HttpClient.HttpClient>
  readonly configProvider: ConfigProvider.ConfigProvider
  readonly persistence?: RelayPersistenceLayer
  readonly workerIdentity?: Layer.Layer<WorkerIdentity, unknown>
  readonly incidentAnalysisModel?: Layer.Layer<IncidentAnalysisModel, unknown>
  readonly incidentAnalysisAudit?: Layer.Layer<IncidentAnalysisAudit, unknown>
}

export const makeRelayAdapterLayer = ({
  httpClient,
  persistence = RelayPersistenceMemory,
}: Pick<RelayApplicationLayerOptions, "httpClient" | "persistence">) =>
  Layer.merge(
    DestinationClientLive.pipe(Layer.provide(httpClient)),
    persistence,
  )

export const makeRelayApplicationLayer = (
  options: RelayApplicationLayerOptions,
) => {
  const configuredAdapters = Layer.mergeAll(
    makeRelayAdapterLayer(options),
    AppConfigurationLive,
    NodeCrypto.layer,
  ).pipe(
    Layer.provide(ConfigProvider.layer(options.configProvider)),
  )
  const applicationDependencies = Layer.merge(
    configuredAdapters,
    options.workerIdentity ?? WorkerIdentityLive,
  )
  const supervisor = DeliverySupervisorLive.pipe(
    Layer.provideMerge(DeliveryEventsLive),
    Layer.provideMerge(applicationDependencies),
  )
  const operations = Layer.mergeAll(
    ReconcilerLive,
    EventIntakeLive,
    DeliveryOperationsLive,
  ).pipe(Layer.provideMerge(supervisor))

  return DeliveryAnalystLive.pipe(
    Layer.provideMerge(operations),
    Layer.provideMerge(
      options.incidentAnalysisModel ?? IncidentAnalysisModelUnavailable,
    ),
    Layer.provideMerge(
      options.incidentAnalysisAudit ?? IncidentAnalysisAuditMemory,
    ),
  )
}

export type RelayHttpServerLayer =
  | ReturnType<typeof NodeHttpServer.layer>
  | ReturnType<typeof NodeHttpServer.layerConfig>

export interface RelayHttpApplicationLayerOptions
  extends RelayApplicationLayerOptions {
  readonly httpServer: RelayHttpServerLayer
  readonly readiness?: Layer.Layer<RelayReadiness>
}

export const makeRelayHttpApplicationLayer = (
  options: RelayHttpApplicationLayerOptions,
) => {
  const application = makeRelayApplicationLayer(options)
  const configuration = ConfigProvider.layer(options.configProvider)

  return HttpRouter.serve(DeliveryHttpRoutes).pipe(
    Layer.provideMerge(application),
    Layer.provideMerge(
      IntakeAuthorizationLive.pipe(Layer.provide(configuration)),
    ),
    Layer.provideMerge(
      OperationsAuthorizationLive.pipe(Layer.provide(configuration)),
    ),
    Layer.provideMerge(options.readiness ?? RelayReadinessLive),
    Layer.provideMerge(options.httpServer),
  )
}

import {
  Clock,
  Context,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
} from "effect"
import { AiError, LanguageModel } from "effect/unstable/ai"
import type { DeliveryRepositoryError } from "./errors.ts"
import {
  DeliveryState,
  type DeliveryStatus,
} from "./delivery.ts"
import { DeliveryId } from "./identifiers.ts"
import { DeliveryOperations } from "./deliveryOperations.ts"

const BoundedAnalysisText = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(500),
)

export const IncidentSuggestion = Schema.Struct({
  summary: BoundedAnalysisText,
  nextChecks: Schema.Tuple([
    BoundedAnalysisText,
    BoundedAnalysisText,
  ]),
})

export interface IncidentSuggestion extends
  Schema.Schema.Type<typeof IncidentSuggestion> {}

export interface IncidentEvidence {
  readonly deliveryId: string
  readonly destinationId: string
  readonly state: DeliveryStatus["delivery"]["state"]["_tag"]
  readonly stateDetail: string | null
  readonly recordedAttemptCount: number
  readonly recentAttempts: ReadonlyArray<{
    readonly ordinal: number
    readonly outcome: DeliveryStatus["attempts"][number]["outcome"]
    readonly decision: DeliveryStatus["attempts"][number]["decision"]
    readonly status: number | null
    readonly retryDelayMillis: number | null
  }>
}

const analysisAttemptLimit = 20

export const IncidentAnalysisReport = Schema.Struct({
  deliveryId: DeliveryId,
  generatedAtMillis: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
  ),
  observedState: Schema.Literals([
    "Pending",
    "Delivered",
    "Rejected",
    "DeadLettered",
    "Terminated",
  ]),
  observedAttemptCount: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
  ),
  source: Schema.Literals(["Model", "DeterministicFallback"]),
  modelFailure: Schema.NullOr(Schema.String),
  suggestion: IncidentSuggestion,
})

export interface IncidentAnalysisReport extends
  Schema.Schema.Type<typeof IncidentAnalysisReport> {}

export class IncidentAnalysisNotFound extends
  Schema.TaggedErrorClass<IncidentAnalysisNotFound>()(
    "IncidentAnalysisNotFound",
    { deliveryId: DeliveryId },
  ) {}

export class IncidentAnalysisModel extends Context.Service<
  IncidentAnalysisModel,
  {
    readonly analyze: (
      evidence: IncidentEvidence,
    ) => Effect.Effect<IncidentSuggestion, AiError.AiError>
  }
>()("Relay/IncidentAnalysisModel") {}

export class IncidentAnalysisAudit extends Context.Service<
  IncidentAnalysisAudit,
  {
    readonly append: (
      report: IncidentAnalysisReport,
    ) => Effect.Effect<void>
    readonly history: () => Effect.Effect<
      ReadonlyArray<IncidentAnalysisReport>
    >
  }
>()("Relay/IncidentAnalysisAudit") {}

export class DeliveryAnalyst extends Context.Service<
  DeliveryAnalyst,
  {
    readonly analyze: (
      deliveryId: DeliveryId,
    ) => Effect.Effect<
      IncidentAnalysisReport,
      IncidentAnalysisNotFound | DeliveryRepositoryError
    >
  }
>()("Relay/DeliveryAnalyst") {}

const makeEvidence = (status: DeliveryStatus): IncidentEvidence => {
  const state = status.delivery.state
  const stateDetail = DeliveryState.match<string | null>(state, {
    Pending: () => null,
    Delivered: ({ status }) => `status:${status}`,
    Rejected: ({ status }) => `status:${status}`,
    DeadLettered: ({ reason }) => `reason:${reason}`,
    Terminated: ({ reason }) => `reason:${reason}`,
  })

  return {
    deliveryId: status.delivery.id,
    destinationId: status.delivery.destinationId,
    state: state._tag,
    stateDetail,
    recordedAttemptCount: status.attempts.length,
    recentAttempts: status.attempts
      .slice(-analysisAttemptLimit)
      .map((attempt) => ({
        ordinal: attempt.ordinal,
        outcome: attempt.outcome,
        decision: attempt.decision,
        status: attempt.status,
        retryDelayMillis: attempt.retryDelayMillis,
      })),
  }
}

const fallbackSuggestion = (
  evidence: IncidentEvidence,
): IncidentSuggestion => IncidentSuggestion.make({
  summary:
    `Automated analysis is unavailable. Delivery ${evidence.deliveryId} is ${evidence.state} with ${evidence.recordedAttemptCount} recorded attempt(s).`,
  nextChecks: [
    "Inspect the ordered attempt history and its last classified outcome.",
    "Follow the recovery runbook before changing durable delivery state.",
  ],
})

export const IncidentAnalysisModelLive = Layer.effect(
  IncidentAnalysisModel,
  Effect.gen(function* () {
    const languageModel = yield* LanguageModel.LanguageModel
    const analyze = Effect.fn("IncidentAnalysisModel.analyze")(
      (evidence: IncidentEvidence) =>
        LanguageModel.generateObject({
          prompt: [
            "Analyze only the supplied Relay delivery evidence.",
            "Return a concise summary and exactly two diagnostic checks.",
            "Do not claim a remote effect occurred and do not recommend changing delivery state.",
            JSON.stringify(evidence),
          ].join("\n"),
          objectName: "relay_incident_suggestion",
          schema: IncidentSuggestion,
        }).pipe(
          Effect.map((response) => response.value),
          Effect.provideService(
            LanguageModel.LanguageModel,
            languageModel,
          ),
        ),
    )
    return IncidentAnalysisModel.of({ analyze })
  }),
)

const unavailableModelError = AiError.make({
  module: "RelayIncidentAnalysisModel",
  method: "analyze",
  reason: new AiError.InternalProviderError({
    description: "No incident-analysis model is configured",
  }),
})

export const IncidentAnalysisModelUnavailable = Layer.succeed(
  IncidentAnalysisModel,
  IncidentAnalysisModel.of({
    analyze: Effect.fn("IncidentAnalysisModel.analyze")(
      () => Effect.fail(unavailableModelError),
    ),
  }),
)

const auditCapacity = 100

export const IncidentAnalysisAuditMemory = Layer.effect(
  IncidentAnalysisAudit,
  Effect.gen(function* () {
    const reports = yield* Ref.make<ReadonlyArray<IncidentAnalysisReport>>([])
    const append = Effect.fn("IncidentAnalysisAudit.append")(
      (report: IncidentAnalysisReport) =>
        Ref.update(reports, (current) => {
          const retained = current.length < auditCapacity
            ? current
            : current.slice(current.length - auditCapacity + 1)
          return [...retained, report]
        }),
    )
    const history = Effect.fn("IncidentAnalysisAudit.history")(
      () => Ref.get(reports),
    )
    return IncidentAnalysisAudit.of({ append, history })
  }),
)

export const DeliveryAnalystLive = Layer.effect(
  DeliveryAnalyst,
  Effect.gen(function* () {
    const operations = yield* DeliveryOperations
    const model = yield* IncidentAnalysisModel
    const audit = yield* IncidentAnalysisAudit

    const analyze = Effect.fn("DeliveryAnalyst.analyze")(
      function* (deliveryId: DeliveryId) {
        const status = yield* operations.status(deliveryId)
        if (Option.isNone(status)) {
          return yield* Effect.fail(
            new IncidentAnalysisNotFound({ deliveryId }),
          )
        }

        const evidence = makeEvidence(status.value)
        const generated = yield* model.analyze(evidence).pipe(
          Effect.match({
            onFailure: (error) => ({
              modelFailure: error.reason._tag,
              source: "DeterministicFallback" as const,
              suggestion: fallbackSuggestion(evidence),
            }),
            onSuccess: (suggestion) => ({
              modelFailure: null,
              source: "Model" as const,
              suggestion,
            }),
          }),
        )
        const report = IncidentAnalysisReport.make({
          deliveryId,
          generatedAtMillis: yield* Clock.currentTimeMillis,
          observedState: evidence.state,
          observedAttemptCount: evidence.recordedAttemptCount,
          ...generated,
        })

        yield* audit.append(report)
        yield* Effect.logInfo("incident.analysis.completed").pipe(
          Effect.annotateLogs({
            "relay.delivery_id": deliveryId,
            "relay.analysis_source": report.source,
            "relay.analysis_model_failure": report.modelFailure ?? "none",
            "relay.analysis_attempt_count": report.observedAttemptCount,
          }),
        )
        return report
      },
    )

    return DeliveryAnalyst.of({ analyze })
  }),
)

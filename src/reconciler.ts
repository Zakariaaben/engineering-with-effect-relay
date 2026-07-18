import {
  Context,
  Duration,
  Effect,
  Layer,
  Metric,
  Schedule,
  Semaphore,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import type { DeliveryRepositoryError } from "./errors.ts"
import type { ClaimedDelivery } from "./services.ts"
import { DeliveryRepository } from "./services.ts"
import { WorkerIdentity } from "./workerIdentity.ts"

const reconciliationPasses = Metric.counter(
  "relay_reconciliation_passes_total",
  {
    description: "Reconciliation scans by result",
    incremental: true,
  },
)

const reconciliationClaims = Metric.counter(
  "relay_reconciliation_claimed_deliveries_total",
  {
    description: "Pending deliveries claimed by reconciliation",
    incremental: true,
  },
)

const reconciliationClaimLag = Metric.histogram(
  "relay_reconciliation_claim_lag_seconds",
  {
    description: "Time eligible delivery work waited before a claim",
    boundaries: [0, 0.1, 0.5, 1, 5, 30, 60, 300, 900, 3_600],
  },
)

export interface ReconciliationReport {
  readonly claimed: number
}

export interface ReconcilerHooks {
  readonly afterClaim?: (
    deliveries: ReadonlyArray<ClaimedDelivery>,
  ) => Effect.Effect<void>
}

export class Reconciler extends Context.Service<Reconciler, {
  readonly reconcileOnce: () => Effect.Effect<
    ReconciliationReport,
    DeliveryRepositoryError
  >
}>()("Relay/Reconciler") {}

export const makeReconcilerLive = (
  hooks: ReconcilerHooks = {},
) => Layer.effect(
  Reconciler,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const repository = yield* DeliveryRepository
    const supervisor = yield* DeliverySupervisor
    const worker = yield* WorkerIdentity
    const mutex = yield* Semaphore.make(1)

    const reconcileOnce = Effect.fn("Reconciler.reconcileOnce")(
      () =>
        mutex.withPermit(
          Effect.gen(function* () {
            const claimed = yield* repository.claimPending(
              worker.id,
              configuration.destination.id,
              configuration.recovery.claimBatchSize,
              Duration.toMillis(
                configuration.recovery.claimLeaseDuration,
              ),
            )
            if (hooks.afterClaim !== undefined) {
              yield* hooks.afterClaim(claimed)
            }
            yield* Effect.all([
              Metric.update(
                Metric.withAttributes(reconciliationPasses, {
                  result: "success",
                }),
                1,
              ),
              Metric.update(reconciliationClaims, claimed.length),
              Effect.forEach(
                claimed,
                (delivery) =>
                  Metric.update(
                    reconciliationClaimLag,
                    delivery.claimLagMillis / 1_000,
                  ),
                { discard: true },
              ),
            ], { discard: true })

            yield* Effect.forEach(
              claimed,
              (delivery) =>
                supervisor.resumeClaimed(delivery).pipe(
                  Effect.matchEffect({
                    onFailure: () =>
                      Effect.logError(
                        "delivery.reconciliation.failed",
                      ).pipe(
                        Effect.annotateLogs({
                          "relay.delivery_id": delivery.delivery.id,
                          "relay.destination_id":
                            delivery.delivery.destinationId,
                          "relay.claim_owner": delivery.claim.ownerId,
                          "relay.claim_generation":
                            delivery.claim.generation,
                        }),
                      ),
                    onSuccess: () => Effect.void,
                  }),
                ),
              {
                concurrency: configuration.concurrency.global,
                discard: true,
              },
            )

            return { claimed: claimed.length }
          }),
        ),
    )

    const safePass = reconcileOnce().pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.all([
            Metric.update(
              Metric.withAttributes(reconciliationPasses, {
                result: "failure",
              }),
              1,
            ),
            Effect.logError("delivery.reconciliation.scan_failed"),
          ], { discard: true }).pipe(Effect.as({ claimed: 0 })),
        onSuccess: Effect.succeed,
      }),
    )

    yield* Effect.repeat(
      safePass,
      Schedule.spaced(configuration.recovery.pollInterval),
    ).pipe(
      Effect.forkScoped({ startImmediately: true }),
    )

    return Reconciler.of({ reconcileOnce })
  }),
)

export const ReconcilerLive = makeReconcilerLive()

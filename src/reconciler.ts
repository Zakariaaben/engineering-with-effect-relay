import {
  Context,
  Effect,
  Layer,
  Schedule,
  Semaphore,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import type { DeliveryRepositoryError } from "./errors.ts"
import type { ClaimedDelivery } from "./services.ts"
import { DeliveryRepository } from "./services.ts"

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
    const mutex = yield* Semaphore.make(1)

    yield* repository.resetClaims()

    const reconcileOnce = Effect.fn("Reconciler.reconcileOnce")(
      () =>
        mutex.withPermit(
          Effect.gen(function* () {
            const claimed = yield* repository.claimPending(
              configuration.destination.id,
              configuration.recovery.claimBatchSize,
            )
            if (hooks.afterClaim !== undefined) {
              yield* hooks.afterClaim(claimed)
            }

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
          Effect.logError("delivery.reconciliation.scan_failed").pipe(
            Effect.as({ claimed: 0 }),
          ),
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

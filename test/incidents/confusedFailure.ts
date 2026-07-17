import { Effect, Exit } from "effect"
import {
  DeliveryOutcome,
  type DeliveryOutcome as DeliveryOutcomeValue,
  type DestinationId,
} from "../../src/model.ts"

// Rejected repair: this invents a provider response for every failure cause.
export const confuseDeliveryFailure = <E, R>(
  program: Effect.Effect<DeliveryOutcomeValue, E, R>,
  destinationId: DestinationId,
): Effect.Effect<DeliveryOutcomeValue, never, R> =>
  program.pipe(
    Effect.exit,
    Effect.map((exit) =>
      Exit.isSuccess(exit)
        ? exit.value
        : DeliveryOutcome.Rejected({ destinationId, status: 500 })
    ),
  )

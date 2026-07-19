import {
  DeliveryAttemptRecord,
  DeliveryResult,
  DeliveryState,
} from "./model.ts"

/** The single domain truth table used by every persistence adapter. */
export const deliveryStateFromResult = (
  result: DeliveryResult,
): DeliveryState =>
  DeliveryResult.$match(result, {
    Delivered: ({ status }) =>
      DeliveryState.cases.Delivered.make({ status }),
    Rejected: ({ status }) =>
      DeliveryState.cases.Rejected.make({ status }),
    ProtocolFailure: () => DeliveryState.cases.DeadLettered.make({
      reason: "ProviderProtocolFailure",
    }),
    Exhausted: () => DeliveryState.cases.DeadLettered.make({
      reason: "RetryBudgetExhausted",
    }),
  })

/** Interpret a persisted attempt without knowing which adapter stored it. */
export const terminalStateFromAttempt = (
  attempt: DeliveryAttemptRecord,
): DeliveryState | undefined => {
  if (attempt.decision === "Exhausted") {
    return DeliveryState.cases.DeadLettered.make({
      reason: "RetryBudgetExhausted",
    })
  }
  if (attempt.decision !== "Terminal") return undefined

  switch (attempt.outcome) {
    case "Delivered":
      return attempt.status === null
        ? undefined
        : DeliveryState.cases.Delivered.make({ status: attempt.status })
    case "Rejected":
      return attempt.status === null
        ? undefined
        : DeliveryState.cases.Rejected.make({ status: attempt.status })
    case "ProtocolFailure":
      return DeliveryState.cases.DeadLettered.make({
        reason: "ProviderProtocolFailure",
      })
    default:
      return undefined
  }
}

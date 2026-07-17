import { Effect } from "effect"
import type { DestinationClientService } from "../../src/destinationClient.ts"
import { deliverCandidate } from "../../src/workflow.ts"
import {
  delivery,
  destination,
  event,
  makeGate,
  provideDestinationClient,
} from "../fixtures.ts"

export const reproduceUnboundedDeliveryPressure = async (
  deliveryCount: number,
) => {
  const allStarted = makeGate<void>()
  const release = makeGate<void>()
  let active = 0
  let maximumActive = 0
  let started = 0

  const client: DestinationClientService = {
    post: async () => {
      active += 1
      started += 1
      maximumActive = Math.max(maximumActive, active)
      if (started === deliveryCount) {
        allStarted.resolve(undefined)
      }

      await release.promise
      active -= 1
      return 202
    },
  }

  const running = Effect.all(
    Array.from(
      { length: deliveryCount },
      () => deliverCandidate(delivery.id, event, destination),
    ),
    { concurrency: "unbounded" },
  ).pipe(
    provideDestinationClient(client),
    Effect.runPromise,
  )

  await allStarted.promise
  const observation = { active, maximumActive, started }
  release.resolve(undefined)
  await running

  return observation
}

import type { DestinationClientService } from "./destinationClient.ts"
import {
  classifyDeliveryStatus,
  makeDeliveryRequest,
  type DeliveryOutcome,
  type Destination,
  type RelayEvent,
} from "./model.ts"

export const sendDeliveryWithPromise = async (
  event: RelayEvent,
  destination: Destination,
  client: DestinationClientService,
  signal: AbortSignal,
): Promise<DeliveryOutcome> => {
  const request = makeDeliveryRequest(event, destination)
  const status = await client.post({ ...request, signal })

  return classifyDeliveryStatus(destination.id, status)
}

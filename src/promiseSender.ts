import type { DestinationClientService } from "./destinationClient.ts"
import {
  classifyDeliveryStatus,
  makeDeliveryRequest,
  type DeliveryId,
  type DeliveryOutcome,
  type Destination,
  type RelayEvent,
} from "./model.ts"

export const sendDeliveryWithPromise = async (
  deliveryId: DeliveryId,
  event: RelayEvent,
  destination: Destination,
  client: DestinationClientService,
  signal: AbortSignal,
): Promise<DeliveryOutcome> => {
  const request = makeDeliveryRequest(
    deliveryId,
    event,
    destination,
  )
  const status = await client.post({ ...request, signal })

  return classifyDeliveryStatus(destination.id, status)
}

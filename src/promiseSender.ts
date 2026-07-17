import type { DestinationClientService } from "./destinationClient.ts"
import {
  classifyDeliveryResponse,
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
  const evidence = await client.post({ ...request, signal })
  const response = typeof evidence === "number"
    ? { status: evidence }
    : evidence

  return classifyDeliveryResponse(
    destination.id,
    response,
    Date.now(),
  )
}

export interface RelayEvent {
  readonly id: string
  readonly type: "invoice.created"
  readonly invoiceId: string
  readonly amountCents: number
}

export interface Destination {
  readonly id: string
  readonly endpoint: URL
  readonly authorization: string
}

export type DeliveryOutcome =
  | {
      readonly _tag: "Delivered"
      readonly destinationId: string
      readonly status: number
    }
  | {
      readonly _tag: "Rejected"
      readonly destinationId: string
      readonly status: number
    }

export const classifyDeliveryStatus = (
  destinationId: string,
  status: number,
): DeliveryOutcome =>
  status >= 200 && status < 300
    ? {
        _tag: "Delivered",
        destinationId,
        status,
      }
    : {
        _tag: "Rejected",
        destinationId,
        status,
      }

export interface DeliveryRequest {
  readonly endpoint: URL
  readonly authorization: string
  readonly body: string
}

export const makeDeliveryRequest = (
  event: RelayEvent,
  destination: Destination,
): DeliveryRequest => ({
  endpoint: destination.endpoint,
  authorization: destination.authorization,
  body: JSON.stringify(event),
})

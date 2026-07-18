import {
  Clock,
  Context,
  Crypto,
  Effect,
  Encoding,
  Layer,
  Option,
  Schema,
} from "effect"
import { AppConfiguration } from "./configuration.ts"
import { DeliverySupervisor } from "./deliverySupervisor.ts"
import {
  EventIdentityError,
  type DeliveryRepositoryError,
  type IngestionConflictError,
  InvalidEventError,
  type RelayIntakeStoreError,
} from "./errors.ts"
import {
  generateDeliveryId,
  generateEventId,
} from "./identifiers.ts"
import {
  DeliveryRouteSnapshot,
  EventAcceptance,
  EventSubmission,
  type IngestionKey,
  RelayEvent,
  RequestFingerprint,
} from "./model.ts"
import {
  IntakeDecision,
  RelayIntakeStore,
} from "./services.ts"

type EventIntakeFailure =
  | DeliveryRepositoryError
  | EventIdentityError
  | IngestionConflictError
  | InvalidEventError
  | RelayIntakeStoreError

const decodeSubmission = Schema.decodeUnknownEffect(EventSubmission)

const fingerprint = Effect.fn("EventIntake.fingerprint")(
  function* (submission: EventSubmission) {
    const crypto = yield* Crypto.Crypto
    const normalized = [
      "relay-intake-v1",
      submission.topic,
      submission.payload.invoiceId,
      submission.payload.amountCents,
    ].join("\n")
    const digest = yield* crypto.digest(
      "SHA-256",
      new TextEncoder().encode(normalized),
    )
    return RequestFingerprint.make(Encoding.encodeHex(digest))
  },
)

export class EventIntake extends Context.Service<EventIntake, {
  readonly accept: (
    ingestionKey: IngestionKey,
    candidate: unknown,
  ) => Effect.Effect<EventAcceptance, EventIntakeFailure>
}>()("Relay/EventIntake") {}

export const EventIntakeLive = Layer.effect(
  EventIntake,
  Effect.gen(function* () {
    const configuration = yield* AppConfiguration
    const crypto = yield* Crypto.Crypto
    const store = yield* RelayIntakeStore
    const supervisor = yield* DeliverySupervisor

    const accept = Effect.fn("EventIntake.accept")(
      function* (ingestionKey: IngestionKey, candidate: unknown) {
        const submission = yield* decodeSubmission(candidate).pipe(
          Effect.mapError((error) =>
            new InvalidEventError({ summary: error.message })
          ),
        )
        const requestFingerprint = yield* fingerprint(submission).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            new EventIdentityError({ operation: "fingerprint", cause })
          ),
        )
        const eventId = yield* generateEventId().pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            new EventIdentityError({ operation: "eventId", cause })
          ),
        )
        const deliveryId = yield* generateDeliveryId().pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            new EventIdentityError({ operation: "deliveryId", cause })
          ),
        )
        const acceptedAtMillis = yield* Clock.currentTimeMillis
        const event = RelayEvent.make({
          id: eventId,
          invoiceId: submission.payload.invoiceId,
          amountCents: submission.payload.amountCents,
        })
        const route = DeliveryRouteSnapshot.make({
          destinationId: configuration.destination.id,
          endpoint: configuration.destination.endpoint,
          configurationVersion:
            configuration.destinationConfigurationVersion,
        })
        const decision = yield* store.accept({
          ingestionKey,
          requestFingerprint,
          event,
          deliveryId,
          route,
          acceptedAtMillis,
        })

        if (IntakeDecision.$is("Accepted")(decision)) {
          yield* supervisor.enqueueClaimed({
            delivery: decision.delivery,
            event: decision.event,
            route: Option.some(decision.route),
          })
        }

        return EventAcceptance.make({
          eventId: decision.event.id,
          deliveryId: decision.delivery.id,
          acceptedAtMillis: decision.acceptedAtMillis,
          replayed: IntakeDecision.$is("Replay")(decision),
        })
      },
    )

    return EventIntake.of({ accept })
  }),
)

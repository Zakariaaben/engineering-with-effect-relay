import { Console, Context, Effect, Layer, Schema } from "effect"
import {
  Argument,
  Command,
  Flag,
  GlobalFlag,
} from "effect/unstable/cli"
import { RelayAdminClient } from "./adminClient.ts"
import {
  DeliveryStatus,
  type DeliveryStatus as DeliveryStatusValue,
} from "./delivery.ts"
import { DeliveryId } from "./identifiers.ts"

export type OutputMode = "human" | "json"

export class ConfirmationRequired extends Schema.TaggedErrorClass<ConfirmationRequired>()(
  "ConfirmationRequired",
  { deliveryId: DeliveryId },
) {}

export class AdminPresenter extends Context.Service<
  AdminPresenter,
  {
    readonly listDeadLetters: (input: {
      readonly mode: OutputMode
      readonly statuses: ReadonlyArray<DeliveryStatusValue>
    }) => Effect.Effect<void>
    readonly retried: (input: {
      readonly mode: OutputMode
      readonly status: DeliveryStatusValue
    }) => Effect.Effect<void>
  }
>()("Relay/AdminPresenter") {}

const encodeStatus = Schema.encodeSync(DeliveryStatus)
const encodeStatuses = Schema.encodeSync(Schema.Array(DeliveryStatus))

const humanDeadLetter = (status: DeliveryStatusValue): string => {
  const state = status.delivery.state
  const reason = state._tag === "DeadLettered"
    ? state.reason
    : state._tag
  return `${status.delivery.id}\t${reason}\tattempts=${status.attempts.length}`
}

export const AdminPresenterLive = Layer.succeed(
  AdminPresenter,
  AdminPresenter.of({
    listDeadLetters: Effect.fn("AdminPresenter.listDeadLetters")(
      ({ mode, statuses }) =>
        mode === "json"
          ? Console.log(JSON.stringify(encodeStatuses(statuses)))
          : Console.log(
            statuses.length === 0
              ? "No dead letters"
              : statuses.map(humanDeadLetter).join("\n"),
          ),
    ),
    retried: Effect.fn("AdminPresenter.retried")(
      ({ mode, status }) =>
        mode === "json"
          ? Console.log(JSON.stringify(encodeStatus(status)))
          : Console.log(
            `Retry accepted for ${status.delivery.id}; state=${status.delivery.state._tag}`,
          ),
    ),
  }),
)

export const Output = GlobalFlag.setting("output")({
  flag: Flag.choice("output", ["human", "json"]).pipe(
    Flag.withDefault("human"),
    Flag.withMetavar("<human|json>"),
    Flag.withDescription("Choose human-readable or JSON output"),
  ),
})

const list = Command.make(
  "list",
  {},
  Effect.fn("RelayAdminCli.list")(function* () {
    const client = yield* RelayAdminClient
    const presenter = yield* AdminPresenter
    const mode = yield* Output
    const statuses = yield* client.listDeadLetters()
    yield* presenter.listDeadLetters({ mode, statuses })
  }),
).pipe(Command.withDescription("List the first 50 dead letters"))

const retry = Command.make(
  "retry",
  {
    deliveryId: Argument.string("delivery-id").pipe(
      Argument.withSchema(DeliveryId),
      Argument.withDescription("Dead-lettered delivery identifier"),
    ),
    yes: Flag.boolean("yes").pipe(
      Flag.withDescription("Confirm the state-changing retry"),
    ),
  },
  Effect.fn("RelayAdminCli.retry")(function* ({ deliveryId, yes }) {
    if (!yes) {
      return yield* new ConfirmationRequired({ deliveryId })
    }
    const client = yield* RelayAdminClient
    const presenter = yield* AdminPresenter
    const mode = yield* Output
    const status = yield* client.retryDeadLetter(deliveryId)
    yield* presenter.retried({ mode, status })
  }),
).pipe(Command.withDescription("Return one dead letter to pending"))

const deadLetters = Command.make("dead-letters").pipe(
  Command.withDescription("Inspect and recover dead-lettered deliveries"),
  Command.withSubcommands([list, retry]),
)

export const relayAdminCommand = Command.make("relayctl").pipe(
  Command.withDescription("Operate a running Relay instance"),
  Command.withExamples([
    {
      command: "relayctl dead-letters list --output json",
      description: "List dead letters for automation",
    },
    {
      command: "relayctl dead-letters retry dlv-example --yes",
      description: "Retry one dead letter explicitly",
    },
  ]),
  Command.withSubcommands([deadLetters]),
  Command.withGlobalFlags([Output]),
)

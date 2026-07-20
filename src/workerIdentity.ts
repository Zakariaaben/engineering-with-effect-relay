import { NodeCrypto } from "@effect/platform-node"
import { Context, Effect, Layer } from "effect"
import { generateWorkerId } from "./identifiers.ts"
import type { WorkerId } from "./identifiers.ts"

export class WorkerIdentity extends Context.Service<WorkerIdentity, {
  readonly id: WorkerId
}>()("Relay/WorkerIdentity") {}

export const WorkerIdentityLive = Layer.effect(
  WorkerIdentity,
  generateWorkerId().pipe(
    Effect.map((id) => WorkerIdentity.of({ id })),
  ),
).pipe(Layer.provide(NodeCrypto.layer))

export const makeWorkerIdentityLayer = (id: WorkerId) =>
  Layer.succeed(WorkerIdentity, WorkerIdentity.of({ id }))

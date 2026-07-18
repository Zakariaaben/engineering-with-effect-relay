import {
  Deferred,
  Effect,
  Fiber,
  Ref,
  Semaphore,
} from "effect"

export const reproduceUnboundedProducerConsumer = (
  produced: number,
  concurrency: number,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const capacity = yield* Semaphore.make(concurrency)
      const active = yield* Ref.make(0)
      const maximumActive = yield* Ref.make(0)
      const owned = yield* Ref.make(0)
      const allOwned = yield* Deferred.make<void>()
      const saturated = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()

      const task = Effect.acquireUseRelease(
        Ref.updateAndGet(owned, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === produced
              ? Deferred.succeed(allOwned, undefined)
              : Effect.void
          ),
        ),
        () =>
          capacity.withPermit(
            Effect.acquireUseRelease(
              Ref.updateAndGet(active, (count) => count + 1).pipe(
                Effect.tap((count) =>
                  Ref.update(
                    maximumActive,
                    (maximum) => Math.max(maximum, count),
                  ),
                ),
                Effect.tap((count) =>
                  count === concurrency
                    ? Deferred.succeed(saturated, undefined)
                    : Effect.void
                ),
              ),
              () => Deferred.await(release),
              () => Ref.update(active, (count) => count - 1),
            ),
          ),
        () => Ref.update(owned, (count) => count - 1),
      )

      const running = yield* Effect.forEach(
        Array.from({ length: produced }),
        () => task,
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.forkChild({ startImmediately: true }))

      yield* Deferred.await(allOwned)
      yield* Deferred.await(saturated)

      const activeNow = yield* Ref.get(active)
      const ownedNow = yield* Ref.get(owned)
      const observation = {
        active: activeNow,
        maximumActive: yield* Ref.get(maximumActive),
        owned: ownedNow,
        waiting: ownedNow - activeNow,
      }

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(running)
      return observation
    }),
  )

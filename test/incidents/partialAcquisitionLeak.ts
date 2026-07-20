/**
 * Deliberately broken M2 incident fixture.
 *
 * The bundle is assigned only after both acquisitions succeed. If the second
 * acquisition fails, the first resource is no longer reachable by `finally`.
 */
export const reproducePartialAcquisitionLeak = async () => {
  const events: Array<string> = []
  let poolOpen = false
  let resources: {
    readonly close: () => void
  } | undefined

  try {
    poolOpen = true
    events.push("pool:acquire")

    events.push("session:acquire")
    throw new Error("destination session unavailable")

    // This assignment is unreachable when the second acquisition fails.
    resources = {
      close: () => {
        poolOpen = false
        events.push("pool:release")
      },
    }
  } catch {
    events.push("startup:failure")
  } finally {
    resources?.close()
  }

  return { events, poolOpen }
}

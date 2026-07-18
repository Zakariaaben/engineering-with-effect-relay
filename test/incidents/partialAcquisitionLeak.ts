/**
 * Deliberately broken C03-09 incident fixture.
 *
 * The bundle is assigned only after both acquisitions succeed. If the second
 * acquisition fails, the first resource is no longer reachable by `finally`.
 */
export const reproducePartialAcquisitionLeak = async () => {
  const events: Array<string> = []
  let repositoryOpen = false
  let resources: {
    readonly close: () => void
  } | undefined

  try {
    repositoryOpen = true
    events.push("repository:acquire")

    events.push("destination:acquire")
    throw new Error("destination configuration rejected")

    // This assignment is unreachable when the second acquisition fails.
    resources = {
      close: () => {
        repositoryOpen = false
        events.push("repository:release")
      },
    }
  } catch {
    events.push("startup:failure")
  } finally {
    resources?.close()
  }

  return { events, repositoryOpen }
}

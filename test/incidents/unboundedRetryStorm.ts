export const reproduceImmediateRetryStorm = async (
  deliveryCount: number,
  attemptsPerDelivery: number,
) => {
  let totalAttempts = 0
  let exhaustedDeliveries = 0
  const attemptsAtLogicalTime = new Map<number, number>()

  const runDelivery = async () => {
    for (let attempt = 0; attempt < attemptsPerDelivery; attempt += 1) {
      totalAttempts += 1
      attemptsAtLogicalTime.set(
        0,
        (attemptsAtLogicalTime.get(0) ?? 0) + 1,
      )
      await Promise.resolve()
    }
    exhaustedDeliveries += 1
  }

  await Promise.all(
    Array.from({ length: deliveryCount }, runDelivery),
  )

  return {
    totalAttempts,
    exhaustedDeliveries,
    maximumBurst: Math.max(...attemptsAtLogicalTime.values()),
  }
}

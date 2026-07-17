import { describe, expect, it } from "bun:test"
import {
  Config,
  ConfigProvider,
  Duration,
  Effect,
  Redacted,
} from "effect"
import {
  AppConfiguration,
  AppConfigurationLive,
} from "../src/configuration.ts"

const loadConfiguration = (values: Record<string, unknown>) =>
  AppConfiguration.pipe(
    Effect.provide(AppConfigurationLive),
    Effect.provide(
      ConfigProvider.layer(ConfigProvider.fromUnknown(values)),
    ),
  )

describe("C03-07 application configuration", () => {
  it("loads required values, applies the missing ID default, and redacts the secret", async () => {
    const configuration = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "test-authorization",
      }),
    )

    expect(String(configuration.destination.id)).toBe("dst-primary")
    expect(configuration.destination.endpoint.href).toBe(
      "https://hooks.example.test/invoices",
    )
    expect(String(configuration.destination.authorization)).toBe("<redacted>")
    expect(JSON.stringify(configuration.destination.authorization)).toBe(
      '"<redacted>"',
    )
    expect(Redacted.value(configuration.destination.authorization)).toBe(
      "test-authorization",
    )
    expect(configuration.concurrency).toEqual({
      global: 64,
      perDestination: 4,
    })
    expect({
      attemptTimeout: Duration.toMillis(
        configuration.resilience.attemptTimeout,
      ),
      maxAttempts: configuration.resilience.maxAttempts,
      maxElapsed: Duration.toMillis(
        configuration.resilience.maxElapsed,
      ),
      baseDelay: Duration.toMillis(
        configuration.resilience.baseDelay,
      ),
      maxDelay: Duration.toMillis(
        configuration.resilience.maxDelay,
      ),
    }).toEqual({
      attemptTimeout: 30_000,
      maxAttempts: 8,
      maxElapsed: 86_400_000,
      baseDelay: 1_000,
      maxDelay: 900_000,
    })
  })

  it("loads positive concurrency limits", async () => {
    const configuration = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "test-authorization",
        RELAY_GLOBAL_CONCURRENCY: 8,
        RELAY_DESTINATION_CONCURRENCY: 2,
      }),
    )

    expect(configuration.concurrency).toEqual({
      global: 8,
      perDestination: 2,
    })
  })

  it("rejects non-positive concurrency limits", async () => {
    const error = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
        RELAY_GLOBAL_CONCURRENCY: 0,
      }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Config.ConfigError)
    expect(error.message).toContain("RELAY_GLOBAL_CONCURRENCY")
    expect(error.message).not.toContain("must-not-leak")
  })

  it("loads a finite bounded retry policy", async () => {
    const configuration = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "test-authorization",
        RELAY_ATTEMPT_TIMEOUT: "2 seconds",
        RELAY_RETRY_MAX_ATTEMPTS: 3,
        RELAY_RETRY_MAX_ELAPSED: "1 minute",
        RELAY_RETRY_BASE_DELAY: "250 millis",
        RELAY_RETRY_MAX_DELAY: "5 seconds",
      }),
    )

    expect({
      attemptTimeout: Duration.toMillis(
        configuration.resilience.attemptTimeout,
      ),
      maxAttempts: configuration.resilience.maxAttempts,
      maxElapsed: Duration.toMillis(
        configuration.resilience.maxElapsed,
      ),
      baseDelay: Duration.toMillis(
        configuration.resilience.baseDelay,
      ),
      maxDelay: Duration.toMillis(
        configuration.resilience.maxDelay,
      ),
    }).toEqual({
      attemptTimeout: 2_000,
      maxAttempts: 3,
      maxElapsed: 60_000,
      baseDelay: 250,
      maxDelay: 5_000,
    })
  })

  it.each([
    ["RELAY_RETRY_MAX_ATTEMPTS", 0],
    ["RELAY_ATTEMPT_TIMEOUT", "0 seconds"],
    ["RELAY_RETRY_MAX_ELAPSED", "-1 second"],
  ] as const)("rejects invalid retry setting %s", async (key, value) => {
    const error = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
        [key]: value,
      }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Config.ConfigError)
    expect(error.message).toContain(key)
    expect(error.message).not.toContain("must-not-leak")
  })

  it("does not replace an invalid refined ID with the default", async () => {
    const error = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_ID: "not a destination ID",
        RELAY_DESTINATION_URL: "https://hooks.example.test/invoices",
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
      }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Config.ConfigError)
    expect(error.message).toContain("RELAY_DESTINATION_ID")
    expect(error.message).not.toContain("must-not-leak")
  })

  it("fails when the required destination URL is missing", async () => {
    const error = await Effect.runPromise(
      loadConfiguration({
        RELAY_DESTINATION_AUTHORIZATION: "must-not-leak",
      }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(Config.ConfigError)
    expect(error.message).toContain("RELAY_DESTINATION_URL")
    expect(error.message).not.toContain("must-not-leak")
  })
})

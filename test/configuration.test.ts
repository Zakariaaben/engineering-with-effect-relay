import { describe, expect, it } from "bun:test"
import {
  Config,
  ConfigProvider,
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

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

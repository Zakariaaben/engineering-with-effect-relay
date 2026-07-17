import type { RelayEvent } from "../../src/model.ts"

/**
 * Deliberately unsafe C02-01 incident fixture.
 *
 * TypeScript 7.0.2 declares JSON.parse as returning `any`, so this annotation
 * is accepted without checking the parsed value. Keep this helper out of the
 * application path; C02-02 and C02-03 replace the assumption with decoding.
 */
export const unsafeParseRelayEvent = (
  text: string,
): RelayEvent => JSON.parse(text)

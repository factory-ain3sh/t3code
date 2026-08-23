import { PROVIDER_DISPLAY_NAMES, ProviderDriverKind } from "@t3tools/contracts";

/**
 * Resolves a raw driver slug through the canonical built-in display-name table.
 * Callers provide the surface-appropriate fallback for custom drivers.
 */
export function resolveProviderDisplayName(driver: string, fallback: string): string {
  const driverKind = ProviderDriverKind.make(driver === "claude" ? "claudeAgent" : driver);
  return PROVIDER_DISPLAY_NAMES[driverKind] ?? fallback;
}

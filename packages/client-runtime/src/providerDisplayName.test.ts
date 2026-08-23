import { describe, expect, it } from "vite-plus/test";

import { resolveProviderDisplayName } from "./providerDisplayName.ts";

describe("resolveProviderDisplayName", () => {
  it("uses canonical built-in names, including the historical claude alias", () => {
    expect(resolveProviderDisplayName("droid", "droid")).toBe("Droid");
    expect(resolveProviderDisplayName("grok", "grok")).toBe("Grok");
    expect(resolveProviderDisplayName("claude", "claude")).toBe("Claude");
  });

  it("preserves the caller's fallback for custom drivers", () => {
    expect(resolveProviderDisplayName("acmeAgent", "Acme Agent")).toBe("Acme Agent");
  });
});

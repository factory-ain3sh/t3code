import { describe, expect, it } from "vite-plus/test";

import { ProviderDriverError } from "./Errors.ts";

describe("ProviderDriverError", () => {
  it("keeps the wrapper message stable while preserving the structured cause", () => {
    const cause = new Error("Droid executable was not found");
    const error = new ProviderDriverError({
      driver: "droid",
      instanceId: "droid-default",
      detail: "Failed to build Droid snapshot.",
      cause,
    });

    expect(error.message).toBe(
      "Provider driver 'droid' failed to create instance 'droid-default': Failed to build Droid snapshot.",
    );
    expect(error.cause).toBe(cause);
  });

  it("omits the cause suffix when no cause is present", () => {
    const error = new ProviderDriverError({
      driver: "droid",
      instanceId: "droid-default",
      detail: "Failed to build Droid snapshot.",
    });

    expect(error.message).toBe(
      "Provider driver 'droid' failed to create instance 'droid-default': Failed to build Droid snapshot.",
    );
  });
});

import { describe, expect, it } from "vite-plus/test";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";
import { ProviderDriverKind } from "@t3tools/contracts";

import {
  mapAcpToAdapterError,
  selectAcpPermissionOptionId,
  supportedAcpApprovalDecisions,
} from "./AcpAdapterSupport.ts";

describe("AcpAdapterSupport", () => {
  it("derives supported decisions from the options the ACP request offered", () => {
    const request = {
      sessionId: "session-1",
      toolCall: {
        toolCallId: "tool-1",
        title: "Run tests",
      },
      options: [
        { optionId: "once", name: "Allow once", kind: "allow_once" },
        { optionId: "reject", name: "Reject", kind: "reject_once" },
      ],
    } as EffectAcpSchema.RequestPermissionRequest;

    expect(supportedAcpApprovalDecisions(request)).toEqual(["accept", "decline", "cancel"]);
    expect(selectAcpPermissionOptionId(request, "accept")).toBe("once");
    expect(selectAcpPermissionOptionId(request, "acceptForSession")).toBeUndefined();
  });

  it("maps ACP request errors to provider adapter request errors", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("cursor"),
      "thread-1" as never,
      "session/prompt",
      new EffectAcpErrors.AcpRequestError({
        code: -32602,
        errorMessage: "Invalid params",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain("Invalid params");
  });
});

import { describe, expect, it } from "vite-plus/test";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";
import { ProviderDriverKind } from "@t3tools/contracts";

import {
  acpApprovalOptions,
  mapAcpToAdapterError,
  selectAcpPermissionOptionId,
} from "./AcpAdapterSupport.ts";

describe("AcpAdapterSupport", () => {
  it("derives approval options from the options the ACP request offered", () => {
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

    expect(acpApprovalOptions(request)).toEqual([
      { decision: "accept", label: "Allow once" },
      { decision: "decline", label: "Reject" },
      { decision: "cancel", label: "Cancel" },
    ]);
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

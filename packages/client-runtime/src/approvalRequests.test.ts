import { describe, expect, it } from "vite-plus/test";

import { approvalOptionsFromPayload, approvalRequestKindFromPayload } from "./approvalRequests.ts";

describe("approvalOptionsFromPayload", () => {
  it("returns undefined when the payload carries no usable options", () => {
    expect(approvalOptionsFromPayload(undefined)).toBeUndefined();
    expect(approvalOptionsFromPayload(null)).toBeUndefined();
    expect(approvalOptionsFromPayload({})).toBeUndefined();
    expect(approvalOptionsFromPayload({ options: "accept" })).toBeUndefined();
    expect(approvalOptionsFromPayload({ options: [] })).toBeUndefined();
    expect(
      approvalOptionsFromPayload({ options: [{ decision: "nuke", label: "x" }] }),
    ).toBeUndefined();
  });

  it("filters malformed entries but keeps valid ones", () => {
    expect(
      approvalOptionsFromPayload({
        options: [
          { decision: "nuke", label: "x" },
          { decision: "accept", label: "Approve" },
          "decline",
          { decision: "decline", label: "Decline" },
        ],
      }),
    ).toEqual([
      { decision: "accept", label: "Approve" },
      { decision: "decline", label: "Decline" },
    ]);
  });

  it("passes through a fully valid option set", () => {
    const options = [
      { decision: "accept", label: "Approve" },
      { decision: "acceptForSession", label: "Always allow this session" },
      { decision: "decline", label: "Decline" },
      { decision: "cancel", label: "Cancel" },
    ];
    expect(approvalOptionsFromPayload({ options })).toEqual(options);
  });
});

describe("approvalRequestKindFromPayload", () => {
  it("prefers the server-stamped requestKind over the raw requestType", () => {
    expect(
      approvalRequestKindFromPayload({
        requestKind: "plan",
        requestType: "command_execution_approval",
      }),
    ).toBe("plan");
  });

  it("falls back to the requestType mapping when requestKind is absent or unknown", () => {
    expect(approvalRequestKindFromPayload({ requestType: "plan_approval" })).toBe("plan");
    expect(approvalRequestKindFromPayload({ requestType: "dynamic_tool_call" })).toBe("command");
    expect(approvalRequestKindFromPayload({ requestType: "mcp_elicitation_approval" })).toBe(
      "mcp-elicitation",
    );
    expect(
      approvalRequestKindFromPayload({ requestKind: "future-kind", requestType: "plan_approval" }),
    ).toBe("plan");
  });

  it("returns null when neither field identifies the request", () => {
    expect(approvalRequestKindFromPayload(undefined)).toBeNull();
    expect(approvalRequestKindFromPayload(null)).toBeNull();
    expect(approvalRequestKindFromPayload({})).toBeNull();
    expect(approvalRequestKindFromPayload({ requestType: "future_request_type" })).toBeNull();
  });
});

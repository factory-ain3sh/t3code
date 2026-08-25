import { describe, expect, it } from "vite-plus/test";
import { EventId, type OrchestrationThreadActivity, TurnId } from "@t3tools/contracts";

import {
  approvalOptionsFromPayload,
  approvalRequestKindFromPayload,
  reducePendingApprovals,
} from "./approvalRequests.ts";

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
  it("prefers a server-stamped kind and falls back to the canonical request-type mapper", () => {
    expect(
      approvalRequestKindFromPayload({
        requestKind: "plan",
        requestType: "command_execution_approval",
      }),
    ).toBe("plan");
    expect(
      approvalRequestKindFromPayload({ requestKind: "future-kind", requestType: "plan_approval" }),
    ).toBe("plan");
    expect(approvalRequestKindFromPayload({ requestType: "future_request_type" })).toBeNull();
  });
});

function activity(
  id: string,
  kind: string,
  createdAt: string,
  payload: Record<string, unknown>,
): OrchestrationThreadActivity {
  return {
    id: EventId.make(id),
    kind,
    summary: kind,
    tone: "approval",
    payload,
    turnId: TurnId.make("turn-1"),
    createdAt,
  };
}

describe("reducePendingApprovals", () => {
  it("reduces requested, resolved, and stale-failure activities", () => {
    const options = [
      { decision: "decline", label: "Decline" },
      { decision: "acceptAlways", label: "Always allow Safari" },
      { decision: "accept", label: "Approve" },
    ];

    expect(
      reducePendingApprovals([
        activity("open-app", "approval.requested", "2026-08-24T00:00:00.000Z", {
          requestId: "req-app",
          requestType: "mcp_elicitation_approval",
          detail: "Allow ChatGPT to use Safari?",
          appName: "Safari",
          options,
        }),
        activity("open-resolved", "approval.requested", "2026-08-24T00:00:01.000Z", {
          requestId: "req-resolved",
          requestKind: "command",
        }),
        activity("resolve", "approval.resolved", "2026-08-24T00:00:02.000Z", {
          requestId: "req-resolved",
        }),
        activity("open-stale", "approval.requested", "2026-08-24T00:00:03.000Z", {
          requestId: "req-stale",
          requestKind: "file-change",
        }),
        activity("stale-failure", "provider.approval.respond.failed", "2026-08-24T00:00:04.000Z", {
          requestId: "req-stale",
          detail: "Unknown pending permission request: req-stale",
        }),
      ]),
    ).toEqual([
      {
        requestId: "req-app",
        requestKind: "mcp-elicitation",
        createdAt: "2026-08-24T00:00:00.000Z",
        detail: "Allow ChatGPT to use Safari?",
        appName: "Safari",
        options,
      },
    ]);
  });
});

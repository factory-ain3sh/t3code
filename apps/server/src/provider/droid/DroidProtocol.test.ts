import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  DroidLoadSessionParams,
  DroidLoadSessionResult,
  DroidPermissionRequest,
  DroidSessionNotification,
} from "./DroidProtocol.ts";

const decodeNotification = Schema.decodeUnknownSync(DroidSessionNotification);
const decodePermissionRequest = Schema.decodeUnknownSync(DroidPermissionRequest);
const decodeLoadSessionResult = Schema.decodeUnknownSync(DroidLoadSessionResult);
const encodeLoadSessionParams = Schema.encodeUnknownSync(DroidLoadSessionParams);

const usage = {
  inputTokens: 10,
  outputTokens: 5,
  cacheCreationTokens: 1,
  cacheReadTokens: 2,
  thinkingTokens: 3,
};

const fixtures: ReadonlyArray<readonly [string, Record<string, unknown>]> = [
  [
    "assistant_text_delta",
    { type: "assistant_text_delta", messageId: "m1", blockIndex: 0, textDelta: "hello" },
  ],
  ["assistant_text_complete", { type: "assistant_text_complete", messageId: "m1", blockIndex: 0 }],
  [
    "thinking_text_delta",
    { type: "thinking_text_delta", messageId: "m1", blockIndex: 0, textDelta: "hmm" },
  ],
  [
    "thinking_text_complete",
    { type: "thinking_text_complete", messageId: "m1", blockIndex: 0, durationMs: 12 },
  ],
  [
    "tool_call",
    {
      type: "tool_call",
      toolUse: { type: "tool_use", id: "tool-1", input: { path: "README.md" }, name: "Read" },
    },
  ],
  [
    "tool_result",
    {
      type: "tool_result",
      messageId: "m1",
      toolUseId: "tool-1",
      content: [{ type: "text", text: "contents" }],
    },
  ],
  [
    "tool_progress_update",
    {
      type: "tool_progress_update",
      toolUseId: "tool-1",
      toolName: "Read",
      update: { type: "status", status: "running" },
    },
  ],
  [
    "tool_execution_phase_changed",
    {
      type: "tool_execution_phase_changed",
      toolUseId: "tool-1",
      toolName: "Read",
      phase: "executing",
    },
  ],
  ["create_message", { type: "create_message", message: { id: "m1", role: "assistant" } }],
  ["droid_working_state_changed", { type: "droid_working_state_changed", newState: "thinking" }],
  [
    "agent_turn_completed",
    { type: "agent_turn_completed", reason: "completed", turnId: "turn-1", tokenUsage: usage },
  ],
  [
    "session_token_usage_changed",
    {
      type: "session_token_usage_changed",
      sessionId: "s1",
      tokenUsage: usage,
      lastCallTokenUsage: {
        inputTokens: 8,
        cacheReadTokens: 2,
        outputTokens: 4,
      },
    },
  ],
  [
    "session_compacted",
    {
      type: "session_compacted",
      summaryId: "summary-1",
      removedCount: 12,
      visibleBoundaryMessageId: "message-4",
    },
  ],
  [
    "error",
    {
      type: "error",
      message: "bad",
      errorType: "SessionError",
      timestamp: "2026-08-23T00:00:00.000Z",
    },
  ],
  ["llm_retry", { type: "llm_retry", attempt: 2, reason: "rate_limited" }],
  [
    "session_title_updated",
    { type: "session_title_updated", title: "A useful title", updateType: "llm_generated" },
  ],
  [
    "child_session_available",
    { type: "child_session_available", childSessionId: "child-1", timestamp: 123 },
  ],
  [
    "permission_resolved",
    {
      type: "permission_resolved",
      requestId: "permission-1",
      toolUseIds: ["tool-1"],
      selectedOption: "proceed_once",
    },
  ],
  [
    "queued_messages_discarded",
    { type: "queued_messages_discarded", text: "discarded", requestId: "queued-1" },
  ],
  ["mcp_status_changed", { type: "mcp_status_changed", servers: [], summary: { status: "ready" } }],
  [
    "settings_updated",
    {
      type: "settings_updated",
      requestId: "settings-1",
      settings: { modelId: "mock-fast", reasoningEffort: "high", autonomyLevel: "medium" },
    },
  ],
  [
    "structured_output",
    { type: "structured_output", messageId: "m1", structuredOutput: { answer: 42 } },
  ],
];

describe("DroidSessionNotification", () => {
  for (const [type, fixture] of fixtures) {
    it(`decodes ${type}`, () => {
      const decoded = decodeNotification(fixture);
      assert.equal(decoded.type, type);
    });
  }

  it("decodes unknown notification types into the forward-compatible fallback", () => {
    const fixture = {
      type: "future_notification",
      newField: { nested: true },
    };
    const decoded = decodeNotification(fixture);

    assert.equal(decoded.type, "__unknown__");
    assert.property(decoded, "payload");
    if ("payload" in decoded) {
      assert.deepStrictEqual(decoded.payload, fixture);
      assert.equal(decoded.notificationType, "future_notification");
    }
  });

  it("tolerates and strips extra fields from known notifications", () => {
    const decoded = decodeNotification({
      type: "assistant_text_delta",
      messageId: "m1",
      blockIndex: 0,
      textDelta: "hello",
      addedByNewerCli: true,
    });

    assert.equal(decoded.type, "assistant_text_delta");
    assert.notProperty(decoded, "addedByNewerCli");
  });

  it("decodes lastCallTokenUsage for context-window accounting", () => {
    const decoded = decodeNotification({
      type: "session_token_usage_changed",
      sessionId: "s1",
      tokenUsage: usage,
      lastCallTokenUsage: {
        inputTokens: 8,
        cacheReadTokens: 2,
        outputTokens: 4,
      },
    });

    assert.equal(decoded.type, "session_token_usage_changed");
    if (decoded.type === "session_token_usage_changed") {
      assert.deepStrictEqual(decoded.lastCallTokenUsage, {
        inputTokens: 8,
        cacheReadTokens: 2,
        outputTokens: 4,
      });
    }
  });

  it("rejects malformed lastCallTokenUsage instead of treating it as unknown", () => {
    assert.throws(() =>
      decodeNotification({
        type: "session_token_usage_changed",
        sessionId: "s1",
        tokenUsage: usage,
        lastCallTokenUsage: {
          inputTokens: "invalid",
          cacheReadTokens: 2,
        },
      }),
    );
  });

  it("decodes spec_handoff as a successful terminal reason", () => {
    const decoded = decodeNotification({
      type: "agent_turn_completed",
      reason: "spec_handoff",
      turnId: "spec-turn",
      tokenUsage: usage,
    });

    assert.equal(decoded.type, "agent_turn_completed");
    if (decoded.type === "agent_turn_completed") {
      assert.equal(decoded.reason, "spec_handoff");
    }
  });

  it("rejects completion reasons outside the protocol enum", () => {
    assert.throws(() =>
      decodeNotification({
        type: "agent_turn_completed",
        reason: "future_reason",
        turnId: "future-turn",
        tokenUsage: usage,
      }),
    );
  });
});

describe("DroidLoadSessionParams", () => {
  it("encodes MCP server overrides with header name/value pairs", () => {
    const encoded = encodeLoadSessionParams({
      sessionId: "session-1",
      mcpServers: [
        {
          type: "http",
          name: "t3-code",
          url: "https://example.com/mcp",
          headers: [{ name: "Authorization", value: "Bearer token" }],
        },
      ],
    });

    assert.deepStrictEqual(encoded, {
      sessionId: "session-1",
      mcpServers: [
        {
          type: "http",
          name: "t3-code",
          url: "https://example.com/mcp",
          headers: [{ name: "Authorization", value: "Bearer token" }],
        },
      ],
    });
  });
});

describe("DroidLoadSessionResult", () => {
  it("decodes resumed-session lastCallTokenUsage", () => {
    const decoded = decodeLoadSessionResult({
      session: { messages: [] },
      settings: {
        modelId: "mock-fast",
        reasoningEffort: "medium",
      },
      lastCallTokenUsage: {
        inputTokens: 21,
        cacheReadTokens: 5,
        outputTokens: 3,
      },
    });

    assert.deepStrictEqual(decoded.lastCallTokenUsage, {
      inputTokens: 21,
      cacheReadTokens: 5,
      outputTokens: 3,
    });
  });
});

describe("DroidPermissionRequest", () => {
  const detailsFixtures = [
    {
      type: "edit",
      filePath: "/repo/src/app.ts",
      fileName: "app.ts",
      oldContent: "const oldValue = 1;",
      newContent: "const newValue = 2;",
    },
    {
      type: "create",
      filePath: "/repo/src/new.ts",
      fileName: "new.ts",
      content: "export const created = true;",
    },
    {
      type: "apply_patch",
      filePath: "/repo/src/app.ts",
      fileName: "app.ts",
      patchContent: "*** Begin Patch",
      oldContent: "old",
      newContent: "new",
      files: [
        {
          filePath: "/repo/src/app.ts",
          fileName: "app.ts",
          operation: "update",
          moveTo: "/repo/src/renamed.ts",
          oldContent: "old",
          newContent: "new",
        },
      ],
    },
    {
      type: "exit_spec_mode",
      plan: "# Implementation plan",
      title: "Implement the feature",
    },
    {
      type: "propose_mission",
      proposal: "Split the work across three agents.",
      title: "Parallel implementation",
    },
    {
      type: "sandbox_violation",
      violatingToolName: "Execute",
      target: "/etc/passwd",
      operationType: "read",
      violationType: "filesystem-read",
      reason: "The target is outside the workspace.",
      violationReason: "not-allowed",
      isOrgDeny: false,
    },
  ] as const;

  for (const details of detailsFixtures) {
    it(`preserves ${details.type} safety details and the raw params`, () => {
      const raw = {
        toolUses: [
          {
            toolUse: {
              type: "tool_use",
              id: `tool-${details.type}`,
              input: { path: "/repo/src/app.ts" },
              name: "ExampleTool",
            },
            confirmationType: details.type,
            details,
          },
        ],
        options: [{ label: "Allow once", value: "proceed_once" }],
        associatedSessionIds: ["child-session"],
        upstreamExtension: { remainsAvailable: true },
      };

      const decoded = decodePermissionRequest(raw);

      assert.deepStrictEqual(decoded.toolUses[0]?.details, details);
      assert.equal(decoded.options[0]?.outcome, "proceed_once");
      assert.deepStrictEqual(decoded.raw, raw);
    });
  }
});

import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  DroidExecuteRewindResult,
  DroidInitializeSessionResult,
  DroidLoadSessionResult,
  DroidModelInfo,
  DroidPermissionRequest,
  DroidSessionNotification,
  DroidSkillInfo,
  knownDroidSessionNotificationTypes,
} from "./DroidProtocol.ts";

const decodeNotification = Schema.decodeUnknownSync(DroidSessionNotification);
const decodePermissionRequest = Schema.decodeUnknownSync(DroidPermissionRequest);
const decodeInitializeSessionResult = Schema.decodeUnknownSync(DroidInitializeSessionResult);
const decodeLoadSessionResult = Schema.decodeUnknownSync(DroidLoadSessionResult);
const decodeExecuteRewindResult = Schema.decodeUnknownSync(DroidExecuteRewindResult);
const decodeModelInfo = Schema.decodeUnknownSync(DroidModelInfo);
const decodeSkillInfo = Schema.decodeUnknownSync(DroidSkillInfo);

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
  it("keeps the known-type guard in parity with every schema member", () => {
    assert.deepStrictEqual(
      [...knownDroidSessionNotificationTypes].sort(),
      [...new Set([...fixtures.map(([type]) => type), "tool_progress_update"])].sort(),
    );
  });

  for (const [type, fixture] of fixtures) {
    it(`decodes ${type}`, () => {
      const decoded = decodeNotification(fixture);
      assert.equal(decoded.type, type);
    });
  }

  it("decodes unknown notification types into the forward-compatible fallback", () => {
    const decoded = decodeNotification({
      type: "future_notification",
      newField: { nested: true },
    });

    assert.equal(decoded.type, "__unknown__");
    assert.deepStrictEqual(decoded, { type: "__unknown__" });
  });

  it("preserves future terminal reasons for adapter-level failure projection", () => {
    const decoded = decodeNotification({
      type: "agent_turn_completed",
      reason: "future_terminal_reason",
      turnId: "turn-future",
      tokenUsage: usage,
    });

    assert.equal(decoded.type, "agent_turn_completed");
    if (decoded.type === "agent_turn_completed") {
      assert.equal(decoded.reason, "future_terminal_reason");
    }
  });

  it("decodes ignored notifications from their discriminator alone", () => {
    assert.deepStrictEqual(
      decodeNotification({
        type: "settings_updated",
        settings: "reshaped-by-a-newer-cli",
      }),
      { type: "settings_updated" },
    );
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

  it("decodes attributed tool progress and strips unknown update fields", () => {
    const decoded = decodeNotification({
      type: "tool_progress_update",
      toolUseId: "tool-1",
      toolName: "Task",
      update: {
        type: "status",
        status: "running",
        text: "Inspecting the repository",
        subagentSessionId: "child-1",
        addedByNewerCli: true,
      },
    });

    assert.equal(decoded.type, "tool_progress_update");
    if (decoded.type === "tool_progress_update") {
      assert.deepStrictEqual(decoded.update, {
        status: "running",
        text: "Inspecting the repository",
        subagentSessionId: "child-1",
      });
      assert.notProperty(decoded.update, "addedByNewerCli");
    }
  });

  it("decodes tool progress without a subagent session id", () => {
    const decoded = decodeNotification({
      type: "tool_progress_update",
      toolUseId: "tool-2",
      toolName: "Execute",
      update: {
        type: "message",
        details: "Still running",
        valueSnippet: "line 42",
      },
    });

    assert.equal(decoded.type, "tool_progress_update");
    if (decoded.type === "tool_progress_update") {
      assert.deepStrictEqual(decoded.update, {
        details: "Still running",
        valueSnippet: "line 42",
      });
      assert.notProperty(decoded.update, "subagentSessionId");
    }
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
});

describe("DroidInitializeSessionResult", () => {
  it("decodes only the session id consumed by the adapter", () => {
    assert.deepStrictEqual(
      decodeInitializeSessionResult({
        sessionId: "session-1",
        session: "reshaped-by-a-newer-cli",
        settings: null,
      }),
      { sessionId: "session-1" },
    );
  });
});

describe("DroidLoadSessionResult", () => {
  it("decodes only resumed-session usage consumed by the adapter", () => {
    const decoded = decodeLoadSessionResult({
      session: "reshaped-by-a-newer-cli",
      settings: null,
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

describe("DroidExecuteRewindResult", () => {
  it("decodes only the successor session id consumed by the adapter", () => {
    assert.deepStrictEqual(
      decodeExecuteRewindResult({
        newSessionId: "session-rewound",
        restoredCount: "reshaped-by-a-newer-cli",
      }),
      { newSessionId: "session-rewound" },
    );
  });
});

describe("DroidPermissionRequest", () => {
  it("rejects permission requests with no tool to classify or render", () => {
    assert.throws(() =>
      decodePermissionRequest({
        toolUses: [],
        options: [{ label: "Allow once", value: "proceed_once" }],
      }),
    );
  });

  it("decodes the canonical option and only permission detail fields the adapter consumes", () => {
    const decoded = decodePermissionRequest({
      toolUses: [
        {
          toolUse: {
            type: "tool_use",
            id: "tool-exec",
            input: { command: "echo hello" },
            name: "Execute",
          },
          confirmationType: "exec",
          details: {
            type: "exec",
            fullCommand: "echo hello",
            command: "echo",
            extractedCommands: "reshaped-by-a-newer-cli",
            impactLevel: { future: true },
          },
        },
      ],
      options: [{ label: "Allow once", value: "proceed_once" }],
      associatedSessionIds: "reshaped-by-a-newer-cli",
    });

    assert.deepStrictEqual(
      {
        toolUses: decoded.toolUses,
        options: decoded.options,
      },
      {
        toolUses: [
          {
            toolUse: {
              id: "tool-exec",
              input: { command: "echo hello" },
              name: "Execute",
            },
            details: {
              type: "exec",
              fullCommand: "echo hello",
              command: "echo",
            },
          },
        ],
        options: [{ label: "Allow once", outcome: "proceed_once" }],
      },
    );
  });
});

describe("Droid inventory entries", () => {
  it("requires the model metadata guaranteed by list_models", () => {
    assert.throws(() =>
      decodeModelInfo({
        id: "mock-fast",
        displayName: "Mock Fast",
      }),
    );
  });

  it("requires the skill location guaranteed by list_skills", () => {
    assert.throws(() =>
      decodeSkillInfo({
        name: "verify",
        filePath: "/skills/verify/SKILL.md",
      }),
    );
  });
});

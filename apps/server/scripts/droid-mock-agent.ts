#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeReadline from "node:readline";

const emitToolCall = process.env.T3_DROID_MOCK_EMIT_TOOL_CALL === "1";
const requestPermission = process.env.T3_DROID_MOCK_REQUEST_PERMISSION === "1";
const askUser = process.env.T3_DROID_MOCK_ASK_USER === "1";
const hangTurn = process.env.T3_DROID_MOCK_HANG_TURN === "1";
const failInit = process.env.T3_DROID_MOCK_FAIL_INIT === "1";
const failUpdateSettings = process.env.T3_DROID_MOCK_FAIL_UPDATE_SETTINGS === "1";
const loadInSpecMode = process.env.T3_DROID_MOCK_LOAD_IN_SPEC_MODE === "1";
const loadSteeringMessages = process.env.T3_DROID_MOCK_LOAD_STEERING_MESSAGES === "1";
const exitMidTurn = process.env.T3_DROID_MOCK_EXIT_MID_TURN === "1";
const emitUnknownNotification = process.env.T3_DROID_MOCK_EMIT_UNKNOWN_NOTIFICATION === "1";
const omitUsageNotification = process.env.T3_DROID_MOCK_OMIT_USAGE_NOTIFICATION === "1";

const initializedSessionId = "mock-session-1";
const knownLoadSessionId = "mock-session-known";
const rewoundSessionId = "mock-session-rewound";
const specSuccessorSessionId = "mock-session-spec-successor";
const childSessionId = "mock-session-child";
let currentSessionId = initializedSessionId;
let previousSessionId: string | undefined;
let emitPostLoadStraggler = false;
let serverRequestId = 0;
let currentSettings = {
  modelId: "mock-fast",
  reasoningEffort: "medium",
  interactionMode: "auto",
  autonomyLevel: "off",
};
let activeTurn:
  | {
      readonly turnId: string;
      completed: boolean;
    }
  | undefined;

const pendingServerRequests = new Map<
  string,
  {
    readonly resolve: (result: unknown) => void;
    readonly reject: (error: Error) => void;
  }
>();

const models = [
  {
    id: "mock-fast",
    displayName: "Mock Fast",
    shortDisplayName: "Fast",
    modelProvider: "factory",
    supportedReasoningEfforts: ["low", "medium", "high"],
    defaultReasoningEffort: "medium",
    isCustom: false,
  },
  {
    id: "mock-deep",
    displayName: "Mock Deep",
    shortDisplayName: "Deep",
    modelProvider: "factory",
    supportedReasoningEfforts: ["medium", "high", "xhigh"],
    defaultReasoningEffort: "high",
    isCustom: false,
  },
];

const tokenUsage = {
  inputTokens: 20,
  outputTokens: 8,
  cacheCreationTokens: 1,
  cacheReadTokens: 4,
  thinkingTokens: 3,
};

function write(message: Record<string, unknown>): void {
  process.stdout.write(
    `${JSON.stringify({
      jsonrpc: "2.0",
      factoryApiVersion: "1.0.0",
      ...message,
    })}\n`,
  );
}

function respond(id: string | number | null, result: unknown): void {
  write({ type: "response", id, result });
}

function fail(id: string | number | null, code: number, message: string): void {
  write({ type: "response", id, error: { code, message } });
}

function notifyForSession(sessionId: string, notification: Record<string, unknown>): void {
  write({
    type: "notification",
    method: "droid.session_notification",
    params: { sessionId, notification },
  });
}

function notify(notification: Record<string, unknown>): void {
  notifyForSession(currentSessionId, notification);
}

function requestClient(method: string, params: unknown): Promise<unknown> {
  const id = `server-${++serverRequestId}`;
  write({ type: "request", id, method, params });
  return new Promise((resolve, reject) => {
    pendingServerRequests.set(id, { resolve, reject });
  });
}

function initializeResult() {
  return {
    sessionId: currentSessionId,
    session: { messages: [] },
    availableModels: models,
    settings: {
      ...currentSettings,
      availableAutonomyLevels: ["off", "low", "medium", "high"],
    },
  };
}

function emitTerminalForSession(sessionId: string, reason: string, turnId: string): void {
  notifyForSession(sessionId, {
    type: "agent_turn_completed",
    reason,
    turnId,
    tokenUsage,
    cumulativeTokenUsage: tokenUsage,
    durationMs: 10,
  });
}

function emitTurnCompleted(reason: string, turnId: string): void {
  if (!activeTurn || activeTurn.turnId !== turnId || activeTurn.completed) {
    return;
  }
  activeTurn.completed = true;
  if (!omitUsageNotification) {
    notify({
      type: "session_token_usage_changed",
      sessionId: currentSessionId,
      tokenUsage,
      inclusiveTokenUsage: tokenUsage,
      lastCallTokenUsage: {
        inputTokens: 7,
        cacheReadTokens: 2,
        outputTokens: 3,
      },
    });
  }
  emitTerminalForSession(currentSessionId, reason, turnId);
  notify({
    type: "droid_working_state_changed",
    newState: "idle",
  });
  activeTurn = undefined;
}

async function runTurn(params: {
  readonly messageId: string;
  readonly text: string;
}): Promise<void> {
  const turnId = params.messageId;
  activeTurn = { turnId, completed: false };

  if (emitPostLoadStraggler && previousSessionId !== undefined) {
    emitPostLoadStraggler = false;
    notifyForSession(previousSessionId, {
      type: "assistant_text_delta",
      messageId: `assistant-stale-${turnId}`,
      blockIndex: 0,
      textDelta: "stale pre-rewind output",
    });
  }

  notify({ type: "droid_working_state_changed", newState: "thinking" });
  notify({
    type: "thinking_text_delta",
    messageId: `assistant-${turnId}`,
    blockIndex: 0,
    textDelta: "Mock thinking",
  });

  if (exitMidTurn) {
    process.exit(7);
  }

  if (emitUnknownNotification) {
    notify({
      type: "future_mock_notification",
      futurePayload: { supported: true },
    });
  }

  if (params.text === "mock spec handoff") {
    notifyForSession(specSuccessorSessionId, {
      type: "assistant_text_delta",
      messageId: `assistant-successor-${turnId}`,
      blockIndex: 0,
      textDelta: "implementation successor",
    });
    notifyForSession(specSuccessorSessionId, {
      type: "assistant_text_complete",
      messageId: `assistant-successor-${turnId}`,
      blockIndex: 0,
    });
    emitTerminalForSession(specSuccessorSessionId, "completed", `successor-${turnId}`);
    emitTurnCompleted("spec_handoff", turnId);
    return;
  }

  if (params.text === "mock compaction") {
    notify({
      type: "session_compacted",
      summaryId: "mock-summary-1",
      removedCount: 3,
      visibleBoundaryMessageId: null,
    });
    notify({
      type: "session_token_usage_changed",
      sessionId: currentSessionId,
      tokenUsage,
      inclusiveTokenUsage: tokenUsage,
      lastCallTokenUsage: {
        inputTokens: 5,
        cacheReadTokens: 1,
        outputTokens: 2,
      },
    });
  }

  if (params.text === "mock child session") {
    notify({
      type: "child_session_available",
      childSessionId,
      description: "Mock delegated task",
      timestamp: 1,
    });
    notify({
      type: "tool_progress_update",
      toolUseId: `child-task-${turnId}`,
      toolName: "Task",
      update: {
        type: "message",
        text: "Inspecting delegated files",
        subagentSessionId: childSessionId,
      },
    });
    notifyForSession(childSessionId, {
      type: "assistant_text_delta",
      messageId: `assistant-child-${turnId}`,
      blockIndex: 0,
      textDelta: "child-only output",
    });
    emitTerminalForSession(childSessionId, "completed", `child-${turnId}`);
  }

  if (params.text === "mock taskless progress") {
    notify({
      type: "tool_progress_update",
      toolUseId: `parent-tool-${turnId}`,
      toolName: "Execute",
      update: {
        type: "status",
        status: "running",
      },
    });
  }

  if (params.text === "mock report interaction mode") {
    notify({
      type: "thinking_text_complete",
      messageId: `assistant-${turnId}`,
      blockIndex: 0,
      durationMs: 5,
    });
    notify({
      type: "assistant_text_delta",
      messageId: `assistant-${turnId}`,
      blockIndex: 1,
      textDelta: currentSettings.interactionMode,
    });
    notify({
      type: "assistant_text_complete",
      messageId: `assistant-${turnId}`,
      blockIndex: 1,
    });
    emitTurnCompleted("completed", turnId);
    return;
  }

  if (params.text === "mock incomplete items") {
    notify({
      type: "assistant_text_delta",
      messageId: `assistant-${turnId}`,
      blockIndex: 1,
      textDelta: "terminal without item completions",
    });
    notify({
      type: "tool_call",
      toolUse: {
        type: "tool_use",
        id: `incomplete-tool-${turnId}`,
        input: { command: "echo incomplete" },
        name: "Execute",
      },
    });
    emitTurnCompleted("completed", turnId);
    return;
  }

  if (params.text === "mock delayed shared tool") {
    notify({
      type: "tool_call",
      toolUse: {
        type: "tool_use",
        id: "shared-tool-use",
        input: { path: "README.md" },
        name: "Read",
      },
    });
    return;
  }

  if (params.text === "mock shared tool execute") {
    notify({
      type: "tool_call",
      toolUse: {
        type: "tool_use",
        id: "shared-tool-use",
        input: { command: "echo shared" },
        name: "Execute",
      },
    });
    notify({
      type: "tool_result",
      messageId: `assistant-${turnId}`,
      toolUseId: "shared-tool-use",
      content: [{ type: "text", text: "shared command output" }],
    });
  }

  if (params.text === "mock steering original") {
    notify({
      type: "tool_call",
      toolUse: {
        type: "tool_use",
        id: `steering-tool-${turnId}`,
        input: { command: "echo steering" },
        name: "Execute",
      },
    });
    return;
  }

  if (requestPermission) {
    const result = (await requestClient("droid.request_permission", {
      toolUses: [
        {
          toolUse: {
            type: "tool_use",
            id: `permission-tool-${turnId}`,
            input: { command: "echo mock" },
            name: "Execute",
          },
          confirmationType: "exec",
          details: {
            type: "exec",
            fullCommand: "echo mock",
            command: "echo",
            impactLevel: "low",
            riskLevelReason: "The mock command only prints text.",
          },
        },
      ],
      options: [
        { label: "Allow once", value: "proceed_once" },
        { label: "Deny", value: "cancel" },
      ],
    })) as { selectedOption?: unknown };
    notify({
      type: "permission_resolved",
      requestId: `permission-${turnId}`,
      toolUseIds: [`permission-tool-${turnId}`],
      selectedOption: typeof result.selectedOption === "string" ? result.selectedOption : "cancel",
    });
    if (result.selectedOption === "cancel") {
      emitTurnCompleted("permission_rejected", turnId);
      return;
    }
  }

  if (askUser) {
    await requestClient("droid.ask_user", {
      toolCallId: `ask-${turnId}`,
      questions: [
        {
          index: 1,
          topic: "Scope",
          question: "Which scope?",
          options: ["workspace", "session"],
        },
      ],
    });
  }

  if (emitToolCall) {
    notify({
      type: "tool_call",
      toolUse: {
        type: "tool_use",
        id: `tool-${turnId}`,
        input: { path: "README.md" },
        name: "Read",
      },
    });
    notify({
      type: "tool_result",
      messageId: `assistant-${turnId}`,
      toolUseId: `tool-${turnId}`,
      content: [{ type: "text", text: "mock file contents" }],
    });
  }

  notify({
    type: "thinking_text_complete",
    messageId: `assistant-${turnId}`,
    blockIndex: 0,
    durationMs: 5,
  });
  notify({
    type: "assistant_text_delta",
    messageId: `assistant-${turnId}`,
    blockIndex: 1,
    textDelta: "hello from ",
  });
  notify({
    type: "assistant_text_delta",
    messageId: `assistant-${turnId}`,
    blockIndex: 1,
    textDelta: "droid mock",
  });
  notify({
    type: "assistant_text_complete",
    messageId: `assistant-${turnId}`,
    blockIndex: 1,
  });

  if (hangTurn) {
    return;
  }
  emitTurnCompleted("completed", turnId);
}

async function handleRequest(message: {
  readonly id: string | number | null;
  readonly method: string;
  readonly params?: unknown;
}): Promise<void> {
  switch (message.method) {
    case "droid.initialize_session":
      if (failInit) {
        fail(message.id, -32603, "Mock initialization failure");
      } else {
        previousSessionId = undefined;
        currentSessionId = initializedSessionId;
        respond(message.id, initializeResult());
      }
      return;
    case "droid.load_session":
      if (
        typeof message.params !== "object" ||
        message.params === null ||
        !("sessionId" in message.params) ||
        typeof message.params.sessionId !== "string" ||
        message.params.sessionId.length === 0
      ) {
        fail(message.id, -32602, "load_session requires sessionId");
        return;
      }
      if (
        message.params.sessionId !== knownLoadSessionId &&
        message.params.sessionId !== rewoundSessionId
      ) {
        fail(message.id, -32004, "Mock session not found");
        return;
      }
      previousSessionId = currentSessionId;
      currentSessionId = message.params.sessionId;
      if (loadInSpecMode) {
        currentSettings = { ...currentSettings, interactionMode: "spec" };
      }
      if (currentSessionId === rewoundSessionId) {
        emitPostLoadStraggler = true;
      }
      respond(message.id, {
        session: {
          title: "Loaded mock session",
          messages: loadSteeringMessages
            ? [
                {
                  id: "loaded-opening-user",
                  role: "user",
                  content: [{ type: "text", text: "loaded opening prompt" }],
                },
                {
                  id: "loaded-steer-user",
                  role: "user",
                  content: [{ type: "text", text: "loaded steer" }],
                },
                {
                  id: "loaded-assistant-1",
                  role: "assistant",
                  content: [{ type: "text", text: "loaded response" }],
                },
              ]
            : [
                {
                  id: "loaded-user-1",
                  role: "user",
                  content: [{ type: "text", text: "loaded prompt" }],
                },
                {
                  id: "loaded-assistant-1",
                  role: "assistant",
                  content: [{ type: "text", text: "loaded response" }],
                },
              ],
        },
        settings: {
          ...currentSettings,
          ...(failUpdateSettings ? { autonomyLevel: "high" } : {}),
          availableAutonomyLevels: ["off", "low", "medium", "high"],
        },
        availableModels: models,
        tokenUsage,
      });
      return;
    case "droid.add_user_message": {
      const params =
        typeof message.params === "object" && message.params !== null
          ? (message.params as { messageId?: unknown; text?: unknown })
          : {};
      if (
        typeof params.messageId !== "string" ||
        params.messageId.length === 0 ||
        typeof params.text !== "string"
      ) {
        fail(message.id, -32602, "add_user_message requires messageId and text");
        return;
      }
      respond(message.id, {});
      if (
        params.text === "mock release shared tool" &&
        activeTurn !== undefined &&
        !activeTurn.completed
      ) {
        const openingTurnId = activeTurn.turnId;
        notify({
          type: "create_message",
          message: {
            id: params.messageId,
            role: "user",
            content: [{ type: "text", text: params.text }],
          },
        });
        notify({
          type: "tool_result",
          messageId: `assistant-${openingTurnId}`,
          toolUseId: "shared-tool-use",
          content: [{ type: "text", text: "shared file contents" }],
        });
        emitTurnCompleted("completed", openingTurnId);
        return;
      }
      if (
        params.text === "mock steering coalesced" &&
        activeTurn !== undefined &&
        !activeTurn.completed
      ) {
        const openingTurnId = activeTurn.turnId;
        notify({
          type: "create_message",
          message: {
            id: params.messageId,
            role: "user",
            content: [{ type: "text", text: params.text }],
          },
        });
        notify({
          type: "assistant_text_delta",
          messageId: `assistant-${openingTurnId}`,
          blockIndex: 1,
          textDelta: "steered output",
        });
        notify({
          type: "assistant_text_complete",
          messageId: `assistant-${openingTurnId}`,
          blockIndex: 1,
        });
        emitTurnCompleted("completed", openingTurnId);
        return;
      }
      if (
        params.text === "mock steering separate" &&
        activeTurn !== undefined &&
        !activeTurn.completed
      ) {
        emitTurnCompleted("completed", activeTurn.turnId);
        void runTurn({ messageId: params.messageId, text: params.text });
        return;
      }
      void runTurn({ messageId: params.messageId, text: params.text });
      return;
    }
    case "droid.interrupt_session":
      if (activeTurn) {
        emitTurnCompleted(
          process.env.T3_DROID_MOCK_INTERRUPT_RACE === "1" ? "completed" : "cancelled",
          activeTurn.turnId,
        );
      }
      respond(message.id, {});
      return;
    case "droid.update_session_settings":
      if (failUpdateSettings) {
        fail(message.id, -32603, "Mock settings update failure");
        return;
      }
      if (typeof message.params === "object" && message.params !== null) {
        currentSettings = { ...currentSettings, ...message.params };
      }
      respond(message.id, {});
      notify({
        type: "settings_updated",
        settings: currentSettings,
      });
      return;
    case "droid.list_models":
      respond(message.id, { models });
      return;
    case "droid.list_commands":
      respond(message.id, {
        commands: [
          { name: "review", description: "Review the current changes", argumentHint: "[path]" },
        ],
      });
      return;
    case "droid.list_skills":
      respond(message.id, {
        skills: [
          {
            name: "mock-skill",
            description: "A mock skill",
            location: "user",
            filePath: "/mock/SKILL.md",
            enabled: true,
          },
        ],
        projectAvailable: true,
      });
      return;
    case "droid.execute_rewind":
      respond(message.id, {
        newSessionId: rewoundSessionId,
        restoredCount: 1,
        deletedCount: 1,
        failedRestoreCount: 0,
        failedDeleteCount: 0,
      });
      return;
    default:
      fail(message.id, -32601, `Unknown mock method: ${message.method}`);
  }
}

function handleMessage(raw: unknown): void {
  if (typeof raw !== "object" || raw === null) {
    return;
  }
  const message = raw as {
    readonly type?: unknown;
    readonly id?: unknown;
    readonly method?: unknown;
    readonly params?: unknown;
    readonly result?: unknown;
    readonly error?: unknown;
  };
  if (
    message.type === "response" &&
    (typeof message.id === "string" || typeof message.id === "number")
  ) {
    const pending = pendingServerRequests.get(String(message.id));
    if (!pending) {
      return;
    }
    pendingServerRequests.delete(String(message.id));
    if (typeof message.error === "object" && message.error !== null) {
      pending.reject(new Error(JSON.stringify(message.error)));
    } else {
      pending.resolve(message.result);
    }
    return;
  }
  if (
    message.type === "request" &&
    (typeof message.id === "string" || typeof message.id === "number" || message.id === null) &&
    typeof message.method === "string"
  ) {
    void handleRequest({
      id: message.id,
      method: message.method,
      params: message.params,
    });
  }
}

const input = NodeReadline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

input.on("line", (line) => {
  if (line.trim().length === 0) {
    return;
  }
  handleMessage(JSON.parse(line));
});

input.once("close", () => {
  process.exit(0);
});

process.once("SIGTERM", () => {
  process.exit(0);
});

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

// factory-mono: protocol/session/settings/schema.ts
export const DroidTokenUsage = Schema.Struct({
  inputTokens: Schema.Number,
  outputTokens: Schema.Number,
  cacheCreationTokens: Schema.Number,
  cacheReadTokens: Schema.Number,
  thinkingTokens: Schema.Number,
  factoryCredits: Schema.optional(Schema.Number),
});
export type DroidTokenUsage = typeof DroidTokenUsage.Type;

export const DroidLastCallTokenUsage = Schema.Struct({
  inputTokens: Schema.Number,
  cacheReadTokens: Schema.Number,
  outputTokens: Schema.optional(Schema.Number),
});
export type DroidLastCallTokenUsage = typeof DroidLastCallTokenUsage.Type;

// factory-mono: protocol/llm/enums.ts
export const DroidReasoningEffort = Schema.Literals([
  "none",
  "dynamic",
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);
export type DroidReasoningEffort = typeof DroidReasoningEffort.Type;

// factory-mono: protocol/shared/enums.ts
export const DroidAutonomyLevel = Schema.Literals(["off", "low", "medium", "high"]);
export type DroidAutonomyLevel = typeof DroidAutonomyLevel.Type;

export const DroidInteractionMode = Schema.Literals(["auto", "spec"]);
export type DroidInteractionMode = typeof DroidInteractionMode.Type;

// factory-mono: protocol/models/schemas.ts
export const DroidModelInfo = Schema.Struct({
  id: Schema.String,
  displayName: Schema.String,
  shortDisplayName: Schema.optional(Schema.String),
  modelProvider: Schema.optional(Schema.String),
  supportedReasoningEfforts: Schema.optional(Schema.Array(DroidReasoningEffort)),
  defaultReasoningEffort: Schema.optional(DroidReasoningEffort),
  isCustom: Schema.optional(Schema.Boolean),
  noImageSupport: Schema.optional(Schema.Boolean),
  disabled: Schema.optional(Schema.Boolean),
});
export type DroidModelInfo = typeof DroidModelInfo.Type;

const DroidSessionSettings = Schema.Struct({
  modelId: Schema.String,
  reasoningEffort: DroidReasoningEffort,
  interactionMode: Schema.optional(DroidInteractionMode),
  autonomyLevel: Schema.optional(DroidAutonomyLevel),
  availableAutonomyLevels: Schema.optional(Schema.Array(DroidAutonomyLevel)),
  specModeModelId: Schema.optional(Schema.String),
  specModeReasoningEffort: Schema.optional(DroidReasoningEffort),
});

// factory-mono: protocol/droid/schemas/client.ts
export const DroidInitializeSessionResult = Schema.Struct({
  sessionId: Schema.String,
  session: Schema.optional(
    Schema.Struct({
      messages: Schema.Array(Schema.Unknown),
      title: Schema.optional(Schema.String),
    }),
  ),
  availableModels: Schema.optional(Schema.Array(DroidModelInfo)),
  settings: DroidSessionSettings,
});
export type DroidInitializeSessionResult = typeof DroidInitializeSessionResult.Type;

const DroidPendingPermission = Schema.Struct({
  requestId: Schema.String,
  toolUses: Schema.Array(Schema.Unknown),
  options: Schema.Array(Schema.Unknown),
});

const DroidPendingAskUser = Schema.Struct({
  requestId: Schema.String,
  toolCallId: Schema.String,
  questions: Schema.Array(Schema.Unknown),
});

export const DroidLoadSessionResult = Schema.Struct({
  session: Schema.Struct({
    messages: Schema.Array(Schema.Unknown),
    title: Schema.optional(Schema.String),
  }),
  settings: DroidSessionSettings,
  pendingPermissions: Schema.optional(Schema.Array(DroidPendingPermission)),
  pendingAskUserRequests: Schema.optional(Schema.Array(DroidPendingAskUser)),
  tokenUsage: Schema.optional(DroidTokenUsage),
  inclusiveTokenUsage: Schema.optional(DroidTokenUsage),
  lastCallTokenUsage: Schema.optional(DroidLastCallTokenUsage),
  availableModels: Schema.optional(Schema.Array(DroidModelInfo)),
});
export type DroidLoadSessionResult = typeof DroidLoadSessionResult.Type;

// factory-mono: protocol/sessionV2/messages/schemas.ts
export const DroidBase64ImageSource = Schema.Struct({
  type: Schema.Literal("base64"),
  data: Schema.String,
  mediaType: Schema.Literals(["image/jpeg", "image/png", "image/gif", "image/webp"]),
});
export type DroidBase64ImageSource = typeof DroidBase64ImageSource.Type;

const DroidMcpOAuthOptions = Schema.Struct({
  scopes: Schema.optional(Schema.Array(Schema.String)),
  resource: Schema.optional(Schema.Union([Schema.String, Schema.Literal(false)])),
  authorizationServerIssuer: Schema.optional(Schema.String),
  clientMetadataUrl: Schema.optional(Schema.String),
  clientId: Schema.optional(Schema.String),
  clientSecret: Schema.optional(Schema.String),
  callbackPort: Schema.optional(Schema.Number),
  tokenEndpointAuthMethod: Schema.optional(
    Schema.Literals(["none", "client_secret_basic", "client_secret_post"]),
  ),
});

const DroidMcpOAuthConfig = Schema.Union([Schema.Literal(false), DroidMcpOAuthOptions]);

const DroidMcpHeader = Schema.Struct({
  name: Schema.String,
  value: Schema.String,
});

export const DroidMcpServerConfig = Schema.Union([
  Schema.Struct({
    name: Schema.String,
    command: Schema.String,
    args: Schema.optional(Schema.Array(Schema.String)),
    env: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.Struct({
    type: Schema.Literal("http"),
    name: Schema.String,
    url: Schema.String,
    headers: Schema.optional(Schema.Array(DroidMcpHeader)),
    oauth: Schema.optional(DroidMcpOAuthConfig),
  }),
  Schema.Struct({
    type: Schema.Literal("sse"),
    name: Schema.String,
    url: Schema.String,
    headers: Schema.optional(Schema.Array(DroidMcpHeader)),
    oauth: Schema.optional(DroidMcpOAuthConfig),
  }),
]);
export type DroidMcpServerConfig = typeof DroidMcpServerConfig.Type;

// factory-mono: protocol/droid/schemas/client.ts
export const DroidInitializeSessionParams = Schema.Struct({
  machineId: Schema.String,
  cwd: Schema.String,
  sessionId: Schema.optional(Schema.String),
  mcpServers: Schema.optional(Schema.Array(DroidMcpServerConfig)),
  interactionMode: Schema.optional(DroidInteractionMode),
  autonomyLevel: Schema.optional(DroidAutonomyLevel),
  modelId: Schema.optional(Schema.String),
  reasoningEffort: Schema.optional(DroidReasoningEffort),
  specModeModelId: Schema.optional(Schema.String),
  specModeReasoningEffort: Schema.optional(DroidReasoningEffort),
  systemPrompt: Schema.optional(Schema.Unknown),
  title: Schema.optional(Schema.String),
  skipPermissionsUnsafe: Schema.optional(Schema.Boolean),
  autoRejectPermissionRequests: Schema.optional(Schema.Boolean),
  disableBuiltinSkills: Schema.optional(Schema.Boolean),
  additionalToolIds: Schema.optional(Schema.Array(Schema.String)),
  enabledToolIds: Schema.optional(Schema.Array(Schema.String)),
  disabledToolIds: Schema.optional(Schema.Array(Schema.String)),
  restrictToolIds: Schema.optional(Schema.Array(Schema.String)),
});
export type DroidInitializeSessionParams = typeof DroidInitializeSessionParams.Type;

export const DroidLoadSessionParams = Schema.Struct({
  sessionId: Schema.String,
  mcpServers: Schema.optional(Schema.Array(DroidMcpServerConfig)),
  loadAllMessages: Schema.optional(Schema.Boolean),
  messageLimit: Schema.optional(Schema.Number),
  autoRejectPermissionRequests: Schema.optional(Schema.Boolean),
  disableBuiltinSkills: Schema.optional(Schema.Boolean),
  additionalToolIds: Schema.optional(Schema.Array(Schema.String)),
  enabledToolIds: Schema.optional(Schema.Array(Schema.String)),
  disabledToolIds: Schema.optional(Schema.Array(Schema.String)),
});
export type DroidLoadSessionParams = typeof DroidLoadSessionParams.Type;

export const DroidAddUserMessageParams = Schema.Struct({
  messageId: Schema.optional(Schema.String),
  text: Schema.String,
  images: Schema.optional(Schema.Array(DroidBase64ImageSource)),
  imagePaths: Schema.optional(Schema.Array(Schema.String)),
  outputFormat: Schema.optional(Schema.Unknown),
  skipAgentLoop: Schema.optional(Schema.Boolean),
  queuePlacement: Schema.optional(Schema.Literals(["end_of_turn", "end_of_loop"])),
});
export type DroidAddUserMessageParams = typeof DroidAddUserMessageParams.Type;

// factory-mono: protocol/droid/schemas/client.ts
const DroidRewindFileSnapshot = Schema.Struct({
  filePath: Schema.String,
  contentHash: Schema.String,
  size: Schema.Number,
});

const DroidRewindFileCreation = Schema.Struct({
  filePath: Schema.String,
});

const DroidRewindEvictedFile = Schema.Struct({
  filePath: Schema.String,
  reason: Schema.String,
});

export const DroidRewindInfo = Schema.Struct({
  availableFiles: Schema.Array(DroidRewindFileSnapshot),
  createdFiles: Schema.Array(DroidRewindFileCreation),
  evictedFiles: Schema.Array(DroidRewindEvictedFile),
});
export type DroidRewindInfo = typeof DroidRewindInfo.Type;

export const DroidExecuteRewindResult = Schema.Struct({
  newSessionId: Schema.String,
  restoredCount: Schema.Number,
  deletedCount: Schema.Number,
  failedRestoreCount: Schema.Number,
  failedDeleteCount: Schema.Number,
});
export type DroidExecuteRewindResult = typeof DroidExecuteRewindResult.Type;

export const DroidCommandInfo = Schema.Struct({
  name: Schema.String,
  description: Schema.String,
  argumentHint: Schema.optional(Schema.String),
  isExecutable: Schema.optional(Schema.Boolean),
});
export type DroidCommandInfo = typeof DroidCommandInfo.Type;

export const DroidSkillInfo = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
  filePath: Schema.String,
  enabled: Schema.optional(Schema.Boolean),
  userInvocable: Schema.optional(Schema.Boolean),
  version: Schema.optional(Schema.String),
});
export type DroidSkillInfo = typeof DroidSkillInfo.Type;

// factory-mono: protocol/droid/schemas/cli.ts
export const DroidToolUse = Schema.Struct({
  type: Schema.Literal("tool_use"),
  id: Schema.String,
  input: Schema.Record(Schema.String, Schema.Unknown),
  name: Schema.String,
});
export type DroidToolUse = typeof DroidToolUse.Type;

const DroidToolResultContentBlock = Schema.Struct({
  type: Schema.String,
  text: Schema.optional(Schema.String),
  source: Schema.optional(Schema.Unknown),
});

const DroidAskUserQuestion = Schema.Struct({
  index: Schema.Number,
  topic: Schema.String,
  question: Schema.String,
  options: Schema.Array(Schema.String),
  multiSelect: Schema.optional(Schema.Boolean),
});

const DroidEditToolConfirmationDetails = Schema.Struct({
  type: Schema.Literal("edit"),
  filePath: Schema.String,
  fileName: Schema.String,
  oldContent: Schema.optional(Schema.String),
  newContent: Schema.optional(Schema.String),
});

const DroidExecuteToolConfirmationDetails = Schema.Struct({
  type: Schema.Literal("exec"),
  fullCommand: Schema.String,
  command: Schema.String,
  extractedCommands: Schema.optional(Schema.Array(Schema.String)),
  impactLevel: Schema.optional(Schema.String),
  riskLevelReason: Schema.optional(Schema.String),
});

const DroidCreateToolConfirmationDetails = Schema.Struct({
  type: Schema.Literal("create"),
  filePath: Schema.String,
  fileName: Schema.String,
  content: Schema.String,
});

const DroidAskUserConfirmationDetails = Schema.Struct({
  type: Schema.Literal("ask_user"),
  questionnaire: Schema.String,
  parsed: Schema.optional(
    Schema.Struct({
      questions: Schema.Array(DroidAskUserQuestion),
    }),
  ),
  parseError: Schema.optional(
    Schema.Struct({
      message: Schema.String,
      line: Schema.optional(Schema.Number),
    }),
  ),
});

const DroidExitSpecModeConfirmationDetails = Schema.Struct({
  type: Schema.Literal("exit_spec_mode"),
  plan: Schema.String,
  title: Schema.optional(Schema.String),
});

const DroidProposeMissionConfirmationDetails = Schema.Struct({
  type: Schema.Literal("propose_mission"),
  proposal: Schema.String,
  title: Schema.optional(Schema.String),
});

const DroidStartMissionRunConfirmationDetails = Schema.Struct({
  type: Schema.Literal("start_mission_run"),
  runningMissionCount: Schema.Number,
  runningMissionSessionIds: Schema.Array(Schema.String),
});

const DroidApplyPatchFileConfirmation = Schema.Struct({
  filePath: Schema.String,
  fileName: Schema.String,
  operation: Schema.Literals(["create", "update", "delete"]),
  moveTo: Schema.optional(Schema.String),
  oldContent: Schema.optional(Schema.String),
  newContent: Schema.optional(Schema.String),
});

const DroidApplyPatchToolConfirmationDetails = Schema.Struct({
  type: Schema.Literal("apply_patch"),
  filePath: Schema.String,
  fileName: Schema.String,
  patchContent: Schema.String,
  oldContent: Schema.optional(Schema.String),
  newContent: Schema.optional(Schema.String),
  files: Schema.optional(Schema.Array(DroidApplyPatchFileConfirmation)),
});

const DroidMcpToolConfirmationDetails = Schema.Struct({
  type: Schema.Literal("mcp_tool"),
  toolName: Schema.String,
  impactLevel: Schema.String,
  serverName: Schema.optional(Schema.String),
  actualToolName: Schema.optional(Schema.String),
});

const DroidSandboxViolationConfirmationDetails = Schema.Struct({
  type: Schema.Literal("sandbox_violation"),
  violatingToolName: Schema.String,
  target: Schema.String,
  operationType: Schema.Literals(["read", "write", "network", "tool"]),
  violationType: Schema.Literals(["filesystem-read", "filesystem-write", "network", "tool"]),
  reason: Schema.String,
  violationReason: Schema.optional(Schema.Literals(["deny-list", "not-allowed"])),
  isOrgDeny: Schema.Boolean,
});

const DroidShieldViolationConfirmationDetails = Schema.Struct({
  type: Schema.Literal("droid_shield_violation"),
  command: Schema.String,
  reason: Schema.String,
});

export const DroidToolConfirmationDetails = Schema.Union([
  DroidEditToolConfirmationDetails,
  DroidExecuteToolConfirmationDetails,
  DroidCreateToolConfirmationDetails,
  DroidAskUserConfirmationDetails,
  DroidExitSpecModeConfirmationDetails,
  DroidProposeMissionConfirmationDetails,
  DroidStartMissionRunConfirmationDetails,
  DroidApplyPatchToolConfirmationDetails,
  DroidMcpToolConfirmationDetails,
  DroidSandboxViolationConfirmationDetails,
  DroidShieldViolationConfirmationDetails,
]);
export type DroidToolConfirmationDetails = typeof DroidToolConfirmationDetails.Type;

const DroidPermissionOptionFields = {
  label: Schema.optional(Schema.String),
  optionId: Schema.optional(Schema.String),
  outcome: Schema.String,
} as const;

export const DroidPermissionOption = Schema.Union([
  Schema.Struct(DroidPermissionOptionFields).pipe(
    Schema.encodeKeys({
      outcome: "value",
    }),
  ),
  Schema.Struct(DroidPermissionOptionFields),
]);
export type DroidPermissionOption = typeof DroidPermissionOption.Type;

const DroidPermissionRequestFields = {
  toolUses: Schema.Array(
    Schema.Struct({
      toolUse: DroidToolUse,
      confirmationType: Schema.Literals([
        "edit",
        "exec",
        "create",
        "ask_user",
        "exit_spec_mode",
        "propose_mission",
        "start_mission_run",
        "apply_patch",
        "mcp_tool",
        "sandbox_violation",
        "droid_shield_violation",
      ]),
      details: DroidToolConfirmationDetails,
    }),
  ),
  options: Schema.Array(DroidPermissionOption),
  associatedSessionIds: Schema.optional(Schema.Array(Schema.String)),
} as const;

const DroidPermissionRequestDecoded = Schema.Struct({
  ...DroidPermissionRequestFields,
  raw: Schema.Unknown,
});

const DroidPermissionRequestRaw = Schema.Record(Schema.String, Schema.Unknown);

export const DroidPermissionRequest = DroidPermissionRequestRaw.pipe(
  Schema.decodeTo(
    DroidPermissionRequestDecoded,
    SchemaTransformation.transformOrFail({
      decode: (raw) =>
        Effect.succeed({
          ...raw,
          raw,
        } as typeof DroidPermissionRequestDecoded.Encoded),
      encode: ({ raw: _raw, ...request }) =>
        Effect.succeed(request as typeof DroidPermissionRequestRaw.Encoded),
    }),
  ),
);
export type DroidPermissionRequest = typeof DroidPermissionRequest.Type;

export const DroidAskUserRequest = Schema.Struct({
  toolCallId: Schema.String,
  questions: Schema.Array(DroidAskUserQuestion),
});
export type DroidAskUserRequest = typeof DroidAskUserRequest.Type;

const AssistantTextDelta = Schema.Struct({
  type: Schema.Literal("assistant_text_delta"),
  messageId: Schema.String,
  blockIndex: Schema.Number,
  textDelta: Schema.String,
});

const AssistantTextComplete = Schema.Struct({
  type: Schema.Literal("assistant_text_complete"),
  messageId: Schema.String,
  blockIndex: Schema.Number,
});

const ThinkingTextDelta = Schema.Struct({
  type: Schema.Literal("thinking_text_delta"),
  messageId: Schema.String,
  blockIndex: Schema.Number,
  textDelta: Schema.String,
});

const ThinkingTextComplete = Schema.Struct({
  type: Schema.Literal("thinking_text_complete"),
  messageId: Schema.String,
  blockIndex: Schema.Number,
  durationMs: Schema.optional(Schema.Number),
});

const ToolCall = Schema.Struct({
  type: Schema.Literal("tool_call"),
  toolUse: DroidToolUse,
});

const ToolResult = Schema.Struct({
  type: Schema.Literal("tool_result"),
  messageId: Schema.String,
  toolUseId: Schema.String,
  content: Schema.optional(
    Schema.Union([Schema.String, Schema.Array(DroidToolResultContentBlock)]),
  ),
  isError: Schema.optional(Schema.Boolean),
});

// factory-mono: protocol/droid/schemas/cli.ts. `type` stays an open string: droid
// enumerates it, but nothing here reads it, and a kind added later must not cost us
// the subagent activity this notification carries.
const ToolProgressUpdatePayload = Schema.Struct({
  type: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  details: Schema.optional(Schema.String),
  text: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
  valueSnippet: Schema.optional(Schema.String),
  subagentSessionId: Schema.optional(Schema.String),
});

const ToolProgressUpdate = Schema.Struct({
  type: Schema.Literal("tool_progress_update"),
  toolUseId: Schema.String,
  toolName: Schema.String,
  update: ToolProgressUpdatePayload,
});

const ToolExecutionPhaseChanged = Schema.Struct({
  type: Schema.Literal("tool_execution_phase_changed"),
  toolUseId: Schema.String,
  toolName: Schema.String,
  phase: Schema.Literals([
    "streaming_input",
    "queued",
    "executing",
    "settled_after_execution",
    "settled_without_execution",
    "settled_unknown",
  ]),
});

const CreateMessage = Schema.Struct({
  type: Schema.Literal("create_message"),
  message: Schema.Unknown,
  parentId: Schema.optional(Schema.String),
  requestId: Schema.optional(Schema.String),
});

const DroidWorkingStateChanged = Schema.Struct({
  type: Schema.Literal("droid_working_state_changed"),
  newState: Schema.Literals([
    "idle",
    "thinking",
    "streaming_assistant_message",
    "waiting_for_tool_confirmation",
    "executing_tool",
    "compacting_conversation",
  ]),
});

export const DroidAgentTurnCompleted = Schema.Struct({
  type: Schema.Literal("agent_turn_completed"),
  reason: Schema.Literals([
    "completed",
    "cancelled",
    "permission_rejected",
    "error",
    "process_exit",
    "spec_handoff",
    "structured_output_missing",
    "structured_output_invalid",
    "structured_output_schema_invalid",
    "model_usage_exhausted",
    "model_authentication_failed",
    "model_request_rejected",
    "model_provider_unreachable",
    "model_provider_unavailable",
    "prompt_rejected",
    "completion_persistence_failed",
    "no_approver_available",
  ]),
  turnId: Schema.optional(Schema.String),
  tokenUsage: DroidTokenUsage,
  cumulativeTokenUsage: Schema.optional(DroidTokenUsage),
  childTokenUsage: Schema.optional(DroidTokenUsage),
  cumulativeChildTokenUsage: Schema.optional(DroidTokenUsage),
  durationMs: Schema.optional(Schema.Number),
});
export type DroidAgentTurnCompleted = typeof DroidAgentTurnCompleted.Type;

const SessionTokenUsageChanged = Schema.Struct({
  type: Schema.Literal("session_token_usage_changed"),
  sessionId: Schema.String,
  tokenUsage: DroidTokenUsage,
  inclusiveTokenUsage: Schema.optional(DroidTokenUsage),
  lastCallTokenUsage: Schema.optional(DroidLastCallTokenUsage),
});

const SessionCompacted = Schema.Struct({
  type: Schema.Literal("session_compacted"),
  summaryId: Schema.String,
  removedCount: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
  visibleBoundaryMessageId: Schema.NullOr(Schema.String),
});

const ErrorNotification = Schema.Struct({
  type: Schema.Literal("error"),
  message: Schema.String,
  errorType: Schema.String,
  timestamp: Schema.String,
  exitCode: Schema.optional(Schema.Number),
  error: Schema.optional(Schema.Unknown),
});

const LlmRetry = Schema.Struct({
  type: Schema.Literal("llm_retry"),
  attempt: Schema.Number,
  reason: Schema.String,
});

const SessionTitleUpdated = Schema.Struct({
  type: Schema.Literal("session_title_updated"),
  requestId: Schema.optional(Schema.String),
  title: Schema.String,
  updateType: Schema.optional(Schema.String),
});

const ChildSessionAvailable = Schema.Struct({
  type: Schema.Literal("child_session_available"),
  childSessionId: Schema.String,
  toolUseId: Schema.optional(Schema.String),
  subagentType: Schema.optional(Schema.String),
  description: Schema.optional(Schema.String),
  timestamp: Schema.Number,
});

const PermissionResolved = Schema.Struct({
  type: Schema.Literal("permission_resolved"),
  requestId: Schema.String,
  toolUseIds: Schema.Array(Schema.String),
  selectedOption: Schema.String,
});

const QueuedMessagesDiscarded = Schema.Struct({
  type: Schema.Literal("queued_messages_discarded"),
  text: Schema.String,
  requestId: Schema.optional(Schema.String),
});

const McpStatusChanged = Schema.Struct({
  type: Schema.Literal("mcp_status_changed"),
  servers: Schema.Array(Schema.Unknown),
  summary: Schema.Unknown,
});

const SettingsUpdated = Schema.Struct({
  type: Schema.Literal("settings_updated"),
  requestId: Schema.optional(Schema.String),
  settings: Schema.Struct({
    interactionMode: Schema.optional(DroidInteractionMode),
    autonomyLevel: Schema.optional(DroidAutonomyLevel),
    availableAutonomyLevels: Schema.optional(Schema.Array(DroidAutonomyLevel)),
    modelId: Schema.optional(Schema.String),
    reasoningEffort: Schema.optional(DroidReasoningEffort),
    specModeModelId: Schema.optional(Schema.String),
    specModeReasoningEffort: Schema.optional(DroidReasoningEffort),
  }),
});

const StructuredOutput = Schema.Struct({
  type: Schema.Literal("structured_output"),
  messageId: Schema.String,
  structuredOutput: Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown)),
});

const KnownDroidSessionNotification = Schema.Union([
  AssistantTextDelta,
  AssistantTextComplete,
  ThinkingTextDelta,
  ThinkingTextComplete,
  ToolCall,
  ToolResult,
  ToolProgressUpdate,
  ToolExecutionPhaseChanged,
  CreateMessage,
  DroidWorkingStateChanged,
  DroidAgentTurnCompleted,
  SessionTokenUsageChanged,
  SessionCompacted,
  ErrorNotification,
  LlmRetry,
  SessionTitleUpdated,
  ChildSessionAvailable,
  PermissionResolved,
  QueuedMessagesDiscarded,
  McpStatusChanged,
  SettingsUpdated,
  StructuredOutput,
]);

const knownNotificationTypes = new Set([
  "assistant_text_delta",
  "assistant_text_complete",
  "thinking_text_delta",
  "thinking_text_complete",
  "tool_call",
  "tool_result",
  "tool_progress_update",
  "tool_execution_phase_changed",
  "create_message",
  "droid_working_state_changed",
  "agent_turn_completed",
  "session_token_usage_changed",
  "session_compacted",
  "error",
  "llm_retry",
  "session_title_updated",
  "child_session_available",
  "permission_resolved",
  "queued_messages_discarded",
  "mcp_status_changed",
  "settings_updated",
  "structured_output",
]);

const DroidUnknownNotificationPayload = Schema.Record(Schema.String, Schema.Unknown).check(
  Schema.makeFilter(
    (input) =>
      (typeof input.type === "string" && !knownNotificationTypes.has(input.type)) ||
      "Expected an unknown Droid notification type",
  ),
);

export const DroidUnknownNotification = DroidUnknownNotificationPayload.pipe(
  Schema.decodeTo(
    Schema.Struct({
      type: Schema.Literal("__unknown__"),
      notificationType: Schema.String,
      payload: Schema.Record(Schema.String, Schema.Unknown),
    }),
    SchemaTransformation.transformOrFail({
      decode: (payload) =>
        Effect.succeed({
          type: "__unknown__" as const,
          notificationType: payload.type as string,
          payload,
        }),
      encode: (notification) =>
        Effect.succeed({
          ...notification.payload,
          type: notification.notificationType,
        }),
    }),
  ),
);
export type DroidUnknownNotification = typeof DroidUnknownNotification.Type;

export const DroidSessionNotification = Schema.Union([
  KnownDroidSessionNotification,
  DroidUnknownNotification,
]);
export type DroidSessionNotification = typeof DroidSessionNotification.Type;

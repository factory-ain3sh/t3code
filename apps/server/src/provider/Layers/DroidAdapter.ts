import {
  ApprovalRequestId,
  type CanonicalRequestType,
  type DroidSettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  type ThreadTokenUsageSnapshot,
  type ThreadId,
  type ToolLifecycleItemType,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  DroidAskUserRequest,
  DroidExecuteRewindResult,
  DroidInitializeSessionResult,
  DroidLoadSessionResult,
  DroidPermissionRequest,
  type DroidLastCallTokenUsage,
  type DroidPermissionOption,
  type DroidSessionNotification,
  type DroidTokenUsage,
  type DroidToolUse,
} from "../droid/DroidProtocol.ts";
import {
  makeDroidRpcClient,
  type DroidRpcClient,
  type DroidServerRequest,
} from "../droid/DroidRpcClient.ts";
import { type DroidAdapterShape } from "../Services/DroidAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("droid");
const DROID_RESUME_VERSION = 1 as const;
const SESSION_INIT_TIMEOUT_MS = 75_000;

const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

export interface DroidAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
  /** Test-only visibility into adapter-owned collections. */
  readonly registerDebugStateReader?: (
    read: (threadId: ThreadId) => Effect.Effect<{
      readonly threadLockCount: number;
      readonly interruptedTurnCount: number;
    }>,
  ) => void;
}

interface ThreadLockEntry {
  readonly semaphore: Semaphore.Semaphore;
  readonly references: number;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

type PendingUserInputResolution =
  | { readonly _tag: "answered"; readonly answers: ProviderUserInputAnswers }
  | { readonly _tag: "cancelled" };

interface PendingUserInput {
  readonly resolution: Deferred.Deferred<PendingUserInputResolution>;
}

interface DroidSessionContext {
  readonly threadId: ThreadId;
  /** Mutable: rewind/compact mint a successor session id. */
  droidSessionId: string;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly rpc: DroidRpcClient;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  /** Turns already interrupted; late completions must not resurrect them. */
  readonly interruptedTurnIds: Set<TurnId>;
  /**
   * Message ids accepted into the current logical t3 turn. A sendTurn while
   * this is non-empty is a steer: droid may either coalesce it into the active
   * physical run or execute it as a later physical run.
   */
  readonly pendingTurnMessageIds: Set<string>;
  /**
   * Pending message ids whose live create_message notification has arrived.
   * At an earlier physical turn's terminal, these are the steers known to have
   * been coalesced into that run.
   */
  readonly persistedPendingTurnMessageIds: Set<string>;
  /** Runtime item ids with an emitted item.started awaiting completion. */
  readonly openItemIds: Set<string>;
  /** Tool names keyed by provider tool-use id within this Droid session. */
  readonly toolUseNames: Map<string, string>;
  /** Droid child (subagent) session ids mapped onto t3 task lifecycles. */
  readonly childSessions: Map<string, { readonly description: string }>;
  /**
   * Implementation session minted by a spec handoff. It streams into the same
   * t3 turn before the spec session's terminal notification arrives, and is
   * adopted as the live session id when that terminal settles the turn.
   */
  specSuccessorSessionId: string | undefined;
  /** Live-context meter from the most recent session_token_usage_changed. */
  lastCallTokenUsage: DroidLastCallTokenUsage | undefined;
  lastEmittedTokenUsage: ThreadTokenUsageSnapshot | undefined;
  currentModelId: string | undefined;
  currentReasoningEffort: string | undefined;
  currentInteractionMode: "auto" | "spec";
  stopped: boolean;
}

/** t3 runtime modes map 1:1 onto droid autonomy levels. */
export function droidAutonomyLevelForRuntimeMode(
  runtimeMode: ProviderSession["runtimeMode"],
): "off" | "low" | "medium" | "high" {
  switch (runtimeMode) {
    case "approval-required":
      return "off";
    case "auto-accept-edits":
      return "low";
    case "auto":
      return "medium";
    case "full-access":
      return "high";
  }
}

export function droidToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  if (toolName.startsWith("mcp__") || toolName.startsWith("mcp_")) return "mcp_tool_call";
  switch (toolName) {
    case "Execute":
    case "Bash":
      return "command_execution";
    case "Edit":
    case "Create":
    case "Write":
    case "ApplyPatch":
      return "file_change";
    case "WebSearch":
    case "FetchUrl":
      return "web_search";
    case "Task":
      return "collab_agent_tool_call";
    default:
      return "dynamic_tool_call";
  }
}

/**
 * Every real droid confirmation type maps onto a canonical request type the
 * clients render; nothing may land on "unknown", which clients drop, leaving
 * an unanswerable hang.
 */
export function droidCanonicalRequestType(
  confirmationType: string | undefined,
): CanonicalRequestType {
  switch (confirmationType) {
    case "exec":
      return "exec_command_approval";
    case "edit":
    case "create":
      return "file_change_approval";
    case "apply_patch":
      return "apply_patch_approval";
    case "mcp_tool":
    case "ask_user":
    case "start_mission_run":
      return "dynamic_tool_call";
    case "exit_spec_mode":
    case "propose_mission":
      return "plan_approval";
    case "sandbox_violation":
    case "droid_shield_violation":
      return "command_execution_approval";
    default:
      return "unknown";
  }
}

/**
 * Pick the droid confirmation outcome for a t3 approval decision. The reply
 * must be one of the outcomes the request offered; anything else is treated
 * as cancel by the CLI, so unmatched preferences fall back explicitly.
 */
export function selectDroidPermissionOutcome(
  options: ReadonlyArray<DroidPermissionOption>,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const outcomes = options
    .map((option) => option.outcome?.trim())
    .filter((outcome): outcome is string => Boolean(outcome));
  const preference =
    decision === "acceptForSession"
      ? ["proceed_always", "proceed_always_file", "proceed_always_tools", "proceed_always_server"]
      : decision === "accept"
        ? ["proceed_once"]
        : ["cancel"];
  for (const preferred of preference) {
    if (outcomes.includes(preferred)) return preferred;
  }
  if (decision === "decline") return outcomes.includes("cancel") ? "cancel" : undefined;
  // Approvals with bespoke outcome sets (spec exit, autonomy raises) still
  // proceed on the first non-cancel option droid offered.
  return outcomes.find((outcome) => outcome !== "cancel");
}

/**
 * Cumulative session spend is not the context meter: droid compacts
 * automatically and reports the live context in `lastCallTokenUsage`. Use the
 * last call for `usedTokens` when droid sent one, and keep the cumulative
 * (child-inclusive) spend as `totalProcessedTokens`.
 */
export function droidTokenUsageSnapshot(
  usage: DroidTokenUsage,
  lastCall?: DroidLastCallTokenUsage,
): ThreadTokenUsageSnapshot {
  const inputTokens = usage.inputTokens ?? 0;
  const cachedInputTokens = usage.cacheReadTokens ?? 0;
  const outputTokens = usage.outputTokens ?? 0;
  const usedTokens = inputTokens + cachedInputTokens + outputTokens;
  const totalProcessedTokens = usedTokens + (usage.cacheCreationTokens ?? 0);
  const lastUsedTokens =
    lastCall === undefined
      ? undefined
      : lastCall.inputTokens + lastCall.cacheReadTokens + (lastCall.outputTokens ?? 0);
  return {
    usedTokens: lastUsedTokens ?? usedTokens,
    totalProcessedTokens,
    inputTokens,
    cachedInputTokens,
    outputTokens,
    ...(usage.thinkingTokens !== undefined ? { reasoningOutputTokens: usage.thinkingTokens } : {}),
    ...(lastCall !== undefined && lastUsedTokens !== undefined
      ? {
          lastUsedTokens,
          lastInputTokens: lastCall.inputTokens,
          lastCachedInputTokens: lastCall.cacheReadTokens,
          lastOutputTokens: lastCall.outputTokens ?? 0,
        }
      : {}),
    compactsAutomatically: true,
  };
}

function droidTokenUsageSnapshotsEqual(
  left: ThreadTokenUsageSnapshot | undefined,
  right: ThreadTokenUsageSnapshot,
): boolean {
  return (
    left !== undefined &&
    left.usedTokens === right.usedTokens &&
    left.totalProcessedTokens === right.totalProcessedTokens &&
    left.inputTokens === right.inputTokens &&
    left.cachedInputTokens === right.cachedInputTokens &&
    left.outputTokens === right.outputTokens &&
    left.reasoningOutputTokens === right.reasoningOutputTokens &&
    left.lastUsedTokens === right.lastUsedTokens &&
    left.lastInputTokens === right.lastInputTokens &&
    left.lastCachedInputTokens === right.lastCachedInputTokens &&
    left.lastOutputTokens === right.lastOutputTokens &&
    left.compactsAutomatically === right.compactsAutomatically
  );
}

type DroidTurnOutcome =
  | { readonly state: "completed"; readonly stopReason: string }
  | { readonly state: "cancelled"; readonly stopReason: string }
  | { readonly state: "failed"; readonly errorMessage: string };

export function droidTurnOutcomeForReason(reason: string | undefined): DroidTurnOutcome {
  switch (reason) {
    case undefined:
    case "completed":
    // A spec handoff is droid finishing planning and forking into the
    // implementation session; the turn succeeded.
    case "spec_handoff":
      return { state: "completed", stopReason: reason ?? "completed" };
    case "cancelled":
      return { state: "cancelled", stopReason: reason };
    case "permission_rejected":
    case "prompt_rejected":
      return { state: "cancelled", stopReason: reason };
    case "model_authentication_failed":
      return {
        state: "failed",
        errorMessage: "Droid is not authenticated. Run `droid` in a terminal to sign in.",
      };
    case "model_usage_exhausted":
      return { state: "failed", errorMessage: "Droid model usage is exhausted." };
    case "no_approver_available":
      return { state: "failed", errorMessage: "Droid required an approval no client answered." };
    default:
      return { state: "failed", errorMessage: `Droid turn ended with reason '${reason}'.` };
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseDroidResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== DROID_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingApprovals.values()),
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    { discard: true },
  );
}

function settlePendingUserInputsAsCancelled(
  pendingUserInputs: ReadonlyMap<ApprovalRequestId, PendingUserInput>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingUserInputs.values()),
    (pending) => Deferred.succeed(pending.resolution, { _tag: "cancelled" }).pipe(Effect.ignore),
    { discard: true },
  );
}

const decodePermissionRequest = Schema.decodeUnknownEffect(DroidPermissionRequest);
const decodeAskUserRequest = Schema.decodeUnknownEffect(DroidAskUserRequest);
const decodeInitializeResult = Schema.decodeUnknownEffect(DroidInitializeSessionResult);
const decodeLoadResult = Schema.decodeUnknownEffect(DroidLoadSessionResult);
const decodeExecuteRewindResult = Schema.decodeUnknownEffect(DroidExecuteRewindResult);

export function makeDroidAdapter(droidSettings: DroidSettings, options?: DroidAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("droid");
    const fileSystem = yield* FileSystem.FileSystem;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;

    const sessions = new Map<ThreadId, DroidSessionContext>();
    // stopAll coordination: reject new starts while closing, and remember
    // threads whose startSession is still in flight (not yet in `sessions`).
    let closing = false;
    const startingThreads = new Set<ThreadId>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<ThreadId, ThreadLockEntry>());
    options?.registerDebugStateReader?.((threadId) =>
      SynchronizedRef.get(threadLocksRef).pipe(
        Effect.map((locks) => ({
          threadLockCount: locks.size,
          interruptedTurnCount: sessions.get(threadId)?.interruptedTurnIds.size ?? 0,
        })),
      ),
    );
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Droid runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const acquireThreadSemaphore = (threadId: ThreadId) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = current.get(threadId);
        if (existing !== undefined) {
          const next = new Map(current);
          next.set(threadId, { ...existing, references: existing.references + 1 });
          return Effect.succeed([existing.semaphore, next] as const);
        }
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(threadId, { semaphore, references: 1 });
            return [semaphore, next] as const;
          }),
        );
      });

    const releaseThreadSemaphore = (threadId: ThreadId) =>
      SynchronizedRef.update(threadLocksRef, (current) => {
        const existing = current.get(threadId);
        if (existing === undefined) return current;
        const next = new Map(current);
        if (existing.references === 1 && !sessions.has(threadId)) {
          next.delete(threadId);
        } else {
          next.set(threadId, { ...existing, references: existing.references - 1 });
        }
        return next;
      });

    const withThreadLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        acquireThreadSemaphore(threadId),
        (semaphore) => semaphore.withPermit(effect),
        () => releaseThreadSemaphore(threadId),
      );

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to write native Droid notification log.", {
            cause,
            threadId,
            method,
          }),
        ),
      );

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<DroidSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const requestViaRpc = (
      ctx: DroidSessionContext,
      method: string,
      params: unknown,
      requestOptions?: { readonly timeoutMs?: number | undefined },
    ) =>
      ctx.rpc.request(method, params, requestOptions).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method,
              detail: cause.message,
              cause,
            }),
        ),
      );

    /** Close an open runtime item, if any, so clients never see a stuck row. */
    const completeOpenItem = (
      ctx: DroidSessionContext,
      itemId: string,
      itemType: "assistant_message" | "reasoning",
      turnId: TurnId,
    ) =>
      Effect.gen(function* () {
        if (!ctx.openItemIds.delete(itemId)) return;
        yield* offerRuntimeEvent({
          type: "item.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          itemId: RuntimeItemId.make(itemId),
          payload: { itemType, status: "completed" },
        });
      });

    const emitStreamedDelta = (
      ctx: DroidSessionContext,
      turnId: TurnId,
      input: {
        readonly itemId: string;
        readonly itemType: "assistant_message" | "reasoning";
        readonly streamKind: "assistant_text" | "reasoning_text";
        readonly delta: string;
      },
    ) =>
      Effect.gen(function* () {
        if (!ctx.openItemIds.has(input.itemId)) {
          ctx.openItemIds.add(input.itemId);
          yield* offerRuntimeEvent({
            type: "item.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId,
            itemId: RuntimeItemId.make(input.itemId),
            payload: { itemType: input.itemType, status: "inProgress" },
          });
        }
        yield* offerRuntimeEvent({
          type: "content.delta",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          itemId: RuntimeItemId.make(input.itemId),
          payload: { streamKind: input.streamKind, delta: input.delta },
        });
      });

    const emitTokenUsage = (
      ctx: DroidSessionContext,
      usage: DroidTokenUsage,
      lastCall?: DroidLastCallTokenUsage,
    ) =>
      Effect.gen(function* () {
        const snapshot = droidTokenUsageSnapshot(usage, lastCall);
        if (droidTokenUsageSnapshotsEqual(ctx.lastEmittedTokenUsage, snapshot)) {
          return;
        }
        yield* offerRuntimeEvent({
          type: "thread.token-usage.updated",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { usage: snapshot },
        });
        ctx.lastEmittedTokenUsage = snapshot;
      });

    const completeAllOpenItems = (
      ctx: DroidSessionContext,
      turnId: TurnId,
      outcome: DroidTurnOutcome,
    ) =>
      Effect.forEach(
        Array.from(ctx.openItemIds),
        (itemId) => {
          const assistantMessage = itemId.startsWith("msg:");
          const reasoning = itemId.startsWith("reasoning:");
          const toolName = assistantMessage || reasoning ? undefined : ctx.toolUseNames.get(itemId);
          return Effect.gen(function* () {
            yield* offerRuntimeEvent({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId,
              itemId: RuntimeItemId.make(itemId),
              payload: {
                itemType: assistantMessage
                  ? "assistant_message"
                  : reasoning
                    ? "reasoning"
                    : toolName
                      ? droidToolLifecycleItemType(toolName)
                      : "dynamic_tool_call",
                status:
                  assistantMessage || reasoning || outcome.state === "completed"
                    ? "completed"
                    : "failed",
                ...(toolName ? { title: toolName } : {}),
              },
            });
            ctx.openItemIds.delete(itemId);
          });
        },
        { discard: true },
      );

    /**
     * Terminal settlement for a turn. Emits exactly one turn.completed; late
     * completions for interrupted or already-settled turns are dropped.
     */
    const settleTurn = (ctx: DroidSessionContext, turnId: TurnId, outcome: DroidTurnOutcome) =>
      Effect.gen(function* () {
        // A pre-marked interrupt outranks any non-cancelled terminal: consume
        // the mark and drop the notification so the pending cancellation
        // settles the turn instead. Cancelled outcomes pass through; the
        // active-turn guard below still prevents a second terminal event.
        if (ctx.interruptedTurnIds.has(turnId) && outcome.state !== "cancelled") {
          ctx.interruptedTurnIds.delete(turnId);
          return;
        }
        if (ctx.activeTurnId !== turnId && ctx.session.activeTurnId !== turnId) {
          // Late cancelled terminal for an already-settled turn retires its mark.
          if (outcome.state === "cancelled") ctx.interruptedTurnIds.delete(turnId);
          return;
        }
        yield* completeAllOpenItems(ctx, turnId, outcome);
        ctx.toolUseNames.clear();
        ctx.pendingTurnMessageIds.clear();
        ctx.persistedPendingTurnMessageIds.clear();
        ctx.activeTurnId = undefined;
        const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
        ctx.session = { ...readySession, status: "ready", updatedAt: yield* nowIso };
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload:
            outcome.state === "failed"
              ? { state: "failed", errorMessage: outcome.errorMessage }
              : { state: outcome.state, stopReason: outcome.stopReason },
        });
      });

    const handleTurnCompleted = (
      ctx: DroidSessionContext,
      notification: Extract<DroidSessionNotification, { type: "agent_turn_completed" }>,
    ) =>
      withThreadLock(
        ctx.threadId,
        Effect.gen(function* () {
          const live = sessions.get(ctx.threadId);
          if (!live || live.stopped || live.droidSessionId !== ctx.droidSessionId) return;
          const turnId = live.activeTurnId ?? live.session.activeTurnId;
          yield* emitTokenUsage(
            live,
            notification.cumulativeTokenUsage ?? notification.tokenUsage,
            live.lastCallTokenUsage,
          );
          // The spec session hands off to the implementation session it
          // spawned; from here on the successor is the conversation.
          if (notification.reason === "spec_handoff" && live.specSuccessorSessionId !== undefined) {
            live.droidSessionId = live.specSuccessorSessionId;
            live.specSuccessorSessionId = undefined;
            live.session = {
              ...live.session,
              resumeCursor: {
                schemaVersion: DROID_RESUME_VERSION,
                sessionId: live.droidSessionId,
              },
            };
          }
          if (notification.turnId !== undefined) {
            live.pendingTurnMessageIds.delete(notification.turnId);
            live.persistedPendingTurnMessageIds.delete(notification.turnId);
            if (live.pendingTurnMessageIds.size > 0) {
              // Factory CLI emits one live terminal for a physical run, keyed
              // by its opening message id. Steers drained into that run emit
              // create_message first and receive only durable outcome records;
              // steers left queued run later with their own message-id terminal
              // (sharedAgentRunner.ts and AgentLoop.ts queued-message contract).
              const allRemainingWereCoalesced = Array.from(live.pendingTurnMessageIds).every(
                (messageId) => live.persistedPendingTurnMessageIds.has(messageId),
              );
              if (!allRemainingWereCoalesced) return;
              live.pendingTurnMessageIds.clear();
              live.persistedPendingTurnMessageIds.clear();
            }
          } else {
            live.pendingTurnMessageIds.clear();
            live.persistedPendingTurnMessageIds.clear();
          }
          if (turnId === undefined) return;
          yield* settleTurn(live, turnId, droidTurnOutcomeForReason(notification.reason));
        }),
      );

    const handleNotification = (ctx: DroidSessionContext, notification: DroidSessionNotification) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        if (notification.type === "agent_turn_completed") {
          return yield* handleTurnCompleted(ctx, notification);
        }
        if (notification.type === "session_compacted") {
          // Compaction keeps the same droid session id (verified in
          // factory-mono); only the context meter moves, and that arrives via
          // session_token_usage_changed.
          yield* logNative(ctx.threadId, "droid.session_compacted", notification);
          return;
        }
        if (notification.type === "child_session_available") {
          const description =
            notification.description ?? notification.subagentType ?? "Droid subagent";
          ctx.childSessions.set(notification.childSessionId, { description });
          yield* offerRuntimeEvent({
            type: "task.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            payload: {
              taskId: RuntimeTaskId.make(notification.childSessionId),
              description,
            },
          });
          return;
        }
        if (notification.type === "session_token_usage_changed") {
          if (notification.lastCallTokenUsage !== undefined) {
            ctx.lastCallTokenUsage = notification.lastCallTokenUsage;
          }
          yield* emitTokenUsage(
            ctx,
            notification.inclusiveTokenUsage ?? notification.tokenUsage,
            ctx.lastCallTokenUsage,
          );
          return;
        }
        if (notification.type === "session_title_updated") {
          yield* offerRuntimeEvent({
            type: "thread.metadata.updated",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            payload: { name: notification.title },
          });
          return;
        }
        if (notification.type === "error") {
          yield* offerRuntimeEvent({
            type: "runtime.error",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            payload: { message: notification.message, class: "provider_error" },
          });
          return;
        }
        if (notification.type === "llm_retry") {
          yield* offerRuntimeEvent({
            type: "runtime.warning",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            payload: {
              message: `Droid is retrying the model request (attempt ${notification.attempt}).`,
            },
          });
          return;
        }
        if (notification.type === "create_message") {
          if (
            isRecord(notification.message) &&
            typeof notification.message.id === "string" &&
            (notification.message.role === "user" ||
              notification.message.type === "user_message") &&
            ctx.pendingTurnMessageIds.has(notification.message.id)
          ) {
            ctx.persistedPendingTurnMessageIds.add(notification.message.id);
          }
          const turnId = ctx.activeTurnId;
          if (turnId !== undefined) {
            const existing = ctx.turns.find((turn) => turn.id === turnId);
            ctx.turns = existing
              ? ctx.turns.map((turn) =>
                  turn.id === turnId
                    ? { ...turn, items: [...turn.items, notification.message] }
                    : turn,
                )
              : [...ctx.turns, { id: turnId, items: [notification.message] }];
          }
          return;
        }

        // Everything below streams inside a turn; drop stragglers with no
        // active turn or an interrupted one (Grok precedent).
        const turnId = ctx.activeTurnId;
        if (turnId === undefined || ctx.interruptedTurnIds.has(turnId)) return;

        switch (notification.type) {
          case "assistant_text_delta":
            yield* emitStreamedDelta(ctx, turnId, {
              itemId: `msg:${notification.messageId}`,
              itemType: "assistant_message",
              streamKind: "assistant_text",
              delta: notification.textDelta,
            });
            return;
          case "assistant_text_complete":
            yield* completeOpenItem(
              ctx,
              `msg:${notification.messageId}`,
              "assistant_message",
              turnId,
            );
            return;
          case "thinking_text_delta":
            yield* emitStreamedDelta(ctx, turnId, {
              itemId: `reasoning:${notification.messageId}`,
              itemType: "reasoning",
              streamKind: "reasoning_text",
              delta: notification.textDelta,
            });
            return;
          case "thinking_text_complete":
            yield* completeOpenItem(
              ctx,
              `reasoning:${notification.messageId}`,
              "reasoning",
              turnId,
            );
            return;
          case "tool_call": {
            const toolUse = notification.toolUse;
            yield* logNative(ctx.threadId, "droid.tool_call", toolUse);
            ctx.openItemIds.add(toolUse.id);
            yield* offerRuntimeEvent({
              type: "item.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId,
              itemId: RuntimeItemId.make(toolUse.id),
              payload: {
                itemType: droidToolLifecycleItemType(toolUse.name),
                status: "inProgress",
                title: toolUse.name,
                ...(toolUse.input !== undefined ? { data: toolUse.input } : {}),
              },
            });
            return;
          }
          case "tool_result": {
            const title = ctx.toolUseNames.get(notification.toolUseId);
            ctx.openItemIds.delete(notification.toolUseId);
            yield* offerRuntimeEvent({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId,
              itemId: RuntimeItemId.make(notification.toolUseId),
              payload: {
                itemType: title ? droidToolLifecycleItemType(title) : "dynamic_tool_call",
                status: notification.isError ? "failed" : "completed",
                ...(title ? { title } : {}),
              },
            });
            ctx.toolUseNames.delete(notification.toolUseId);
            return;
          }
          case "tool_progress_update": {
            const taskId = notification.update.subagentSessionId?.trim();
            if (!taskId) {
              // Ingestion discards taskless progress, and the parent
              // conversation's item lifecycle already covers its tools.
              return;
            }
            const toolUseId = notification.toolUseId.trim();
            const toolName = notification.toolName.trim();
            const summary = [
              notification.update.text,
              notification.update.details,
              notification.update.error,
              notification.update.status,
              notification.update.valueSnippet,
            ]
              .map((value) => value?.trim())
              .find((value): value is string => value !== undefined && value.length > 0);
            // Do not gate on childSessions: progress can arrive before
            // child_session_available, and its session id already owns it.
            yield* offerRuntimeEvent({
              type: "tool.progress",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              turnId,
              payload: {
                taskId: RuntimeTaskId.make(taskId),
                ...(toolUseId ? { toolUseId } : {}),
                ...(toolName ? { toolName } : {}),
                ...(summary ? { summary } : {}),
              },
            });
            return;
          }
          default:
            // Unknown and internal notification types (heartbeats, working
            // state, mission traffic) are intentionally ignored.
            return;
        }
      });

    // Notifications from a session id that is neither the live session nor a
    // known child are stragglers from an abandoned (pre-rewind, pre-compact)
    // session and must not touch turn state.
    const handleChildSessionNotification = (
      ctx: DroidSessionContext,
      sessionId: string,
      notification: DroidSessionNotification,
    ) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        const child = ctx.childSessions.get(sessionId);
        if (!child) return;
        if (notification.type === "agent_turn_completed") {
          const outcome = droidTurnOutcomeForReason(notification.reason);
          yield* offerRuntimeEvent({
            type: "task.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: ctx.activeTurnId,
            payload: {
              taskId: RuntimeTaskId.make(sessionId),
              status:
                outcome.state === "completed"
                  ? "completed"
                  : outcome.state === "cancelled"
                    ? "stopped"
                    : "failed",
              summary: child.description,
            },
          });
          ctx.childSessions.delete(sessionId);
        }
      });

    // tool_result carries no tool name; remember tool_call names per session.
    const rememberToolUse = (ctx: DroidSessionContext, toolUse: DroidToolUse) => {
      ctx.toolUseNames.set(toolUse.id, toolUse.name);
    };

    const handlePermissionRequest = (ctx: DroidSessionContext, request: DroidServerRequest) =>
      Effect.gen(function* () {
        const params = yield* decodePermissionRequest(request.params).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "droid.request_permission",
                detail: "Failed to decode Droid permission request.",
                cause,
              }),
          ),
        );
        yield* logNative(ctx.threadId, "droid.request_permission", request.params);
        const primaryToolUse = params.toolUses[0];
        if (primaryToolUse) rememberToolUse(ctx, primaryToolUse.toolUse);
        const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
        const runtimeRequestId = RuntimeRequestId.make(requestId);
        const decision = yield* Deferred.make<ProviderApprovalDecision>();
        const turnId = ctx.activeTurnId;
        ctx.pendingApprovals.set(requestId, { decision });
        const requestType = droidCanonicalRequestType(primaryToolUse?.confirmationType);
        yield* offerRuntimeEvent({
          type: "request.opened",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          requestId: runtimeRequestId,
          payload: {
            requestType,
            detail:
              droidPermissionDetail(params) ??
              encodeJsonStringForDiagnostics(request.params)?.slice(0, 2000) ??
              "[unserializable params]",
            args: request.params,
          },
          raw: {
            source: "droid.jsonrpc.request",
            method: "droid.request_permission",
            payload: request.params,
          },
        });
        const resolved = yield* Deferred.await(decision);
        ctx.pendingApprovals.delete(requestId);
        yield* offerRuntimeEvent({
          type: "request.resolved",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          requestId: runtimeRequestId,
          payload: { requestType, decision: resolved },
        });
        const selectedOutcome =
          resolved === "cancel"
            ? "cancel"
            : (selectDroidPermissionOutcome(params.options, resolved) ?? "cancel");
        yield* request.respond({ selectedOption: selectedOutcome });
      });

    const handleAskUserRequest = (ctx: DroidSessionContext, request: DroidServerRequest) =>
      Effect.gen(function* () {
        const params = yield* decodeAskUserRequest(request.params).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "droid.ask_user",
                detail: "Failed to decode Droid ask_user request.",
                cause,
              }),
          ),
        );
        yield* logNative(ctx.threadId, "droid.ask_user", request.params);
        const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
        const runtimeRequestId = RuntimeRequestId.make(requestId);
        const resolution = yield* Deferred.make<PendingUserInputResolution>();
        const turnId = ctx.activeTurnId;
        ctx.pendingUserInputs.set(requestId, { resolution });
        yield* offerRuntimeEvent({
          type: "user-input.requested",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          requestId: runtimeRequestId,
          payload: {
            questions: params.questions.map((question) => ({
              id: String(question.index),
              header: question.topic,
              question: question.question,
              options: question.options.map((option) => ({ label: option, description: option })),
              multiSelect: question.multiSelect ?? false,
            })),
          },
          raw: {
            source: "droid.jsonrpc.request",
            method: "droid.ask_user",
            payload: request.params,
          },
        });
        const resolved = yield* Deferred.await(resolution);
        ctx.pendingUserInputs.delete(requestId);
        const resolvedAnswers = resolved._tag === "answered" ? resolved.answers : {};
        yield* offerRuntimeEvent({
          type: "user-input.resolved",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          requestId: runtimeRequestId,
          payload: { answers: resolvedAnswers },
        });
        if (resolved._tag === "cancelled") {
          yield* request.respond({ cancelled: true, answers: [] });
          return;
        }
        yield* request.respond({
          answers: params.questions.map((question) => {
            const raw = resolved.answers[String(question.index)];
            const answer = Array.isArray(raw) ? raw.map(String).join(", ") : String(raw ?? "");
            return { index: question.index, question: question.question, answer };
          }),
        });
      });

    const handleServerRequest = (ctx: DroidSessionContext, request: DroidServerRequest) => {
      const handler =
        request.method === "droid.request_permission"
          ? handlePermissionRequest(ctx, request)
          : handleAskUserRequest(ctx, request);
      // Each HITL request parks on a Deferred until a client answers, so it
      // must not block the request stream; failures answer the RPC so the
      // CLI never hangs on t3.
      return handler.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Droid server request handling failed.", {
            cause,
            method: request.method,
          }).pipe(
            Effect.andThen(
              request.fail(-32603, "t3-code failed to process the request.").pipe(Effect.ignore),
            ),
          ),
        ),
        Effect.forkIn(ctx.scope),
      );
    };

    const stopSessionInternal = (ctx: DroidSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsCancelled(ctx.pendingUserInputs);
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: DroidAdapterShape["startSession"] = (input) =>
      Effect.suspend(() => {
        if (closing) {
          return Effect.fail(
            new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "Droid adapter is stopping; cannot start a new session.",
            }),
          );
        }
        startingThreads.add(input.threadId);
        return startSessionLocked(input).pipe(
          Effect.ensuring(Effect.sync(() => startingThreads.delete(input.threadId))),
        );
      });

    const startSessionLocked = (input: Parameters<DroidAdapterShape["startSession"]>[0]) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const cwd = input.cwd.trim();
          const modelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const requestedModelId = modelSelection?.model;
          const requestedEffort = getModelSelectionStringOptionValue(
            modelSelection,
            "reasoningEffort",
          );

          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const resumeSessionId = parseDroidResume(input.resumeCursor)?.sessionId;
          const rpc = yield* makeDroidRpcClient({
            command: droidSettings.binaryPath,
            args: ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"],
            cwd,
            ...(options?.environment ? { env: options.environment } : {}),
          }).pipe(
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );

          const mcpServers = droidMcpServersParam(input.threadId);
          const autonomyLevel = droidAutonomyLevelForRuntimeMode(input.runtimeMode);

          const requestSession = (method: string, params: unknown) =>
            rpc.request(method, params, { timeoutMs: SESSION_INIT_TIMEOUT_MS }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: cause.message,
                    cause,
                  }),
              ),
            );

          const decodeSessionResult = <A>(
            decode: (value: unknown) => Effect.Effect<A, Schema.SchemaError>,
            value: unknown,
            method: string,
          ) =>
            decode(value).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method,
                    detail: "Failed to decode Droid session result.",
                    cause,
                  }),
              ),
            );

          const initialized = resumeSessionId
            ? {
                kind: "loaded" as const,
                sessionId: resumeSessionId,
                result: yield* decodeSessionResult(
                  decodeLoadResult,
                  yield* requestSession("droid.load_session", {
                    sessionId: resumeSessionId,
                    ...mcpServers,
                  }),
                  "droid.load_session",
                ),
              }
            : {
                kind: "initialized" as const,
                result: yield* decodeSessionResult(
                  decodeInitializeResult,
                  yield* requestSession("droid.initialize_session", {
                    machineId: "default",
                    cwd,
                    autonomyLevel,
                    interactionMode: "auto",
                    ...(requestedModelId ? { modelId: requestedModelId } : {}),
                    ...(requestedEffort ? { reasoningEffort: requestedEffort } : {}),
                    ...(input.title ? { title: input.title } : {}),
                    ...mcpServers,
                  }),
                  "droid.initialize_session",
                ),
              };
          const droidSessionId =
            initialized.kind === "loaded" ? initialized.sessionId : initialized.result.sessionId;

          // A loaded session keeps its persisted settings; re-assert t3's
          // autonomy, reset interaction to ordinary auto mode, and apply any
          // requested model so the first resumed turn uses the requested mode.
          if (initialized.kind === "loaded") {
            yield* requestSession("droid.update_session_settings", {
              autonomyLevel,
              interactionMode: "auto",
              ...(requestedModelId ? { modelId: requestedModelId } : {}),
              ...(requestedEffort ? { reasoningEffort: requestedEffort } : {}),
            });
          }

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(requestedModelId ? { model: requestedModelId } : {}),
            threadId: input.threadId,
            resumeCursor: {
              schemaVersion: DROID_RESUME_VERSION,
              sessionId: droidSessionId,
            },
            createdAt: now,
            updatedAt: now,
          };

          const ctx: DroidSessionContext = {
            threadId: input.threadId,
            droidSessionId,
            session,
            scope: sessionScope,
            rpc,
            pendingApprovals: new Map(),
            pendingUserInputs: new Map(),
            // Durable Droid user messages do not identify which ones were
            // steers coalesced into an earlier t3 turn. Only turns opened by
            // this process are safe rewind anchors; resumed rollback fails
            // loudly rather than guessing at a user-message boundary.
            turns: [],
            activeTurnId: undefined,
            interruptedTurnIds: new Set(),
            pendingTurnMessageIds: new Set(),
            persistedPendingTurnMessageIds: new Set(),
            openItemIds: new Set(),
            toolUseNames: new Map(),
            childSessions: new Map(),
            specSuccessorSessionId: undefined,
            lastCallTokenUsage:
              initialized.kind === "loaded" ? initialized.result.lastCallTokenUsage : undefined,
            lastEmittedTokenUsage: undefined,
            currentModelId: requestedModelId,
            currentReasoningEffort: requestedEffort,
            currentInteractionMode: "auto",
            stopped: false,
          };

          yield* Stream.runDrain(
            Stream.mapEffect(rpc.notifications, (envelope) =>
              Effect.gen(function* () {
                const notification = envelope.notification;
                // The envelope session id is the rewind guard: only the live
                // droid session's notifications reach turn handling. Known
                // child sessions become task lifecycles; a spec handoff's
                // implementation successor streams into the same t3 turn.
                if (envelope.sessionId !== undefined && envelope.sessionId !== ctx.droidSessionId) {
                  if (ctx.childSessions.has(envelope.sessionId)) {
                    return yield* handleChildSessionNotification(
                      ctx,
                      envelope.sessionId,
                      notification,
                    );
                  }
                  const isSpecSuccessor =
                    ctx.activeTurnId !== undefined &&
                    ctx.currentInteractionMode === "spec" &&
                    (ctx.specSuccessorSessionId === undefined ||
                      ctx.specSuccessorSessionId === envelope.sessionId);
                  if (!isSpecSuccessor) {
                    return yield* Effect.logDebug(
                      "Dropped Droid notification from an abandoned session.",
                      { sessionId: envelope.sessionId, type: notification.type },
                    );
                  }
                  ctx.specSuccessorSessionId = envelope.sessionId;
                }
                if (notification.type === "tool_call") rememberToolUse(ctx, notification.toolUse);
                yield* handleNotification(ctx, notification);
              }),
            ),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logError("Failed to process Droid runtime notification.", { cause }),
            ),
            // Fork into the session scope, not the calling fiber: children of
            // startSession are interrupted when it returns (see the Grok
            // adapter's war story).
            Effect.forkIn(ctx.scope),
          );

          // handleServerRequest forks each HITL exchange and answers the RPC
          // on failure, so draining the request stream itself cannot fail.
          yield* Stream.runDrain(
            Stream.mapEffect(rpc.serverRequests, (request) =>
              Effect.asVoid(handleServerRequest(ctx, request)),
            ),
          ).pipe(Effect.forkIn(ctx.scope));

          // Unexpected process death fails the active turn and tears the
          // session down so the UI never waits on a corpse.
          yield* rpc.exits.pipe(
            Effect.flatMap((exit) =>
              withThreadLock(
                input.threadId,
                Effect.gen(function* () {
                  // Identity check, not a session-id compare: rewind mints a
                  // successor droid session id on this same process, and a
                  // stale watcher must not tear down a replacement session.
                  const live = sessions.get(input.threadId);
                  if (live !== ctx || live.stopped) return;
                  const activeTurnId = live.activeTurnId ?? live.session.activeTurnId;
                  if (activeTurnId !== undefined) {
                    yield* settleTurn(live, activeTurnId, {
                      state: "failed",
                      errorMessage: `Droid exited unexpectedly (${exit.description}).`,
                    });
                  }
                  live.stopped = true;
                  yield* settlePendingApprovalsAsCancelled(live.pendingApprovals);
                  yield* settlePendingUserInputsAsCancelled(live.pendingUserInputs);
                  sessions.delete(input.threadId);
                  // Close the scope first so notification/request fibers are
                  // quiesced and nothing ordinary can publish after the
                  // terminal session event. Closing our own scope is safe:
                  // the forkIn finalizer skips interrupting the closing fiber.
                  yield* Effect.ignore(Scope.close(live.scope, Exit.void));
                  yield* offerRuntimeEvent({
                    type: "session.exited",
                    ...(yield* makeEventStamp()),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    payload: { exitKind: "error" },
                  });
                }),
              ),
            ),
            Effect.catch((cause) =>
              Effect.logError("Failed to process Droid process exit.", { cause }),
            ),
            Effect.forkIn(ctx.scope),
          );

          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: initialized.kind === "loaded" },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Droid session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: droidSessionId },
          });
          if (initialized.kind === "loaded") {
            // Resumed threads show the real context meter before the first
            // turn instead of an empty gauge.
            const usage = initialized.result.inclusiveTokenUsage ?? initialized.result.tokenUsage;
            if (usage) yield* emitTokenUsage(ctx, usage, ctx.lastCallTokenUsage);
          }

          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: DroidAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          const text = input.input?.trim();
          const attachments = input.attachments ?? [];
          if (!text && attachments.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or attachments.",
            });
          }

          const modelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const requestedModelId = modelSelection?.model;
          const requestedEffort = getModelSelectionStringOptionValue(
            modelSelection,
            "reasoningEffort",
          );
          const requestedInteractionMode = input.interactionMode === "plan" ? "spec" : "auto";
          const settingsPatch = {
            ...(requestedModelId && requestedModelId !== ctx.currentModelId
              ? { modelId: requestedModelId }
              : {}),
            ...(requestedEffort && requestedEffort !== ctx.currentReasoningEffort
              ? { reasoningEffort: requestedEffort }
              : {}),
            ...(requestedInteractionMode !== ctx.currentInteractionMode
              ? { interactionMode: requestedInteractionMode }
              : {}),
          };
          if (Object.keys(settingsPatch).length > 0) {
            yield* requestViaRpc(ctx, "droid.update_session_settings", settingsPatch);
            ctx.currentModelId = requestedModelId ?? ctx.currentModelId;
            ctx.currentReasoningEffort = requestedEffort ?? ctx.currentReasoningEffort;
            ctx.currentInteractionMode = requestedInteractionMode;
          }

          const images = yield* Effect.forEach(attachments, (attachment) =>
            Effect.gen(function* () {
              const attachmentPath = resolveAttachmentPath({
                attachmentsDir: serverConfig.attachmentsDir,
                attachment,
              });
              if (!attachmentPath) {
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "droid.add_user_message",
                  detail: `Invalid attachment id '${attachment.id}'.`,
                });
              }
              const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "droid.add_user_message",
                      detail: cause.message,
                      cause,
                    }),
                ),
              );
              return {
                type: "base64" as const,
                data: Buffer.from(bytes).toString("base64"),
                mediaType: attachment.mimeType,
              };
            }),
          );

          const messageId = yield* randomUUIDv4;
          const steeringTurnId = ctx.pendingTurnMessageIds.size > 0 ? ctx.activeTurnId : undefined;
          const turnId = steeringTurnId ?? TurnId.make(messageId);
          ctx.pendingTurnMessageIds.add(messageId);
          ctx.activeTurnId = turnId;
          const displayModel = ctx.currentModelId;
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
            ...(displayModel ? { model: displayModel } : {}),
          };

          if (steeringTurnId === undefined) {
            ctx.lastEmittedTokenUsage = undefined;
            // Track the turn here, not from create_message notifications, so
            // rewind anchoring stays 1:1 with t3's turn count.
            ctx.turns.push({ id: turnId, items: [] });
            yield* offerRuntimeEvent({
              type: "turn.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: {
                ...(displayModel ? { model: displayModel } : {}),
                ...(ctx.currentReasoningEffort ? { effort: ctx.currentReasoningEffort } : {}),
              },
            });
          }

          yield* requestViaRpc(ctx, "droid.add_user_message", {
            messageId,
            ...(text ? { text } : { text: "" }),
            ...(images.length > 0 ? { images } : {}),
          }).pipe(
            Effect.tapError(() =>
              Effect.gen(function* () {
                ctx.pendingTurnMessageIds.delete(messageId);
                ctx.persistedPendingTurnMessageIds.delete(messageId);
                if (steeringTurnId === undefined && ctx.pendingTurnMessageIds.size === 0) {
                  // A rejected opening message never became a droid turn, so
                  // it must not count toward rewind anchoring either.
                  ctx.turns = ctx.turns.filter((turn) => turn.id !== turnId);
                  yield* settleTurn(ctx, turnId, {
                    state: "failed",
                    errorMessage: "Droid rejected the user message.",
                  });
                }
              }),
            ),
          );

          return {
            threadId: input.threadId,
            turnId,
            resumeCursor: ctx.session.resumeCursor,
          };
        }),
      );

    const interruptTurn: DroidAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        // Mark before waiting for the thread lock so cancellation wins races
        // against a completion notification already queued on the lock:
        // settleTurn consumes the mark and drops that completion.
        const observed = yield* Effect.sync(() => {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped) {
            return { _tag: "Proceed" as const, interruptedTurnId: turnId };
          }
          const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
          if (turnId !== undefined && activeTurnId !== undefined && activeTurnId !== turnId) {
            return { _tag: "Ignore" as const };
          }
          const interruptedTurnId = turnId ?? activeTurnId;
          if (interruptedTurnId !== undefined) {
            ctx.interruptedTurnIds.add(interruptedTurnId);
          }
          return { _tag: "Proceed" as const, interruptedTurnId };
        });
        if (observed._tag === "Ignore") return;

        yield* withThreadLock(
          threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
            const interruptedTurnId = observed.interruptedTurnId ?? activeTurnId;
            if (
              interruptedTurnId !== undefined &&
              activeTurnId !== undefined &&
              activeTurnId !== interruptedTurnId
            ) {
              return;
            }
            yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
            yield* settlePendingUserInputsAsCancelled(ctx.pendingUserInputs);
            yield* Effect.ignore(requestViaRpc(ctx, "droid.interrupt_session", {}));
            if (interruptedTurnId !== undefined) {
              // Settle immediately; the late cancelled completion notification
              // is dropped by settleTurn's cleared-active-turn guard.
              yield* settleTurn(ctx, interruptedTurnId, {
                state: "cancelled",
                stopReason: "cancelled",
              });
              ctx.interruptedTurnIds.delete(interruptedTurnId);
            }
          }),
        );
      });

    const respondToRequest: DroidAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "droid.request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: DroidAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUserInputs.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "droid.ask_user",
            detail: `Unknown pending user-input request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.resolution, { _tag: "answered", answers });
      });

    const readThread: DroidAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    // Rewind forks the droid session before the first discarded user message
    // (t3 turn ids are those message ids) and re-anchors the live process on
    // the fork. File arrays stay empty: t3's checkpoint refs own filesystem
    // restoration, droid only rolls conversation state back.
    const rollbackThread: DroidAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          if (!Number.isInteger(numTurns) || numTurns < 1) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "numTurns must be an integer >= 1.",
            });
          }
          if (ctx.activeTurnId !== undefined) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "Cannot roll back while a turn is running.",
            });
          }
          const anchorIndex = ctx.turns.length - numTurns;
          const anchor = ctx.turns[anchorIndex];
          if (anchorIndex < 0 || anchor === undefined) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "droid.execute_rewind",
              detail: `Cannot roll back ${numTurns} turn(s); only ${ctx.turns.length} tracked in this session.`,
            });
          }
          const rewound = yield* decodeExecuteRewindResult(
            yield* requestViaRpc(ctx, "droid.execute_rewind", {
              sessionId: ctx.droidSessionId,
              messageId: String(anchor.id),
              filesToRestore: [],
              filesToDelete: [],
              forkTitle: "T3 Code checkpoint revert",
            }),
          ).pipe(
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "droid.execute_rewind",
                  detail: "Failed to decode Droid rewind result.",
                  cause,
                }),
              ),
            ),
          );
          // execute_rewind preserves the current session; the live process
          // must load the fork to continue on the rewound conversation.
          yield* requestViaRpc(ctx, "droid.load_session", {
            sessionId: rewound.newSessionId,
            ...droidMcpServersParam(threadId),
          });
          ctx.droidSessionId = rewound.newSessionId;
          ctx.turns = ctx.turns.slice(0, anchorIndex);
          ctx.session = {
            ...ctx.session,
            resumeCursor: {
              schemaVersion: DROID_RESUME_VERSION,
              sessionId: rewound.newSessionId,
            },
            updatedAt: yield* nowIso,
          };
          return { threadId, turns: ctx.turns };
        }),
      );

    const stopSession: DroidAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: DroidAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: DroidAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    // stopAll must also cover sessions still inside startSession: the gate
    // rejects new starts while in-flight ones are serialized through their
    // thread locks, so no live droid process can outlast the sweep.
    const stopAll: DroidAdapterShape["stopAll"] = () =>
      Effect.suspend(() => {
        closing = true;
        const threadIds = new Set([...sessions.keys(), ...startingThreads]);
        return Effect.forEach(
          threadIds,
          (threadId) =>
            withThreadLock(
              threadId,
              Effect.suspend(() => {
                const ctx = sessions.get(threadId);
                return ctx ? stopSessionInternal(ctx) : Effect.void;
              }),
            ),
          { discard: true },
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              closing = false;
            }),
          ),
        );
      });

    yield* Effect.addFinalizer(() =>
      Effect.ignore(stopAll()).pipe(
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies DroidAdapterShape;
  });
}

/**
 * Droid takes MCP servers as an array of named configs with {name, value}
 * header pairs (factory-mono HttpMcpSchema). Recomputed per call so loads and
 * rewinds pick up the current t3 MCP endpoint and credential, not the ones
 * from initialization.
 */
function droidMcpServersParam(threadId: ThreadId) {
  const mcpSession = McpProviderSession.readMcpProviderSession(threadId);
  return mcpSession
    ? {
        mcpServers: [
          {
            type: "http" as const,
            name: "t3-code",
            url: mcpSession.endpoint,
            headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
          },
        ],
      }
    : {};
}

/**
 * The human-readable summary a client shows next to the approval buttons.
 * Full structured detail (diff contents, plan text, per-file patches) rides
 * along untouched in the event's `args`/raw payload.
 */
function droidPermissionDetail(params: DroidPermissionRequest): string | undefined {
  const primary = params.toolUses[0];
  if (!primary) return undefined;
  const details = primary.details;
  switch (details.type) {
    case "exec":
      return details.fullCommand.trim() || details.command;
    case "edit":
      return details.filePath;
    case "create":
      return details.filePath;
    case "apply_patch": {
      const files = details.files?.map((file) => file.filePath);
      return files && files.length > 0 ? files.join("\n") : details.filePath;
    }
    case "exit_spec_mode":
      return details.title ? `${details.title}\n\n${details.plan}` : details.plan;
    case "propose_mission":
      return details.title ? `${details.title}\n\n${details.proposal}` : details.proposal;
    case "start_mission_run":
      return `Start a mission run (${details.runningMissionCount} already running).`;
    case "mcp_tool":
      return details.serverName
        ? `${details.serverName}: ${details.actualToolName ?? details.toolName}`
        : details.toolName;
    case "ask_user":
      return details.questionnaire;
    case "sandbox_violation":
      return `${details.violatingToolName} attempted a ${details.operationType} of ${details.target}: ${details.reason}`;
    case "droid_shield_violation":
      return `${details.command}\n${details.reason}`;
  }
}

/**
 * ProviderAdapter - Provider-specific runtime adapter contract.
 *
 * Defines the provider-native session/protocol operations that `ProviderService`
 * routes to after resolving the target provider. Implementations should focus
 * on provider behavior only and avoid cross-provider orchestration concerns.
 *
 * @module ProviderAdapter
 */
import {
  TrimmedNonEmptyString,
  type ApprovalRequestId,
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ProviderUserInputAnswers,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionLease,
  type ProviderSessionStartInput,
  type ProviderUploadFeedbackInput,
  type ProviderUploadFeedbackResult,
  type ThreadId,
  type ProviderTurnStartResult,
  type TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";

import { ProviderAdapterSessionNotFoundError } from "../Errors.ts";

export type ProviderSessionModelSwitchMode = "in-session" | "unsupported";
export type ProviderConversationRollbackMode = "supported" | "unsupported";

export interface ProviderAdapterCapabilities {
  /**
   * Declares whether changing the model on an existing session is supported.
   */
  readonly sessionModelSwitch: ProviderSessionModelSwitchMode;
  /** Declares whether rollback survives provider session restart/resume. */
  readonly conversationRollback: ProviderConversationRollbackMode;
}

export interface ProviderThreadTurnSnapshot {
  readonly id: TurnId;
  readonly items: ReadonlyArray<unknown>;
}

export interface ProviderThreadSnapshot {
  readonly threadId: ThreadId;
  readonly turns: ReadonlyArray<ProviderThreadTurnSnapshot>;
  readonly resumeCursor?: unknown;
}

export interface ProviderThreadRollbackTarget {
  readonly turnIds: ReadonlyArray<TurnId>;
  readonly anchorTurnId?: TurnId;
}

export function rollbackTargetMatchesTurnPrefix(
  turns: ReadonlyArray<Pick<ProviderThreadTurnSnapshot, "id">>,
  target: ProviderThreadRollbackTarget,
): boolean {
  return target.turnIds.every((turnId, index) => turns[index]?.id === turnId);
}

export function rollbackTargetMatchesKnownHistory(
  turns: ReadonlyArray<Pick<ProviderThreadTurnSnapshot, "id">>,
  target: ProviderThreadRollbackTarget,
): boolean {
  if (!rollbackTargetMatchesTurnPrefix(turns, target)) {
    return false;
  }
  if (target.anchorTurnId === undefined) {
    return true;
  }
  if (turns.length === target.turnIds.length) {
    return turns.length > 0;
  }
  return turns[target.turnIds.length]?.id === target.anchorTurnId;
}

export function parseVersionedSessionResumeCursor(
  raw: unknown,
  schemaVersion: number,
): string | undefined {
  const decode = Schema.decodeUnknownOption(
    Schema.Struct({
      schemaVersion: Schema.Literal(schemaVersion),
      sessionId: TrimmedNonEmptyString,
    }),
  );
  return Option.getOrUndefined(Option.map(decode(raw), (cursor) => cursor.sessionId));
}

export type ProviderAdapterSession = ProviderSession & {
  readonly sessionLease: ProviderSessionLease;
};

export const makeRequireActiveProviderSession = <Session extends { readonly stopped: boolean }>(
  sessions: ReadonlyMap<ThreadId, Session>,
  provider: ProviderDriverKind,
) => {
  return (threadId: ThreadId): Effect.Effect<Session, ProviderAdapterSessionNotFoundError> => {
    const session = sessions.get(threadId);
    return session === undefined || session.stopped
      ? Effect.fail(new ProviderAdapterSessionNotFoundError({ provider, threadId }))
      : Effect.succeed(session);
  };
};

export interface ProviderAdapterShape<TError> {
  /**
   * Provider kind implemented by this adapter.
   */
  readonly provider: ProviderDriverKind;
  readonly capabilities: ProviderAdapterCapabilities;

  /**
   * Start a provider-backed session.
   */
  readonly startSession: (
    input: ProviderSessionStartInput,
  ) => Effect.Effect<ProviderAdapterSession, TError>;

  /**
   * Send a turn to an active provider session.
   */
  readonly sendTurn: (
    input: ProviderSendTurnInput,
  ) => Effect.Effect<ProviderTurnStartResult, TError>;

  /**
   * Interrupt an active turn.
   */
  readonly interruptTurn: (threadId: ThreadId, turnId?: TurnId) => Effect.Effect<void, TError>;

  /**
   * Respond to an interactive approval request.
   */
  readonly respondToRequest: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ) => Effect.Effect<void, TError>;

  /**
   * Respond to a structured user-input request.
   */
  readonly respondToUserInput: (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ) => Effect.Effect<void, TError>;

  /**
   * Stop one provider session.
   */
  readonly stopSession: (threadId: ThreadId) => Effect.Effect<void, TError>;

  /**
   * List currently active provider sessions for this adapter.
   */
  readonly listSessions: () => Effect.Effect<ReadonlyArray<ProviderAdapterSession>>;

  /**
   * Check whether this adapter owns an active session id.
   */
  readonly hasSession: (threadId: ThreadId) => Effect.Effect<boolean>;

  /**
   * Read a provider thread snapshot.
   */
  readonly readThread: (threadId: ThreadId) => Effect.Effect<ProviderThreadSnapshot, TError>;

  /**
   * Roll back a provider thread to one absolute target.
   */
  readonly rollbackThread?: (
    threadId: ThreadId,
    target: ProviderThreadRollbackTarget,
  ) => Effect.Effect<ProviderThreadSnapshot, TError>;

  /**
   * Upload a thread to the provider when the adapter supports feedback.
   */
  readonly uploadFeedback?: (
    input: ProviderUploadFeedbackInput,
  ) => Effect.Effect<ProviderUploadFeedbackResult, TError>;

  /**
   * Stop all sessions owned by this adapter.
   */
  readonly stopAll: () => Effect.Effect<void, TError>;

  /**
   * Canonical runtime event stream emitted by this adapter.
   */
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}

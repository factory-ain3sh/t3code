import type {
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionLease,
  ProviderSessionRuntimeStatus,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  ProviderSessionDirectoryPersistenceError,
  ProviderValidationError,
} from "../Errors.ts";

/**
 * Runtime-payload key holding the checkpoint reactor's persisted revert
 * intent. Unlike the rest of the payload, which is scoped to one session
 * incarnation, the intent is thread-durable recovery state: the directory
 * carries it (re-stamped to the incoming lease) across owner and incarnation
 * payload wipes, and only the reactor's explicit null write clears it.
 */
export const CHECKPOINT_REVERT_INTENT_KEY = "checkpointRevertIntent";

export interface ProviderRuntimeBinding {
  readonly threadId: ThreadId;
  readonly provider: ProviderDriverKind;
  /**
   * Routing key for the configured provider instance that owns this
   * session. The persistence layer promotes legacy null rows before
   * exposing bindings; runtime callers must not infer this from `provider`.
   */
  readonly providerInstanceId?: ProviderInstanceId;
  /** Incarnation token for the specific live adapter session owning the binding. */
  readonly sessionLease?: ProviderSessionLease | null;
  readonly adapterKey?: string;
  readonly status?: ProviderSessionRuntimeStatus;
  readonly resumeCursor?: unknown | null;
  readonly runtimePayload?: unknown | null;
  readonly runtimeMode?: RuntimeMode;
}

export interface ProviderRuntimeBindingWithMetadata extends ProviderRuntimeBinding {
  readonly lastSeenAt: string;
}

export interface ProviderSessionOwnership {
  readonly providerInstanceId: ProviderInstanceId;
  readonly sessionLease: ProviderSessionLease | null;
}

export type ProviderSessionDirectoryReadError = ProviderSessionDirectoryPersistenceError;

export type ProviderSessionDirectoryWriteError =
  | ProviderValidationError
  | ProviderSessionDirectoryPersistenceError;

export interface ProviderSessionDirectoryShape {
  readonly upsert: (
    binding: ProviderRuntimeBinding,
  ) => Effect.Effect<void, ProviderSessionDirectoryWriteError>;

  readonly getProvider: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderDriverKind, ProviderSessionDirectoryReadError>;

  readonly getBinding: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<ProviderRuntimeBinding>, ProviderSessionDirectoryReadError>;

  /** Clear volatile routing ownership without changing the persisted binding. */
  readonly invalidateOwnership: (threadId: ThreadId) => Effect.Effect<void>;

  readonly matchesOwnership: (input: {
    readonly threadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly sessionLease: ProviderSessionLease;
  }) => Effect.Effect<boolean>;

  /**
   * Persist a cursor only when both the configured provider instance and its
   * current live session incarnation still own the thread.
   */
  readonly updateResumeCursorIfOwned: (input: {
    readonly threadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly sessionLease: ProviderSessionLease;
    readonly resumeCursor: unknown;
  }) => Effect.Effect<boolean, ProviderSessionDirectoryWriteError>;

  /**
   * Merge runtime payload only while the configured provider instance and its
   * current live session incarnation still own the thread.
   */
  readonly updateRuntimePayloadIfOwned: (input: {
    readonly threadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly sessionLease: ProviderSessionLease;
    readonly runtimePayload: unknown | null;
  }) => Effect.Effect<boolean, ProviderSessionDirectoryWriteError>;

  readonly listThreadIds: () => Effect.Effect<
    ReadonlyArray<ThreadId>,
    ProviderSessionDirectoryPersistenceError
  >;

  readonly listBindings: () => Effect.Effect<
    ReadonlyArray<ProviderRuntimeBindingWithMetadata>,
    ProviderSessionDirectoryPersistenceError
  >;
}

export class ProviderSessionDirectory extends Context.Service<
  ProviderSessionDirectory,
  ProviderSessionDirectoryShape
>()("t3/provider/Services/ProviderSessionDirectory") {}

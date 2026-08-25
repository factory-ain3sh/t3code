import { defaultInstanceIdForDriver, ProviderDriverKind, type ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProviderSessionDirectoryPersistenceError, ProviderValidationError } from "../Errors.ts";
import {
  CHECKPOINT_REVERT_INTENT_KEY,
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderRuntimeBindingWithMetadata,
  type ProviderSessionDirectoryShape,
} from "../Services/ProviderSessionDirectory.ts";
const decodeProviderDriverKindValue = Schema.decodeUnknownEffect(ProviderDriverKind);

function toPersistenceError(operation: string) {
  return (cause: unknown) =>
    new ProviderSessionDirectoryPersistenceError({
      operation,
      detail: `Failed to execute ${operation}.`,
      cause,
    });
}

function decodeProviderDriverKind(
  providerName: string,
  operation: string,
): Effect.Effect<ProviderDriverKind, ProviderSessionDirectoryPersistenceError> {
  return decodeProviderDriverKindValue(providerName).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderSessionDirectoryPersistenceError({
          operation,
          detail: `Unknown persisted provider '${providerName}'.`,
          cause,
        }),
    ),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function mergeRuntimePayload(
  existing: unknown | null,
  next: unknown | null | undefined,
): unknown | null {
  if (next === undefined) {
    return existing ?? null;
  }
  if (isRecord(existing) && isRecord(next)) {
    return { ...existing, ...next };
  }
  return next;
}

// The checkpoint reactor's persisted revert intent is thread-durable recovery
// state, not incarnation-scoped runtime state: locked recovery replaces the
// session incarnation mid-revert, and wiping the intent with the outgoing
// incarnation's payload would strand a rewound provider with no startup
// replay. The intent survives the wipe re-stamped to the incoming lease (the
// new incarnation inherits the revert obligation), and staleness stays with
// the reactor's provider/instance guard, its turn-identity progress fence,
// and adapter rollback validation. Only the reactor's explicit null write
// clears the slot.
function threadDurableRuntimePayload(
  existing: unknown | null,
  incomingLease: string | null | undefined,
): Record<string, unknown> | null {
  if (!isRecord(existing)) {
    return null;
  }
  const intent = existing[CHECKPOINT_REVERT_INTENT_KEY];
  if (!isRecord(intent)) {
    return null;
  }
  return {
    [CHECKPOINT_REVERT_INTENT_KEY]:
      incomingLease === null || incomingLease === undefined
        ? intent
        : { ...intent, sessionLease: incomingLease },
  };
}

function toRuntimeBinding(
  runtime: ProviderSessionRuntime.ProviderSessionRuntime,
  operation: string,
): Effect.Effect<ProviderRuntimeBindingWithMetadata, ProviderSessionDirectoryPersistenceError> {
  return decodeProviderDriverKind(runtime.providerName, operation).pipe(
    Effect.map(
      (provider) =>
        ({
          threadId: runtime.threadId,
          provider,
          // Migration boundary only: rows written before the instance split
          // have a null provider_instance_id. Promote them as they leave
          // persistence so hot routing code never has to infer an instance
          // from a driver kind.
          providerInstanceId: runtime.providerInstanceId ?? defaultInstanceIdForDriver(provider),
          sessionLease: runtime.sessionLease,
          adapterKey: runtime.adapterKey,
          runtimeMode: runtime.runtimeMode,
          status: runtime.status,
          resumeCursor: runtime.resumeCursor,
          runtimePayload: runtime.runtimePayload,
          lastSeenAt: runtime.lastSeenAt,
        }) satisfies ProviderRuntimeBindingWithMetadata,
    ),
  );
}

const makeProviderSessionDirectory = Effect.gen(function* () {
  const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

  // upsert reads the row, merges in process, and replaces the full row in a
  // second statement, while the ownership-CAS updates commit single
  // statements from callers that hold no lifecycle lock (live-turn cursor
  // persistence, the reactor's intent writes). A CAS committing inside
  // upsert's read-to-write window would be erased by the stale snapshot, so
  // every write to a thread's row serializes on a per-thread mutex here,
  // where the row lives, instead of relying on caller lock discipline. The
  // CAS predicates stay: they are ownership semantics, not race guards.
  const writeLocks = new Map<ThreadId, Semaphore.Semaphore>();
  const withThreadWriteLock = <A, E>(
    threadId: ThreadId,
    effect: Effect.Effect<A, E>,
  ): Effect.Effect<A, E> =>
    Effect.suspend(() => {
      let lock = writeLocks.get(threadId);
      if (lock === undefined) {
        lock = Semaphore.makeUnsafe(1);
        writeLocks.set(threadId, lock);
      }
      return lock.withPermit(effect);
    });

  const getBinding = (threadId: ThreadId) =>
    repository.getByThreadId({ threadId }).pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.getBinding:getByThreadId")),
      Effect.flatMap((runtime) =>
        Option.match(runtime, {
          onNone: () => Effect.succeed(Option.none<ProviderRuntimeBinding>()),
          onSome: (value) =>
            toRuntimeBinding(value, "ProviderSessionDirectory.getBinding").pipe(
              Effect.map((binding) => Option.some(binding)),
            ),
        }),
      ),
    );

  const upsertUnlocked = Effect.fn(function* (binding: ProviderRuntimeBinding) {
    const existing = yield* repository
      .getByThreadId({ threadId: binding.threadId })
      .pipe(Effect.mapError(toPersistenceError("ProviderSessionDirectory.upsert:getByThreadId")));

    const existingRuntime = Option.getOrUndefined(existing);
    const resolvedThreadId = binding.threadId ?? existingRuntime?.threadId;
    if (!resolvedThreadId) {
      return yield* new ProviderValidationError({
        operation: "ProviderSessionDirectory.upsert",
        issue: "threadId must be a non-empty string.",
      });
    }

    const now = DateTime.formatIso(yield* DateTime.now);
    const providerChanged =
      existingRuntime !== undefined && existingRuntime.providerName !== binding.provider;
    const providerInstanceId =
      binding.providerInstanceId ?? (!providerChanged ? existingRuntime?.providerInstanceId : null);
    if (providerInstanceId === null || providerInstanceId === undefined) {
      return yield* new ProviderValidationError({
        operation: "ProviderSessionDirectory.upsert",
        issue: "providerInstanceId is required for provider session runtime bindings.",
      });
    }
    const ownerChanged =
      providerChanged ||
      (existingRuntime !== undefined &&
        existingRuntime.providerInstanceId !== null &&
        existingRuntime.providerInstanceId !== providerInstanceId);
    const sessionIncarnationChanged =
      binding.sessionLease !== undefined &&
      binding.sessionLease !== null &&
      existingRuntime !== undefined &&
      existingRuntime.sessionLease !== binding.sessionLease;
    yield* repository
      .upsert({
        threadId: resolvedThreadId,
        providerName: binding.provider,
        providerInstanceId,
        sessionLease:
          binding.sessionLease !== undefined
            ? binding.sessionLease
            : ownerChanged
              ? null
              : (existingRuntime?.sessionLease ?? null),
        adapterKey:
          binding.adapterKey ??
          (providerChanged ? binding.provider : (existingRuntime?.adapterKey ?? binding.provider)),
        runtimeMode: binding.runtimeMode ?? existingRuntime?.runtimeMode ?? "full-access",
        status: binding.status ?? existingRuntime?.status ?? "running",
        lastSeenAt: now,
        resumeCursor:
          binding.resumeCursor !== undefined
            ? binding.resumeCursor
            : ownerChanged
              ? null
              : (existingRuntime?.resumeCursor ?? null),
        runtimePayload: mergeRuntimePayload(
          ownerChanged || sessionIncarnationChanged
            ? threadDurableRuntimePayload(
                existingRuntime?.runtimePayload ?? null,
                binding.sessionLease,
              )
            : (existingRuntime?.runtimePayload ?? null),
          binding.runtimePayload,
        ),
      })
      .pipe(Effect.mapError(toPersistenceError("ProviderSessionDirectory.upsert:upsert")));
  });

  const upsert: ProviderSessionDirectoryShape["upsert"] = (binding) =>
    withThreadWriteLock(binding.threadId, upsertUnlocked(binding));

  const getProvider: ProviderSessionDirectoryShape["getProvider"] = (threadId) =>
    getBinding(threadId).pipe(
      Effect.flatMap((binding) =>
        Option.match(binding, {
          onSome: (value) => Effect.succeed(value.provider),
          onNone: () =>
            Effect.fail(
              new ProviderSessionDirectoryPersistenceError({
                operation: "ProviderSessionDirectory.getProvider",
                detail: `No persisted provider binding found for thread '${threadId}'.`,
              }),
            ),
        }),
      ),
    );

  const updateResumeCursorIfOwned: ProviderSessionDirectoryShape["updateResumeCursorIfOwned"] = (
    input,
  ) =>
    withThreadWriteLock(
      input.threadId,
      DateTime.now.pipe(
        Effect.flatMap((now) =>
          repository.updateResumeCursorIfOwned({
            ...input,
            lastSeenAt: DateTime.formatIso(now),
          }),
        ),
        Effect.mapError(
          toPersistenceError(
            "ProviderSessionDirectory.updateResumeCursorIfOwned:updateResumeCursorIfOwned",
          ),
        ),
      ),
    );

  const updateRuntimePayloadIfOwned: ProviderSessionDirectoryShape["updateRuntimePayloadIfOwned"] =
    (input) =>
      withThreadWriteLock(
        input.threadId,
        DateTime.now.pipe(
          Effect.flatMap((now) =>
            repository.updateRuntimePayloadIfOwned({
              ...input,
              lastSeenAt: DateTime.formatIso(now),
            }),
          ),
          Effect.mapError(
            toPersistenceError(
              "ProviderSessionDirectory.updateRuntimePayloadIfOwned:updateRuntimePayloadIfOwned",
            ),
          ),
        ),
      );

  const listThreadIds: ProviderSessionDirectoryShape["listThreadIds"] = () =>
    repository.list().pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.listThreadIds:list")),
      Effect.map((rows) => rows.map((row) => row.threadId)),
    );

  const listBindings: ProviderSessionDirectoryShape["listBindings"] = () =>
    repository.list().pipe(
      Effect.mapError(toPersistenceError("ProviderSessionDirectory.listBindings:list")),
      Effect.flatMap((rows) =>
        Effect.forEach(
          rows,
          (row) => toRuntimeBinding(row, "ProviderSessionDirectory.listBindings"),
          { concurrency: "unbounded" },
        ),
      ),
    );

  return {
    upsert,
    getProvider,
    getBinding,
    updateResumeCursorIfOwned,
    updateRuntimePayloadIfOwned,
    listThreadIds,
    listBindings,
  } satisfies ProviderSessionDirectoryShape;
});

export const ProviderSessionDirectoryLive = Layer.effect(
  ProviderSessionDirectory,
  makeProviderSessionDirectory,
);

export function makeProviderSessionDirectoryLive() {
  return Layer.effect(ProviderSessionDirectory, makeProviderSessionDirectory);
}

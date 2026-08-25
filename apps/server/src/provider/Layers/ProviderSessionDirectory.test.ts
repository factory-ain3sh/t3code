// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionLease,
  ThreadId,
} from "@t3tools/contracts";
import { it, assert } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderSessionDirectoryShape,
  type ProviderSessionDirectoryWriteError,
} from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

function makeDirectoryLayer<E, R>(persistenceLayer: Layer.Layer<SqlClient.SqlClient, E, R>) {
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(Layer.provide(persistenceLayer));
  return Layer.mergeAll(
    // Expose the SqlClient alongside the services built on it. Effect
    // memoizes layers within a single build, so this is the same instance
    // the repository uses.
    persistenceLayer,
    runtimeRepositoryLayer,
    ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer)),
    NodeServices.layer,
  );
}

const droid = ProviderDriverKind.make("droid");
const droidOwner = ProviderInstanceId.make("droid-owner");

function getBinding(directory: ProviderSessionDirectoryShape, threadId: ThreadId) {
  return directory.getBinding(threadId).pipe(Effect.map(Option.getOrThrow));
}

it.layer(makeDirectoryLayer(SqlitePersistenceMemory))("ProviderSessionDirectoryLive", (it) => {
  it.effect("upserts and reads thread bindings", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const initialThreadId = ThreadId.make("thread-1");
      const initialLease = ProviderSessionLease.make("lease-initial");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: initialLease,
        threadId: initialThreadId,
      });

      const provider = yield* directory.getProvider(initialThreadId);
      assert.equal(provider, "codex");
      const initialBinding = Option.getOrThrow(yield* directory.getBinding(initialThreadId));
      assert.equal(initialBinding.threadId, initialThreadId);
      assert.equal(initialBinding.provider, "codex");
      assert.equal(initialBinding.providerInstanceId, "codex");
      assert.equal(
        yield* directory.matchesOwnership({
          threadId: initialThreadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: initialLease,
        }),
        true,
      );
      assert.equal(
        yield* directory.matchesOwnership({
          threadId: initialThreadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-stale"),
        }),
        false,
      );

      const nextThreadId = ThreadId.make("thread-2");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        threadId: nextThreadId,
      });
      const updatedBinding = Option.getOrThrow(yield* directory.getBinding(nextThreadId));
      assert.equal(updatedBinding.threadId, nextThreadId);

      const runtime = Option.getOrThrow(
        yield* runtimeRepository.getByThreadId({ threadId: nextThreadId }),
      );
      assert.equal(runtime.threadId, nextThreadId);
      assert.equal(runtime.status, "running");
      assert.equal(runtime.providerName, "codex");

      // The it.layer database is shared across tests, so only compare the
      // thread ids this test wrote. Rows written under the frozen test clock
      // tie on last_seen_at and fall back to thread_id ascending.
      const threadIds = yield* directory.listThreadIds();
      assert.deepEqual(
        threadIds.filter((id) => id === initialThreadId || id === nextThreadId),
        [initialThreadId, nextThreadId],
      );
    }),
  );

  it.effect("persists runtime fields and merges payload updates", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = ThreadId.make("thread-runtime");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        threadId,
        status: "starting",
        resumeCursor: {
          threadId: "provider-thread-runtime",
        },
        runtimePayload: {
          cwd: "/tmp/project",
          model: "gpt-5-codex",
        },
      });

      // Second upsert omits providerInstanceId: it is inherited from the
      // existing row because the provider did not change.
      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        status: "running",
        runtimePayload: {
          activeTurnId: "turn-1",
        },
      });

      const runtime = Option.getOrThrow(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(runtime.threadId, threadId);
      assert.equal(runtime.providerName, "codex");
      assert.equal(runtime.providerInstanceId, "codex");
      assert.equal(runtime.status, "running");
      assert.deepEqual(runtime.resumeCursor, {
        threadId: "provider-thread-runtime",
      });
      assert.deepEqual(runtime.runtimePayload, {
        cwd: "/tmp/project",
        model: "gpt-5-codex",
        activeTurnId: "turn-1",
      });
    }),
  );

  it.effect("ownership-checks cursor and payload updates", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const currentLease = ProviderSessionLease.make("lease-current");
      const cases: ReadonlyArray<{
        readonly name: string;
        readonly initial: Partial<ProviderRuntimeBinding>;
        readonly staleValue: unknown;
        readonly currentValue: unknown;
        readonly initialValue: unknown;
        readonly finalValue: unknown;
        readonly update: (
          directory: ProviderSessionDirectoryShape,
          input: {
            readonly threadId: ThreadId;
            readonly providerInstanceId: ProviderInstanceId;
            readonly sessionLease: ProviderSessionLease;
          },
          value: unknown,
        ) => Effect.Effect<boolean, ProviderSessionDirectoryWriteError>;
        readonly read: (binding: ProviderRuntimeBinding) => unknown;
      }> = [
        {
          name: "cursor",
          initial: { resumeCursor: { sessionId: "original" } },
          staleValue: { sessionId: "stale" },
          currentValue: { sessionId: "current" },
          initialValue: { sessionId: "original" },
          finalValue: { sessionId: "current" },
          update: (service, input, resumeCursor) =>
            service.updateResumeCursorIfOwned({ ...input, resumeCursor }),
          read: (binding) => binding.resumeCursor,
        },
        {
          name: "payload",
          initial: {
            runtimePayload: { cwd: "/tmp/project", checkpointRevertIntent: null },
          },
          staleValue: { checkpointRevertIntent: { turnCount: 1 } },
          currentValue: { checkpointRevertIntent: { turnCount: 2 } },
          initialValue: { cwd: "/tmp/project", checkpointRevertIntent: null },
          finalValue: {
            cwd: "/tmp/project",
            checkpointRevertIntent: { turnCount: 2 },
          },
          update: (service, input, runtimePayload) =>
            service.updateRuntimePayloadIfOwned({ ...input, runtimePayload }),
          read: (binding) => binding.runtimePayload,
        },
      ];

      for (const testCase of cases) {
        const threadId = ThreadId.make(`thread-owned-${testCase.name}`);
        const ownership = { threadId, providerInstanceId: droidOwner, sessionLease: currentLease };
        yield* directory.upsert({
          provider: droid,
          ...ownership,
          ...testCase.initial,
        });

        assert.isFalse(
          yield* testCase.update(
            directory,
            { ...ownership, providerInstanceId: ProviderInstanceId.make("droid-stale") },
            testCase.staleValue,
          ),
        );
        assert.isFalse(
          yield* testCase.update(
            directory,
            { ...ownership, sessionLease: ProviderSessionLease.make("lease-stale") },
            testCase.staleValue,
          ),
        );
        assert.deepEqual(
          testCase.read(yield* getBinding(directory, threadId)),
          testCase.initialValue,
        );

        assert.isTrue(yield* testCase.update(directory, ownership, testCase.currentValue));
        const binding = yield* getBinding(directory, threadId);
        assert.equal(binding.providerInstanceId, droidOwner);
        assert.equal(binding.sessionLease, currentLease);
        assert.deepEqual(testCase.read(binding), testCase.finalValue);
      }
    }),
  );

  it.effect("atomically merges concurrent runtime payload patches for the owning session", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-concurrent-payload");
      const owner = ProviderInstanceId.make("droid-owner");
      const sessionLease = ProviderSessionLease.make("lease-current");
      const patches = Array.from(
        { length: 32 },
        (_, index) => [`patch${String(index)}`, index] as const,
      );

      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease,
        threadId,
        runtimePayload: { cwd: "/tmp/project" },
      });

      const results = yield* Effect.all(
        patches.map(([key, value]) =>
          directory.updateRuntimePayloadIfOwned({
            threadId,
            providerInstanceId: owner,
            sessionLease,
            runtimePayload: { [key]: value },
          }),
        ),
        { concurrency: "unbounded" },
      );
      assert.isTrue(results.every(Boolean));

      const binding = yield* directory.getBinding(threadId);
      assert.isTrue(Option.isSome(binding));
      if (Option.isSome(binding)) {
        assert.deepEqual(binding.value.runtimePayload, {
          cwd: "/tmp/project",
          ...Object.fromEntries(patches),
        });
      }
    }),
  );

  it.effect("resets incarnation payload with the correct revert-intent durability", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const intent = (threadId: ThreadId) => ({
        commandId: "server:checkpoint-revert-complete:test",
        threadId,
        provider: "droid",
        providerInstanceId: droidOwner,
        cwd: "/tmp/project",
        turnCount: 1,
        retainedTurnIds: ["turn-1"],
        staleCheckpoints: [{ turnId: "turn-2", checkpointRef: `refs/test/${threadId}/2` }],
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const cases = [
        {
          name: "lease-change-carry",
          nextOwner: droidOwner,
          initialIntent: intent,
          expectedIntent: intent,
          preservesCursor: true,
        },
        {
          name: "cleared-intent-drop",
          nextOwner: droidOwner,
          initialIntent: () => null,
          expectedIntent: () => undefined,
          preservesCursor: true,
        },
        {
          name: "owner-rebind-drop",
          nextOwner: ProviderInstanceId.make("droid-next"),
          initialIntent: intent,
          expectedIntent: () => undefined,
          preservesCursor: false,
        },
      ] as const;

      for (const testCase of cases) {
        const threadId = ThreadId.make(`thread-${testCase.name}`);
        const checkpointRevertIntent = testCase.initialIntent(threadId);
        yield* directory.upsert({
          provider: droid,
          providerInstanceId: droidOwner,
          sessionLease: ProviderSessionLease.make("lease-a"),
          threadId,
          resumeCursor: { sessionId: "session-a" },
          runtimePayload: { activeTurnId: "turn-live", checkpointRevertIntent },
        });
        yield* directory.upsert({
          provider: droid,
          providerInstanceId: testCase.nextOwner,
          sessionLease: ProviderSessionLease.make("lease-b"),
          threadId,
          runtimePayload: { cwd: "/tmp/project" },
        });

        const binding = yield* getBinding(directory, threadId);
        assert.equal(binding.providerInstanceId, testCase.nextOwner);
        assert.deepEqual(
          binding.resumeCursor,
          testCase.preservesCursor ? { sessionId: "session-a" } : null,
        );
        const expectedIntent = testCase.expectedIntent(threadId);
        assert.deepEqual(binding.runtimePayload, {
          cwd: "/tmp/project",
          ...(expectedIntent === undefined ? {} : { checkpointRevertIntent: expectedIntent }),
        });
      }
    }),
  );

  it.effect("lists persisted bindings with metadata in oldest-first order", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const olderThreadId = ThreadId.make("thread-runtime-older");
      const newerThreadId = ThreadId.make("thread-runtime-newer");

      yield* runtimeRepository.upsert({
        threadId: newerThreadId,
        providerName: "codex",
        providerInstanceId: null,
        sessionLease: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T12:05:00.000Z",
        resumeCursor: {
          opaque: "resume-newer",
        },
        runtimePayload: {
          cwd: "/tmp/newer",
        },
      });

      yield* runtimeRepository.upsert({
        threadId: olderThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        sessionLease: null,
        adapterKey: "claudeAgent",
        runtimeMode: "approval-required",
        status: "starting",
        lastSeenAt: "2026-04-14T12:00:00.000Z",
        resumeCursor: {
          opaque: "resume-older",
        },
        runtimePayload: {
          cwd: "/tmp/older",
        },
      });

      // The it.layer database is shared across tests, so only compare the
      // rows this test wrote. Legacy null provider_instance_id rows are
      // promoted to the driver's default instance id as they leave
      // persistence.
      const bindings = yield* directory.listBindings();
      const relevant = bindings.filter(
        (binding) => binding.threadId === olderThreadId || binding.threadId === newerThreadId,
      );

      assert.deepEqual(relevant, [
        {
          threadId: olderThreadId,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: ProviderInstanceId.make("claudeAgent"),
          sessionLease: null,
          adapterKey: "claudeAgent",
          runtimeMode: "approval-required",
          status: "starting",
          lastSeenAt: "2026-04-14T12:00:00.000Z",
          resumeCursor: {
            opaque: "resume-older",
          },
          runtimePayload: {
            cwd: "/tmp/older",
          },
        },
        {
          threadId: newerThreadId,
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: null,
          adapterKey: "codex",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: "2026-04-14T12:05:00.000Z",
          resumeCursor: {
            opaque: "resume-newer",
          },
          runtimePayload: {
            cwd: "/tmp/newer",
          },
        },
      ]);
    }),
  );

  it.effect(
    "resets adapterKey to the new provider when provider changes without an explicit adapter key",
    () =>
      Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory;
        const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        const threadId = ThreadId.make("thread-provider-change");

        yield* runtimeRepository.upsert({
          threadId,
          providerName: "claudeAgent",
          providerInstanceId: null,
          sessionLease: null,
          adapterKey: "claudeAgent",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: "2026-01-01T00:00:00.000Z",
          resumeCursor: null,
          runtimePayload: null,
        });

        yield* directory.upsert({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          threadId,
        });

        const runtime = Option.getOrThrow(yield* runtimeRepository.getByThreadId({ threadId }));
        assert.equal(runtime.providerName, "codex");
        assert.equal(runtime.providerInstanceId, "codex");
        assert.equal(runtime.adapterKey, "codex");
      }),
  );

  it.effect("rehydrates persisted mappings across layer restart", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-directory-"));
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const directoryLayer = makeDirectoryLayer(makeSqlitePersistenceLive(dbPath));

      const threadId = ThreadId.make("thread-restart");

      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory;
        yield* directory.upsert({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-restart"),
          threadId,
        });
      }).pipe(Effect.provide(directoryLayer));

      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory;
        const sql = yield* SqlClient.SqlClient;
        yield* directory.listBindings();
        assert.equal(
          yield* directory.matchesOwnership({
            threadId,
            providerInstanceId: ProviderInstanceId.make("codex"),
            sessionLease: ProviderSessionLease.make("lease-restart"),
          }),
          true,
        );
        const provider = yield* directory.getProvider(threadId);
        assert.equal(provider, "codex");

        const resolvedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
        assert.equal(resolvedBinding.threadId, threadId);
        assert.equal(resolvedBinding.provider, "codex");
        assert.equal(resolvedBinding.providerInstanceId, "codex");

        const legacyTableRows = yield* sql<{ readonly name: string }>`
          SELECT name
          FROM sqlite_master
          WHERE type = 'table' AND name = 'provider_sessions'
        `;
        assert.equal(legacyTableRows.length, 0);
      }).pipe(Effect.provide(directoryLayer));

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }),
  );
});

it.effect("serializes a full-row upsert with ownership patches on the same thread", () =>
  Effect.gen(function* () {
    // upsert reads the row and replaces it in a second statement; an
    // ownership patch issued inside that window must not be erased by the
    // stale snapshot, and ownership invalidation must not interleave with the
    // write that would otherwise reinstall the replaced lease. The gate parks
    // upsert's row write mid-window while both operations queue behind it.
    const writeGate = yield* Deferred.make<void>();
    const writeEntered = yield* Deferred.make<void>();
    const armed = yield* Ref.make(false);

    const persistence = SqlitePersistenceMemory;
    const repositoryLayer = ProviderSessionRuntime.layer.pipe(Layer.provide(persistence));
    const gatedRepositoryLayer = Layer.effect(
      ProviderSessionRuntime.ProviderSessionRuntimeRepository,
      Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        return {
          ...repository,
          upsert: (row) =>
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(armed, false)) {
                yield* Deferred.succeed(writeEntered, undefined);
                yield* Deferred.await(writeGate);
              }
              return yield* repository.upsert(row);
            }),
        } satisfies typeof repository;
      }),
    ).pipe(Layer.provide(repositoryLayer));
    const directoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(gatedRepositoryLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-write-serialization");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: ProviderSessionLease.make("lease-a"),
        threadId,
        runtimePayload: { cwd: "/tmp/project" },
      });

      yield* Ref.set(armed, true);
      const fullRowWrite = yield* directory
        .upsert({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-a"),
          threadId,
          status: "stopped",
          runtimePayload: { activeTurnId: null },
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(writeEntered);

      const ownershipPatch = yield* directory
        .updateRuntimePayloadIfOwned({
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-a"),
          runtimePayload: { checkpointRevertIntent: { turnCount: 1 } },
        })
        .pipe(Effect.forkScoped);
      const invalidation = yield* directory.invalidateOwnership(threadId).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.equal(invalidation.pollUnsafe(), undefined);

      yield* Deferred.succeed(writeGate, undefined);
      yield* Fiber.join(fullRowWrite);
      assert.equal(yield* Fiber.join(ownershipPatch), true);
      yield* Fiber.join(invalidation);
      assert.isFalse(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-a"),
        }),
      );

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.deepEqual(binding.runtimePayload, {
        cwd: "/tmp/project",
        activeTurnId: null,
        checkpointRevertIntent: { turnCount: 1 },
      });
      assert.isFalse(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-a"),
        }),
        "reading the persisted binding must not repopulate invalidated volatile ownership",
      );
    }).pipe(Effect.scoped, Effect.provide(directoryLayer));
  }),
);

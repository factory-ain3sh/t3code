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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
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

it.layer(makeDirectoryLayer(SqlitePersistenceMemory))("ProviderSessionDirectoryLive", (it) => {
  it.effect("upserts and reads thread bindings", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const initialThreadId = ThreadId.make("thread-1");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        threadId: initialThreadId,
      });

      const provider = yield* directory.getProvider(initialThreadId);
      assert.equal(provider, "codex");
      const initialBinding = Option.getOrThrow(yield* directory.getBinding(initialThreadId));
      assert.equal(initialBinding.threadId, initialThreadId);
      assert.equal(initialBinding.provider, "codex");
      assert.equal(initialBinding.providerInstanceId, "codex");

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

  it.effect("updates resume cursors only for the owning provider session incarnation", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-owned-cursor");
      const owner = ProviderInstanceId.make("droid-owner");
      const currentLease = ProviderSessionLease.make("lease-current");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: currentLease,
        threadId,
        resumeCursor: { sessionId: "original" },
      });

      assert.equal(
        yield* directory.updateResumeCursorIfOwned({
          threadId,
          providerInstanceId: ProviderInstanceId.make("droid-stale"),
          sessionLease: currentLease,
          resumeCursor: { sessionId: "stale" },
        }),
        false,
      );
      assert.equal(
        yield* directory.updateResumeCursorIfOwned({
          threadId,
          providerInstanceId: owner,
          sessionLease: ProviderSessionLease.make("lease-stale"),
          resumeCursor: { sessionId: "stale" },
        }),
        false,
      );

      // Refused writes leave the persisted cursor untouched.
      const refusedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.deepEqual(refusedBinding.resumeCursor, { sessionId: "original" });

      assert.equal(
        yield* directory.updateResumeCursorIfOwned({
          threadId,
          providerInstanceId: owner,
          sessionLease: currentLease,
          resumeCursor: { sessionId: "current" },
        }),
        true,
      );

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.threadId, threadId);
      assert.equal(binding.provider, "droid");
      assert.equal(binding.providerInstanceId, owner);
      assert.equal(binding.sessionLease, currentLease);
      assert.deepEqual(binding.resumeCursor, { sessionId: "current" });
    }),
  );

  it.effect("updates runtime payload only for the owning provider session incarnation", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-owned-payload");
      const owner = ProviderInstanceId.make("droid-owner");
      const currentLease = ProviderSessionLease.make("lease-current");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: currentLease,
        threadId,
        runtimePayload: { cwd: "/tmp/project", checkpointRevertIntent: null },
      });

      assert.equal(
        yield* directory.updateRuntimePayloadIfOwned({
          threadId,
          providerInstanceId: ProviderInstanceId.make("droid-stale"),
          sessionLease: currentLease,
          runtimePayload: { checkpointRevertIntent: { turnCount: 1 } },
        }),
        false,
      );
      assert.equal(
        yield* directory.updateRuntimePayloadIfOwned({
          threadId,
          providerInstanceId: owner,
          sessionLease: ProviderSessionLease.make("lease-stale"),
          runtimePayload: { checkpointRevertIntent: { turnCount: 1 } },
        }),
        false,
      );

      // Refused writes leave the persisted payload untouched.
      const refusedBinding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.deepEqual(refusedBinding.runtimePayload, {
        cwd: "/tmp/project",
        checkpointRevertIntent: null,
      });

      assert.equal(
        yield* directory.updateRuntimePayloadIfOwned({
          threadId,
          providerInstanceId: owner,
          sessionLease: currentLease,
          runtimePayload: { checkpointRevertIntent: { turnCount: 2 } },
        }),
        true,
      );

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.threadId, threadId);
      assert.equal(binding.provider, "droid");
      assert.equal(binding.providerInstanceId, owner);
      assert.equal(binding.sessionLease, currentLease);
      assert.deepEqual(binding.runtimePayload, {
        cwd: "/tmp/project",
        checkpointRevertIntent: { turnCount: 2 },
      });
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

  it.effect("wipes incarnation payload on lease change but carries the revert intent", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-incarnation-payload");
      const owner = ProviderInstanceId.make("droid-owner");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: ProviderSessionLease.make("lease-a"),
        threadId,
        resumeCursor: { sessionId: "session-a" },
        runtimePayload: {
          cwd: "/tmp/project",
          activeTurnId: "turn-live",
          checkpointRevertIntent: { turnCount: 1, sessionLease: "lease-a" },
        },
      });
      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: ProviderSessionLease.make("lease-b"),
        threadId,
        runtimePayload: { cwd: "/tmp/project" },
      });

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.equal(binding.threadId, threadId);
      assert.equal(binding.provider, "droid");
      assert.equal(binding.providerInstanceId, owner);
      assert.equal(binding.sessionLease, "lease-b");
      // The resume cursor survives a lease change and incarnation-scoped
      // payload does not merge across incarnations, but the persisted revert
      // intent is thread-durable recovery state: it survives the wipe
      // re-stamped to the incoming lease, so the incarnation minted by locked
      // recovery mid-revert inherits the revert obligation.
      assert.deepEqual(binding.resumeCursor, { sessionId: "session-a" });
      assert.deepEqual(binding.runtimePayload, {
        cwd: "/tmp/project",
        checkpointRevertIntent: { turnCount: 1, sessionLease: "lease-b" },
      });
    }),
  );

  it.effect("drops a cleared revert intent with the outgoing incarnation's payload", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      const threadId = ThreadId.make("thread-incarnation-cleared-intent");
      const owner = ProviderInstanceId.make("droid-owner");

      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: ProviderSessionLease.make("lease-a"),
        threadId,
        runtimePayload: { activeTurnId: "turn-live", checkpointRevertIntent: null },
      });
      yield* directory.upsert({
        provider: ProviderDriverKind.make("droid"),
        providerInstanceId: owner,
        sessionLease: ProviderSessionLease.make("lease-b"),
        threadId,
        runtimePayload: { cwd: "/tmp/project" },
      });

      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      // A terminally cleared intent is an empty slot, not durable state; the
      // incarnation wipe must not resurrect it.
      assert.deepEqual(binding.runtimePayload, { cwd: "/tmp/project" });
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
          threadId,
        });
      }).pipe(Effect.provide(directoryLayer));

      yield* Effect.gen(function* () {
        const directory = yield* ProviderSessionDirectory;
        const sql = yield* SqlClient.SqlClient;
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

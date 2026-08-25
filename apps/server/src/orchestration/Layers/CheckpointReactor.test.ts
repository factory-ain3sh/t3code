// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";

import {
  ProviderDriverKind,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderInstanceId,
  ProviderSessionLease,
} from "@t3tools/contracts";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { CheckpointReactorLive } from "./CheckpointReactor.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { RuntimeReceiptBusLive } from "./RuntimeReceiptBus.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { ProviderSessionDirectoryPersistenceError } from "../../provider/Errors.ts";
import { ProviderSessionDirectoryLive } from "../../provider/Layers/ProviderSessionDirectory.ts";
import { ProviderSessionDirectory } from "../../provider/Services/ProviderSessionDirectory.ts";
import { checkpointRefForThreadTurn } from "../../checkpointing/Utils.ts";
import { ServerConfig } from "../../config.ts";
import * as WorkspaceEntries from "../../workspace/WorkspaceEntries.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderDriverKind;
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

function createProviderServiceHarness(
  cwd: string,
  hasSession = true,
  sessionCwd = cwd,
  providerName: ProviderSession["provider"] = ProviderDriverKind.make("codex"),
  onRollbackConversation?: (input: {
    readonly threadId: ThreadId;
    readonly turnIds: ReadonlyArray<TurnId>;
    readonly anchorTurnId?: TurnId;
  }) => Effect.Effect<void>,
  rollbackResumeCursor?: unknown,
) {
  const now = "2026-01-01T00:00:00.000Z";
  let sessionAvailable = hasSession;
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  const runtimeEventSubscriptionReady = Effect.runSync(Deferred.make<void>());
  const sessionLifecycleLock = Effect.runSync(Semaphore.make(1));
  const rollbackConversation = vi.fn(
    (input: {
      readonly threadId: ThreadId;
      readonly turnIds: ReadonlyArray<TurnId>;
      readonly anchorTurnId?: TurnId;
    }) =>
      Effect.gen(function* () {
        // Faithful to the real ProviderService, which rejects an empty target
        // without an anchor before session routing.
        if (input.turnIds.length === 0 && input.anchorTurnId === undefined) {
          return yield* Effect.die(
            new Error("Rollback target must include at least one turn ID or an anchor turn ID."),
          );
        }
        if (onRollbackConversation) {
          yield* onRollbackConversation(input);
        }
        return {
          threadId: input.threadId,
          turns: input.turnIds.map((id) => ({ id, items: [] })),
          ...(rollbackResumeCursor !== undefined ? { resumeCursor: rollbackResumeCursor } : {}),
        };
      }),
  );

  const unsupported = <A>() =>
    Effect.die(new Error("Unsupported provider call in test")) as Effect.Effect<A, never>;
  const listSessions = () =>
    sessionAvailable
      ? Effect.succeed([
          {
            provider: providerName,
            providerInstanceId: ProviderInstanceId.make(providerName),
            status: "ready",
            runtimeMode: "full-access",
            threadId: ThreadId.make("thread-1"),
            cwd: sessionCwd,
            createdAt: now,
            updatedAt: now,
          },
        ] satisfies ReadonlyArray<ProviderSession>)
      : Effect.succeed([] as ReadonlyArray<ProviderSession>);
  const recoverSession = vi.fn(() =>
    Effect.sync(() => {
      sessionAvailable = true;
      return {
        provider: providerName,
        providerInstanceId: ProviderInstanceId.make(providerName),
        status: "ready",
        runtimeMode: "full-access",
        threadId: ThreadId.make("thread-1"),
        cwd: sessionCwd,
        createdAt: now,
        updatedAt: now,
      } satisfies ProviderSession;
    }),
  );
  const service: ProviderServiceShape = {
    startSession: () => unsupported(),
    sendTurn: () => unsupported(),
    interruptTurn: () => unsupported(),
    respondToRequest: () => unsupported(),
    respondToUserInput: () => unsupported(),
    stopSession: () => unsupported(),
    listSessions,
    recoverSession,
    withSessionLifecycleLock: (_threadId, effect) => sessionLifecycleLock.withPermit(effect),
    getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
    getInstanceInfo: (instanceId) =>
      Effect.succeed({
        instanceId,
        driverKind: ProviderDriverKind.make(providerName),
        displayName: undefined,
        enabled: true,
        continuationIdentity: {
          driverKind: ProviderDriverKind.make(providerName),
          continuationKey: `${providerName}:instance:${instanceId}`,
        },
      }),
    rollbackConversation,
    uploadFeedback: () => unsupported(),
    streamEvents: Stream.unwrap(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(runtimeEventPubSub);
        yield* Deferred.succeed(runtimeEventSubscriptionReady, undefined);
        return Stream.fromSubscription(subscription);
      }),
    ),
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    Effect.runSync(PubSub.publish(runtimeEventPubSub, event as unknown as ProviderRuntimeEvent));
  };

  return {
    service,
    rollbackConversation,
    recoverSession,
    emit,
    awaitSubscription: Deferred.await(runtimeEventSubscriptionReady),
  };
}

async function waitForThread(
  readModel: () => Promise<{
    readonly threads: ReadonlyArray<{
      readonly id: ThreadId;
      readonly latestTurn: { readonly turnId: string } | null;
      readonly checkpoints: ReadonlyArray<{ readonly checkpointTurnCount: number }>;
      readonly activities: ReadonlyArray<{ readonly kind: string }>;
    }>;
  }>,
  predicate: (thread: {
    latestTurn: { turnId: string } | null;
    checkpoints: ReadonlyArray<{ checkpointTurnCount: number }>;
    activities: ReadonlyArray<{ kind: string }>;
  }) => boolean,
  timeoutMs = 15_000,
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<{
    latestTurn: { turnId: string } | null;
    checkpoints: ReadonlyArray<{ checkpointTurnCount: number }>;
    activities: ReadonlyArray<{ kind: string }>;
  }> => {
    const snapshot = await readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    if (thread && predicate(thread)) {
      return thread;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for thread state.");
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

async function waitForEvent(
  engine: OrchestrationEngineShape,
  predicate: (event: { type: string }) => boolean,
  timeoutMs = 15_000,
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async () => {
    const events = await Effect.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(Effect.map((chunk) => Array.from(chunk))),
    );
    if (events.some(predicate)) {
      return events;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for orchestration event.");
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

function runGit(cwd: string, args: ReadonlyArray<string>) {
  return NodeChildProcess.execFileSync("git", args, {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    encoding: "utf8",
  });
}

function createGitRepository() {
  const cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-handler-"));
  runGit(cwd, ["init", "--initial-branch=main"]);
  runGit(cwd, ["config", "user.email", "test@example.com"]);
  runGit(cwd, ["config", "user.name", "Test User"]);
  NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v1\n", "utf8");
  runGit(cwd, ["add", "."]);
  runGit(cwd, ["commit", "-m", "Initial"]);
  return cwd;
}

function gitRefExists(cwd: string, ref: string): boolean {
  try {
    runGit(cwd, ["show-ref", "--verify", "--quiet", ref]);
    return true;
  } catch {
    return false;
  }
}

function gitShowFileAtRef(cwd: string, ref: string, filePath: string): string {
  return runGit(cwd, ["show", `${ref}:${filePath}`]);
}

function makeGate() {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, promise };
}

async function waitForGitRefExists(cwd: string, ref: string, timeoutMs = 15_000) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<void> => {
    if (gitRefExists(cwd, ref)) {
      return;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error(`Timed out waiting for git ref '${ref}'.`);
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

describe("CheckpointReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | CheckpointReactor
    | CheckpointStore.CheckpointStore
    | ProjectionSnapshotQuery,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  const tempDirs: string[] = [];

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
    while (tempDirs.length > 0) {
      const dir = tempDirs.pop();
      if (dir) {
        NodeFS.rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  async function createHarness(options?: {
    readonly hasSession?: boolean;
    readonly seedFilesystemCheckpoints?: boolean;
    readonly projectWorkspaceRoot?: string;
    readonly threadWorktreePath?: string | null;
    readonly threadBranch?: string | null;
    readonly secondThreadSharingWorktree?: boolean;
    readonly localStatusRefName?: string | null;
    readonly providerSessionCwd?: string;
    readonly providerName?: ProviderDriverKind;
    readonly gitStatusRefreshCalls?: Array<string>;
    readonly pendingRevertRecovery?: boolean;
    readonly pendingRevertStaleCheckpointRefs?: ReadonlyArray<string>;
    readonly pendingRevertStaleTurnIds?: ReadonlyArray<string>;
    readonly pendingRevertBindingLease?: null;
    readonly reLeaseDuringRollback?: boolean;
    readonly skipStartupRecovery?: boolean;
    readonly holdLifecycleLockOnStart?: boolean;
    readonly replacementBeforeRecovery?: boolean;
    readonly onRollbackConversation?: (input: {
      readonly threadId: ThreadId;
      readonly turnIds: ReadonlyArray<TurnId>;
      readonly anchorTurnId?: TurnId;
    }) => Effect.Effect<void>;
    readonly rollbackResumeCursor?: unknown;
    readonly persistedSessionBinding?: boolean;
    readonly replaceBindingDuringRollback?: boolean;
    readonly failResumeCursorUpdate?: boolean;
    readonly rejectRevertIntentPersistence?: boolean;
    readonly failFirstRevertCompleteDispatch?: boolean;
  }) {
    const cwd = createGitRepository();
    tempDirs.push(cwd);
    let replaceBindingDuringRollback: Effect.Effect<void> = Effect.void;
    const provider = createProviderServiceHarness(
      cwd,
      options?.hasSession ?? true,
      options?.providerSessionCwd ?? cwd,
      options?.providerName ?? ProviderDriverKind.make("codex"),
      (input) =>
        (options?.onRollbackConversation?.(input) ?? Effect.void).pipe(
          Effect.andThen(Effect.suspend(() => replaceBindingDuringRollback)),
        ),
      options?.rollbackResumeCursor,
    );
    const baseOrchestrationLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    // Simulates a crash between the destructive revert phase and its
    // bookkeeping: the first thread.revert.complete dispatch dies as if the
    // process were killed right before the read model learned about the
    // revert. Persisted state (intent, tree, refs) is exactly what a real
    // crash leaves behind.
    const orchestrationLayer = options?.failFirstRevertCompleteDispatch
      ? Layer.effect(
          OrchestrationEngineService,
          Effect.gen(function* () {
            const engine = yield* Effect.service(OrchestrationEngineService);
            let injected = false;
            return OrchestrationEngineService.of({
              ...engine,
              dispatch: (command) => {
                if (!injected && command.type === "thread.revert.complete") {
                  injected = true;
                  return Effect.die(new Error("Injected crash between restore and bookkeeping"));
                }
                return engine.dispatch(command);
              },
            });
          }),
        ).pipe(Layer.provide(baseOrchestrationLayer))
      : baseOrchestrationLayer;
    const projectionSnapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );

    const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-checkpoint-reactor-test-",
    });
    const vcsStatusBroadcasterLayer = Layer.succeed(VcsStatusBroadcaster, {
      getStatus: () => Effect.die("getStatus should not be called in this test"),
      refreshLocalStatus: (cwd: string) =>
        Effect.sync(() => {
          options?.gitStatusRefreshCalls?.push(cwd);
        }).pipe(
          Effect.as({
            isRepo: true,
            hasPrimaryRemote: false,
            isDefaultRef: true,
            refName:
              options?.localStatusRefName !== undefined ? options.localStatusRefName : "main",
            hasWorkingTreeChanges: false,
            workingTree: { files: [], insertions: 0, deletions: 0 },
          }),
        ),
      refreshStatus: () => Effect.die("refreshStatus should not be called in this test"),
      streamStatus: () => Stream.empty,
    });
    const providerSessionDirectoryBaseLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistenceMemory))),
    );
    const providerSessionDirectoryLayer =
      options?.failResumeCursorUpdate || options?.rejectRevertIntentPersistence
        ? Layer.effect(
            ProviderSessionDirectory,
            Effect.gen(function* () {
              const directory = yield* Effect.service(ProviderSessionDirectory);
              return ProviderSessionDirectory.of({
                ...directory,
                ...(options?.failResumeCursorUpdate
                  ? {
                      updateResumeCursorIfOwned: () =>
                        Effect.fail(
                          new ProviderSessionDirectoryPersistenceError({
                            operation: "updateResumeCursorIfOwned",
                            detail: "Injected cursor persistence failure",
                          }),
                        ),
                    }
                  : {}),
                ...(options?.rejectRevertIntentPersistence
                  ? {
                      updateRuntimePayloadIfOwned: (input) => {
                        const runtimePayload = input.runtimePayload;
                        return runtimePayload !== null &&
                          typeof runtimePayload === "object" &&
                          "checkpointRevertIntent" in runtimePayload
                          ? Effect.succeed(false)
                          : directory.updateRuntimePayloadIfOwned(input);
                      },
                    }
                  : {}),
              });
            }),
          ).pipe(Layer.provide(providerSessionDirectoryBaseLayer))
        : providerSessionDirectoryBaseLayer;

    const checkpointReactorLayer = CheckpointReactorLive.pipe(
      Layer.provideMerge(orchestrationLayer),
      Layer.provideMerge(projectionSnapshotLayer),
      Layer.provideMerge(RuntimeReceiptBusLive),
      Layer.provideMerge(Layer.succeed(ProviderService, provider.service)),
      Layer.provideMerge(providerSessionDirectoryLayer),
      Layer.provideMerge(vcsStatusBroadcasterLayer),
      Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))),
      Layer.provideMerge(
        WorkspaceEntries.layer.pipe(
          Layer.provide(WorkspacePaths.layer),
          Layer.provideMerge(VcsDriverRegistry.layer),
        ),
      ),
      Layer.provideMerge(WorkspacePaths.layer),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(ServerConfigLayer),
      Layer.provideMerge(NodeServices.layer),
    );
    const layer = Layer.merge(checkpointReactorLayer, providerSessionDirectoryLayer);

    const managedRuntime = ManagedRuntime.make(layer);
    runtime = managedRuntime;
    const engine = await managedRuntime.runPromise(Effect.service(OrchestrationEngineService));
    const snapshotQuery = await managedRuntime.runPromise(Effect.service(ProjectionSnapshotQuery));
    const reactor = await managedRuntime.runPromise(Effect.service(CheckpointReactor));
    const providerSessionDirectory = await managedRuntime.runPromise(
      Effect.service(ProviderSessionDirectory),
    );
    const checkpointStore = await managedRuntime.runPromise(
      Effect.service(CheckpointStore.CheckpointStore),
    );
    if (options?.replaceBindingDuringRollback) {
      replaceBindingDuringRollback = providerSessionDirectory
        .upsert({
          threadId: ThreadId.make("thread-1"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-b"),
          resumeCursor: { sessionId: "session-a" },
          runtimePayload: { cwd },
        })
        .pipe(Effect.orDie);
    }
    // Models the real rollbackConversation's locked recovery of a stopped,
    // unowned session: same instance, fresh lease minted mid-rollback.
    if (options?.reLeaseDuringRollback) {
      replaceBindingDuringRollback = providerSessionDirectory
        .upsert({
          threadId: ThreadId.make("thread-1"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-recovered"),
          runtimePayload: { cwd },
        })
        .pipe(Effect.orDie);
    }
    const drain = () => Effect.runPromise(reactor.drain);

    const createdAt = "2026-01-01T00:00:00.000Z";
    const sessionLease = ProviderSessionLease.make("lease-a");
    if (options?.persistedSessionBinding ?? options?.hasSession ?? true) {
      await runtime.runPromise(
        providerSessionDirectory.upsert({
          threadId: ThreadId.make("thread-1"),
          provider: options?.providerName ?? ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make(
            options?.providerName ?? ProviderDriverKind.make("codex"),
          ),
          sessionLease,
          resumeCursor: { sessionId: "session-a" },
          runtimePayload: { cwd: options?.providerSessionCwd ?? cwd },
        }),
      );
    }
    await Effect.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-create"),
        projectId: asProjectId("project-1"),
        title: "Test Project",
        workspaceRoot: options?.projectWorkspaceRoot ?? cwd,
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await Effect.runPromise(
      engine
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create"),
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: options?.threadBranch ?? null,
          worktreePath: options?.threadWorktreePath ?? cwd,
          createdAt,
        })
        .pipe(
          options?.secondThreadSharingWorktree
            ? Effect.andThen(
                engine.dispatch({
                  type: "thread.create",
                  commandId: CommandId.make("cmd-thread-create-2"),
                  threadId: ThreadId.make("thread-2"),
                  projectId: asProjectId("project-1"),
                  title: "Thread 2",
                  modelSelection: {
                    instanceId: ProviderInstanceId.make("codex"),
                    model: "gpt-5-codex",
                  },
                  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                  runtimeMode: "approval-required",
                  branch: null,
                  worktreePath: options?.threadWorktreePath ?? cwd,
                  createdAt,
                }),
              )
            : Effect.asVoid,
        ),
    );

    if (options?.seedFilesystemCheckpoints ?? true) {
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        }),
      );
    }

    if (options?.pendingRevertRecovery) {
      await runtime.runPromise(
        providerSessionDirectory.upsert({
          threadId: ThreadId.make("thread-1"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: options?.pendingRevertBindingLease === null ? null : sessionLease,
          runtimePayload: {
            checkpointRevertIntent: {
              commandId: CommandId.make("server:checkpoint-revert-complete:recovery"),
              threadId: ThreadId.make("thread-1"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              sessionLease,
              cwd,
              turnCount: 1,
              turnIds: [asTurnId("turn-1")],
              anchorTurnId: asTurnId("turn-2"),
              checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
              staleCheckpointRefs: options?.pendingRevertStaleCheckpointRefs ?? [
                checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
              ],
              staleTurnIds: options?.pendingRevertStaleTurnIds ?? [asTurnId("turn-2")],
              createdAt,
            },
          },
        }),
      );
    }

    if (options?.replacementBeforeRecovery) {
      await runtime.runPromise(
        providerSessionDirectory.upsert({
          threadId: ThreadId.make("thread-1"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-b"),
          resumeCursor: { sessionId: "session-b" },
          runtimePayload: { owner: "b" },
        }),
      );
    }

    // Holds thread-1's lifecycle lock across reactor start so a recovery
    // replay stays parked while the test advances the read model first.
    let releaseLifecycleLock: (() => void) | undefined;
    if (options?.holdLifecycleLockOnStart) {
      const lockHeld = makeGate();
      const lockRelease = makeGate();
      void Effect.runPromise(
        provider.service.withSessionLifecycleLock(
          ThreadId.make("thread-1"),
          Effect.sync(lockHeld.open).pipe(
            Effect.andThen(Effect.promise(() => lockRelease.promise)),
          ),
        ),
      );
      await lockHeld.promise;
      releaseLifecycleLock = lockRelease.open;
    }

    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(reactor.start().pipe(Scope.provide(scope)));
    await Effect.runPromise(provider.awaitSubscription);
    // Mirrors the startup phases: subscriptions from start(), then the
    // persisted-intent replay as its own later phase.
    if (options?.skipStartupRecovery !== true) {
      await Effect.runPromise(reactor.recoverPersistedIntents());
    }

    // Tears down the reactor's start scope and starts it again over the same
    // persisted state, the in-process equivalent of a server restart: startup
    // recovery re-reads the bindings and replays any retained revert intent.
    const restartReactor = async () => {
      if (scope) {
        await Effect.runPromise(Scope.close(scope, Exit.void));
      }
      scope = await Effect.runPromise(Scope.make("sequential"));
      await Effect.runPromise(reactor.start().pipe(Scope.provide(scope)));
      await Effect.runPromise(provider.awaitSubscription);
      await Effect.runPromise(reactor.recoverPersistedIntents());
    };

    return {
      engine,
      dispatch: (command: Parameters<typeof engine.dispatch>[0]) =>
        managedRuntime.runPromise(engine.dispatch(command)),
      readModel: () => Effect.runPromise(snapshotQuery.getSnapshot()),
      provider,
      providerSessionDirectory,
      runtime: managedRuntime,
      sessionLease,
      cwd,
      drain,
      releaseLifecycleLock,
      restartReactor,
      recoverPersistedIntents: () => Effect.runPromise(reactor.recoverPersistedIntents()),
    };
  }

  it("captures pre-turn baseline on turn.started and post-turn checkpoint on turn.completed", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-capture"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-1"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-1"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-1" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0)),
    ).toBe(true);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        "README.md",
      ),
    ).toBe("v1\n");
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        "README.md",
      ),
    ).toBe("v2\n");
  });

  it("ignores lifecycle events emitted by a stale provider session incarnation", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "stale\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-stale-lease"),
      provider: ProviderDriverKind.make("codex"),
      sessionLease: ProviderSessionLease.make("lease-stale"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-stale"),
      payload: { state: "completed" },
    });

    await harness.drain();

    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(false);
  });

  it("refreshes local git status state on turn completion using the session cwd", async () => {
    const gitStatusRefreshCalls: string[] = [];
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      gitStatusRefreshCalls,
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-refresh-local-status"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-refresh-local-status"),
      payload: { state: "completed" },
    });

    await harness.drain();

    expect(gitStatusRefreshCalls).toEqual([harness.cwd]);
  });

  it("adopts a drifted checkout as the thread branch on a dedicated worktree", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/renamed-by-agent",
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift"),
      payload: { state: "completed" },
    });

    await harness.drain();
    await waitForEvent(
      harness.engine,
      (event) =>
        event.type === "thread.meta-updated" &&
        (event as unknown as { payload: { branch?: string } }).payload.branch ===
          "t3code/renamed-by-agent",
    );

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/renamed-by-agent");
  });

  it("does not adopt a drifted checkout when the worktree is shared by another thread", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/renamed-by-agent",
      secondThreadSharingWorktree: true,
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift-shared"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift-shared"),
      payload: { state: "completed" },
    });

    await harness.drain();

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/original-branch");
  });

  it("does not adopt a temporary placeholder checkout as the thread branch", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/0a1b2c3d",
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift-temp"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift-temp"),
      payload: { state: "completed" },
    });

    await harness.drain();

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/original-branch");
  });

  it("ignores auxiliary thread turn completion while primary turn is active", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-primary-running"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-main"),
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-main"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-main"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-aux"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-aux"),
      payload: { state: "completed" },
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(midThread?.checkpoints).toHaveLength(0);

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-main"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-main"),
      payload: { state: "completed" },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-main" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
  });

  it("captures pre-turn and completion checkpoints for claude runtime events", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      providerName: ProviderDriverKind.make("claudeAgent"),
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-capture-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "claudeAgent",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-claude-1"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-1"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-claude-1"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-1"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-claude-1" && entry.checkpoints.length === 1,
    );

    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
  });

  it("appends capture failure activity when turn diff summary cannot be derived", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-baseline-diff"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-missing-baseline"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-missing-baseline"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.checkpoints.length === 1 &&
        entry.activities.some((activity) => activity.kind === "checkpoint.capture.failed"),
    );

    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      thread.activities.some((activity) => activity.kind === "checkpoint.capture.failed"),
    ).toBe(true);
  });

  it("captures pre-turn baseline from project workspace root when thread worktree is unset", async () => {
    const harness = await createHarness({
      hasSession: false,
      seedFilesystemCheckpoints: false,
      threadWorktreePath: null,
    });

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-for-baseline"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: MessageId.make("message-user-1"),
          role: "user",
          text: "start turn",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        "README.md",
      ),
    ).toBe("v1\n");
  });

  it("captures turn completion checkpoint from project workspace root when provider session cwd is unavailable", async () => {
    const harness = await createHarness({
      hasSession: false,
      seedFilesystemCheckpoints: false,
      threadWorktreePath: null,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-provider-cwd"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-missing-cwd"),
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-missing-provider-cwd"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-missing-cwd"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        "README.md",
      ),
    ).toBe("v2\n");
  });

  it("ignores non-v2 checkpoint.captured runtime events", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-checkpoint-captured"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "checkpoint.captured",
      eventId: EventId.make("evt-checkpoint-captured-3"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-3"),
      turnCount: 3,
      status: "completed",
    });

    await harness.drain();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.checkpoints.some((checkpoint) => checkpoint.checkpointTurnCount === 3)).toBe(
      false,
    );
  });

  it("continues processing runtime events after a single checkpoint runtime failure", async () => {
    const nonRepositorySessionCwd = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-runtime-non-repo-"),
    );
    tempDirs.push(nonRepositorySessionCwd);

    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      providerSessionCwd: nonRepositorySessionCwd,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-non-repo-runtime"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-runtime-capture-failure"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-runtime-failure"),
      payload: { state: "completed" },
    });

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-after-runtime-failure"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-after-runtime-failure"),
    });

    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0)),
    ).toBe(true);
  });

  it("executes provider revert and emits thread.reverted for checkpoint revert requests", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-1"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-2"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-2"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        status: "ready",
        files: [],
        checkpointTurnCount: 2,
        createdAt,
      }),
    );

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-request"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.checkpoints.length === 1,
    );

    expect(thread.latestTurn?.turnId).toBe("turn-1");
    expect(thread.checkpoints).toHaveLength(1);
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(false);
  });

  it("executes provider revert and emits thread.reverted for claude sessions", async () => {
    const harness = await createHarness({ providerName: ProviderDriverKind.make("claudeAgent") });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-claude"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "ready",
        providerName: "claudeAgent",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });

    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-diff-claude-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-diff-claude-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-request-claude"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      turnIds: [asTurnId("turn-claude-1")],
      anchorTurnId: asTurnId("turn-claude-2"),
    });
  });

  it("parks persisted intents until startup recovery is invoked", async () => {
    // Recovery is a dedicated startup phase sequenced after provider-session
    // reconciliation. Replaying from start() would race the orphan sweep,
    // which can stomp a freshly recovered lease with its stale liveness
    // snapshot.
    const harness = await createHarness({
      pendingRevertRecovery: true,
      skipStartupRecovery: true,
    });

    await harness.drain();
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    const parked = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(parked)).toBe(true);
    if (Option.isSome(parked)) {
      expect(parked.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { turnCount: 1 },
      });
    }

    await harness.recoverPersistedIntents();
    await harness.drain();
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
  });

  it("recovers a retained intent after a graceful shutdown cleared the session lease", async () => {
    // runStopAll nulls every binding lease on graceful shutdown while the
    // merged runtime payload keeps the intent. The replay must treat the
    // unowned session as recoverable, not as a replacement to clear against;
    // rollbackConversation's locked recovery then mints the fresh lease the
    // completion bookkeeping lands on.
    const harness = await createHarness({
      pendingRevertRecovery: true,
      pendingRevertBindingLease: null,
      reLeaseDuringRollback: true,
    });

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(false);
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.sessionLease).toBe(ProviderSessionLease.make("lease-recovered"));
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }
  });

  it("recovers a persisted revert intent and clears it after completion", async () => {
    const harness = await createHarness({ pendingRevertRecovery: true });

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(false);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }
  });

  it("persists a recovered rewind cursor against the replacement session lease", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      replaceBindingDuringRollback: true,
      rollbackResumeCursor: { sessionId: "session-rewound" },
    });

    await harness.drain();

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value).toMatchObject({
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: ProviderSessionLease.make("lease-b"),
        resumeCursor: { sessionId: "session-rewound" },
      });
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }
  });

  it("recovers a stopped persisted session before a requested checkpoint revert", async () => {
    const harness = await createHarness({
      hasSession: false,
      persistedSessionBinding: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-recovery-diff-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-recovery-diff-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-recovery-revert"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });

    await harness.drain();

    expect(harness.provider.recoverSession).toHaveBeenCalledWith(ThreadId.make("thread-1"));
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
  });

  it("replays an inherited revert intent against a same-thread replacement session", async () => {
    // A replacement session on the same thread resumes the same conversation,
    // so the directory stamps the retained intent to the replacement's lease
    // and the replay converges on the accepted revert instead of stranding a
    // half-executed one behind a "session changed" receipt. Real staleness
    // stays guarded: the turn-identity fence cancels over newer completed
    // turns, and provider/instance drift discards the intent terminally.
    const harness = await createHarness({
      pendingRevertRecovery: true,
      replacementBeforeRecovery: true,
    });

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(false);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value).toMatchObject({
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: ProviderSessionLease.make("lease-b"),
        resumeCursor: { sessionId: "session-b" },
      });
      expect(binding.value.runtimePayload).toMatchObject({
        owner: "b",
        checkpointRevertIntent: null,
      });
    }
  });

  it("preserves a replacement session's newer revert intent when a stale intent clears terminally", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      holdLifecycleLockOnStart: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    // The recovered stale intent is parked on the lifecycle lock. A
    // replacement session takes ownership and persists its own revert intent
    // into the same slot before the stale replay gets to run.
    const newerCommandId = CommandId.make("server:checkpoint-revert-complete:newer");
    await harness.runtime.runPromise(
      harness.providerSessionDirectory.upsert({
        threadId: ThreadId.make("thread-1"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: ProviderSessionLease.make("lease-b"),
        resumeCursor: { sessionId: "session-b" },
        runtimePayload: {
          checkpointRevertIntent: {
            commandId: newerCommandId,
            threadId: ThreadId.make("thread-1"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            sessionLease: ProviderSessionLease.make("lease-b"),
            cwd: harness.cwd,
            turnCount: 1,
            turnIds: [asTurnId("turn-1")],
            anchorTurnId: asTurnId("turn-2"),
            checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
            staleCheckpointRefs: [checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)],
            staleTurnIds: [asTurnId("turn-2")],
            createdAt,
          },
        },
      }),
    );

    harness.releaseLifecycleLock?.();
    await harness.drain();

    // The stale intent's terminal clear is identity-guarded: it must surface
    // its failure receipt without erasing the replacement's newer intent.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.sessionLease).toBe(ProviderSessionLease.make("lease-b"));
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { commandId: newerCommandId },
      });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("does not clear a replacement owner's binding after an in-flight recovered revert", async () => {
    const rollbackEntered = makeGate();
    const allowRollback = makeGate();
    let verifyRollbackOwnership: Effect.Effect<void> = Effect.void;
    const harness = await createHarness({
      pendingRevertRecovery: true,
      onRollbackConversation: () =>
        Effect.sync(rollbackEntered.open).pipe(
          Effect.andThen(Effect.promise(() => allowRollback.promise)),
          Effect.andThen(Effect.suspend(() => verifyRollbackOwnership)),
        ),
    });

    await rollbackEntered.promise;
    verifyRollbackOwnership = harness.providerSessionDirectory
      .getBinding(ThreadId.make("thread-1"))
      .pipe(
        Effect.tap((binding) =>
          Effect.sync(() => {
            expect(Option.isSome(binding)).toBe(true);
            if (Option.isSome(binding)) {
              expect(binding.value.sessionLease).toBe(ProviderSessionLease.make("lease-a"));
            }
          }),
        ),
        Effect.asVoid,
        Effect.orDie,
      );
    const replacement = harness.runtime.runPromise(
      harness.provider.service.withSessionLifecycleLock(
        ThreadId.make("thread-1"),
        harness.providerSessionDirectory.upsert({
          threadId: ThreadId.make("thread-1"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex-b"),
          sessionLease: ProviderSessionLease.make("lease-b"),
          resumeCursor: { sessionId: "session-b" },
          runtimePayload: { owner: "b" },
        }),
      ),
    );
    allowRollback.open();
    await replacement;
    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value).toMatchObject({
        providerInstanceId: ProviderInstanceId.make("codex-b"),
        sessionLease: ProviderSessionLease.make("lease-b"),
        resumeCursor: { sessionId: "session-b" },
        runtimePayload: { owner: "b" },
      });
    }
  });

  it("processes consecutive revert requests with deterministic rollback sequencing", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-inline-revert"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });

    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-inline-revert-diff-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-inline-revert-diff-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-sequenced-revert-request-1"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-sequenced-revert-request-0"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 0,
      createdAt,
    });

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(2);
    expect(harness.provider.rollbackConversation.mock.calls[0]?.[0]).toEqual({
      threadId: ThreadId.make("thread-1"),
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(harness.provider.rollbackConversation.mock.calls[1]?.[0]).toEqual({
      threadId: ThreadId.make("thread-1"),
      turnIds: [],
      anchorTurnId: asTurnId("turn-1"),
    });
  });

  it("completes a turn-zero revert on a thread with no checkpoints without a provider rollback", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-empty-revert"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-empty-target"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 0,
      createdAt,
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    // The provider recorded no turns, so the intent carries an empty target
    // that must never reach ProviderService (which rejects it) and the revert
    // completes as a pure tree restore.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(false);
  });

  it("appends an error activity when revert is requested without an active session", async () => {
    const harness = await createHarness({ hasSession: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-no-session"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    );

    expect(thread.activities.some((activity) => activity.kind === "checkpoint.revert.failed")).toBe(
      true,
    );
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
  });

  it("appends an error activity when revert intent loses session ownership", async () => {
    const harness = await createHarness({ rejectRevertIntentPersistence: true });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-lost-ownership"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 0,
      createdAt,
    });
    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
  });

  it("retains a recovered revert intent when its rewind cursor fails to persist", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failResumeCursorUpdate: true,
    });

    await harness.drain();

    // The destructive phase completed but the rewound cursor never persisted.
    // The intent is the recovery state that reconciles the rewound provider
    // with the stored cursor, so it survives for the startup replay (which is
    // safe: the rollback replay is a no-op, the restore is idempotent, and
    // the execute-time progress fence blocks replay over newer work).
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.resumeCursor).toEqual({ sessionId: "session-a" });
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { threadId: "thread-1" },
      });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("replays a retained intent to convergence after a crash between restore and bookkeeping", async () => {
    const harness = await createHarness({
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failFirstRevertCompleteDispatch: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-crash-replay"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-crash-replay-diff-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-crash-replay-diff-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-crash-replay-revert"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });
    await harness.drain();

    // First pass: the destructive phase finished (provider rolled back, tree
    // restored, stale ref deleted) and the injected crash landed before the
    // read model learned about the revert. The intent is the durable recovery
    // state a real SIGKILL would leave behind.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const retained = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(retained)).toBe(true);
    if (Option.isSome(retained)) {
      expect(retained.value.resumeCursor).toEqual({ sessionId: "session-a" });
      expect(retained.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { threadId: "thread-1" },
      });
    }
    const eventsBeforeReplay = await harness.runtime.runPromise(
      Stream.runCollect(harness.engine.readEvents(0)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      ),
    );
    expect(eventsBeforeReplay.filter((event) => event.type === "thread.reverted")).toHaveLength(0);

    await harness.restartReactor();
    await harness.drain();

    // Startup replay converges: the equal-target rollback replays, the restore
    // is idempotent, exactly one thread.reverted lands, the rewound cursor
    // persists, and the intent clears terminally.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(2);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const eventsAfterReplay = await harness.runtime.runPromise(
      Stream.runCollect(harness.engine.readEvents(0)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      ),
    );
    expect(eventsAfterReplay.filter((event) => event.type === "thread.reverted")).toHaveLength(1);
    const converged = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(converged)).toBe(true);
    if (Option.isSome(converged)) {
      expect(converged.value.resumeCursor).toEqual({ sessionId: "session-rewound" });
      expect(converged.value.runtimePayload).toMatchObject({ checkpointRevertIntent: null });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.checkpoints.every((checkpoint) => checkpoint.checkpointTurnCount <= 1)).toBe(
      true,
    );
  });

  it("replays to convergence after a crash when recovery re-leased the session mid-rollback", async () => {
    const harness = await createHarness({
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failFirstRevertCompleteDispatch: true,
      reLeaseDuringRollback: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-release-crash"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-release-crash-diff-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-release-crash-diff-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-release-crash-revert"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });
    await harness.drain();

    // First pass: locked recovery re-leased the session mid-rollback, the
    // destructive phase finished, and the injected crash landed before
    // bookkeeping. The directory carried the intent across the incarnation
    // wipe re-stamped to the fresh lease, so the restart replay still owns
    // the session it must reconcile.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const retained = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(retained)).toBe(true);
    if (Option.isSome(retained)) {
      expect(retained.value.sessionLease).toBe(ProviderSessionLease.make("lease-recovered"));
      expect(retained.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: {
          threadId: "thread-1",
          sessionLease: "lease-recovered",
        },
      });
    }
    const eventsBeforeReplay = await harness.runtime.runPromise(
      Stream.runCollect(harness.engine.readEvents(0)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      ),
    );
    expect(eventsBeforeReplay.filter((event) => event.type === "thread.reverted")).toHaveLength(0);

    await harness.restartReactor();
    await harness.drain();

    // The replay executes under the inherited lease: the equal-target
    // rollback is a no-op, the restore is idempotent, exactly one
    // thread.reverted lands, and the intent clears terminally.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(2);
    const eventsAfterReplay = await harness.runtime.runPromise(
      Stream.runCollect(harness.engine.readEvents(0)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      ),
    );
    expect(eventsAfterReplay.filter((event) => event.type === "thread.reverted")).toHaveLength(1);
    const converged = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(converged)).toBe(true);
    if (Option.isSome(converged)) {
      expect(converged.value.resumeCursor).toEqual({ sessionId: "session-rewound" });
      expect(converged.value.runtimePayload).toMatchObject({ checkpointRevertIntent: null });
    }
  });

  it("clears a missing-checkpoint revert intent before rolling back the provider", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      seedFilesystemCheckpoints: false,
    });

    await harness.drain();

    // The ref pre-check proves the checkpoint exists before anything
    // destructive runs: a deterministically missing ref clears the intent
    // terminally with the provider conversation untouched.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({ checkpointRevertIntent: null });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("clears a recovered revert intent terminally when the provider rollback fails", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      onRollbackConversation: () => Effect.die(new Error("rollback exploded")),
    });

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    // The rollback runs before the filesystem restore, so a failing rollback
    // leaves the working tree and the stale checkpoint refs untouched.
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("rejects a revert request while a turn is active", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-active-turn"),
      threadId: ThreadId.make("thread-1"),
      session: {
        threadId: ThreadId.make("thread-1"),
        status: "running",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: asTurnId("turn-live"),
        lastError: null,
        updatedAt: createdAt,
      },
      createdAt,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-active-turn-diff-1"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt,
    });

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-mid-turn"),
      threadId: ThreadId.make("thread-1"),
      turnCount: 1,
      createdAt,
    });

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    );

    expect(thread.activities.some((activity) => activity.kind === "checkpoint.revert.failed")).toBe(
      true,
    );
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
  });

  it("cancels a recovered revert intent after the thread progressed past it", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      pendingRevertStaleCheckpointRefs: [],
      pendingRevertStaleTurnIds: [],
      holdLifecycleLockOnStart: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    // The recovery replay is parked on the lifecycle lock; land a checkpoint
    // the intent never accounted for before letting it run.
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-progressed-diff-2"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length >= 1);
    harness.releaseLifecycleLock?.();
    await harness.drain();

    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("treats a new turn on a recycled checkpoint ref as progress, not staleness", async () => {
    // Checkpoint refs are deterministic per turn count. After a revert whose
    // bookkeeping stopped between thread.revert.complete and the intent
    // clear, a new turn recycles a discarded ref, so a ref-based progress
    // fence would replay the rollback over the new work.
    const harness = await createHarness({
      pendingRevertRecovery: true,
      holdLifecycleLockOnStart: true,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    // The recovery replay is parked on the lifecycle lock; land a NEW turn's
    // checkpoint on the ref the intent recorded as stale.
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-recycled-ref-diff"),
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-2-replacement"),
      completedAt: createdAt,
      checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt,
    });
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length >= 1);
    harness.releaseLifecycleLock?.();
    await harness.drain();

    // Turn identity, not the recycled ref, decides progress: the replay must
    // cancel without rolling back the provider or the working tree.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(ThreadId.make("thread-1")),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });
});

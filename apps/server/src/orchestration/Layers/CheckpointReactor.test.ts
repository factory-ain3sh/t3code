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
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vite-plus/test";

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
import {
  makeProviderRuntimeEvent,
  type ProviderRuntimeEventFixture,
} from "../../provider/testUtils/providerRuntimeEvent.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const THREAD_ID = ThreadId.make("thread-1");
const CREATED_AT = "2026-01-01T00:00:00.000Z";
const checkpointRef = (turnCount: number) => checkpointRefForThreadTurn(THREAD_ID, turnCount);

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
            threadId: THREAD_ID,
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
        threadId: THREAD_ID,
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
    getCapabilities: () =>
      Effect.succeed({
        sessionModelSwitch: "in-session",
        conversationRollback:
          providerName === ProviderDriverKind.make("codex") ||
          providerName === ProviderDriverKind.make("droid")
            ? "supported"
            : "unsupported",
      }),
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

  const emit = (event: ProviderRuntimeEventFixture): void => {
    Effect.runSync(
      PubSub.publish(
        runtimeEventPubSub,
        makeProviderRuntimeEvent(event, ProviderSessionLease.make("lease-a")),
      ),
    );
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
    const thread = snapshot.threads.find((entry) => entry.id === THREAD_ID);
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

function createGitRepository(template: string) {
  const cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-handler-"));
  runGit(process.cwd(), ["clone", "--quiet", "--no-hardlinks", template, cwd]);
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
  let gitTemplate: string;
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | CheckpointReactor
    | CheckpointStore.CheckpointStore
    | ProjectionSnapshotQuery,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  const tempDirs: string[] = [];

  beforeAll(() => {
    gitTemplate = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-template-"));
    runGit(gitTemplate, ["init", "--initial-branch=main"]);
    runGit(gitTemplate, ["config", "user.email", "test@example.com"]);
    runGit(gitTemplate, ["config", "user.name", "Test User"]);
    NodeFS.writeFileSync(NodePath.join(gitTemplate, "README.md"), "v1\n", "utf8");
    runGit(gitTemplate, ["add", "."]);
    runGit(gitTemplate, ["commit", "-m", "Initial"]);
  });

  afterAll(() => {
    NodeFS.rmSync(gitTemplate, { recursive: true, force: true });
  });

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
    const cwd = createGitRepository(gitTemplate);
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
          threadId: THREAD_ID,
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
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: ProviderSessionLease.make("lease-recovered"),
          runtimePayload: { cwd },
        })
        .pipe(Effect.orDie);
    }
    let startStartupRecovery = (): Promise<void> => Promise.resolve();
    const drain = async () => {
      if (options?.skipStartupRecovery !== true) {
        await startStartupRecovery();
      }
      await Effect.runPromise(reactor.drain);
    };

    const sessionLease = ProviderSessionLease.make("lease-a");
    if (options?.persistedSessionBinding ?? options?.hasSession ?? true) {
      await managedRuntime.runPromise(
        providerSessionDirectory.upsert({
          threadId: THREAD_ID,
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
        createdAt: CREATED_AT,
      }),
    );
    await Effect.runPromise(
      engine
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create"),
          threadId: THREAD_ID,
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
          createdAt: CREATED_AT,
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
                  createdAt: CREATED_AT,
                }),
              )
            : Effect.asVoid,
        ),
    );

    const seedFilesystemCheckpoints = async () => {
      await managedRuntime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 0),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
      await managedRuntime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 1),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
      await managedRuntime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
        }),
      );
      if (options?.skipStartupRecovery !== true) {
        const recovery = startStartupRecovery();
        if (!options?.holdLifecycleLockOnStart && options?.onRollbackConversation === undefined) {
          await recovery;
        }
      }
    };

    if (options?.pendingRevertRecovery) {
      await runtime.runPromise(
        providerSessionDirectory.upsert({
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: options?.pendingRevertBindingLease === null ? null : sessionLease,
          runtimePayload: {
            checkpointRevertIntent: {
              commandId: CommandId.make("server:checkpoint-revert-complete:recovery"),
              threadId: THREAD_ID,
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              cwd,
              turnCount: 1,
              retainedTurnIds: [asTurnId("turn-1")],
              staleCheckpoints: (options?.pendingRevertStaleTurnIds ?? [asTurnId("turn-2")]).map(
                (turnId, index) => ({
                  turnId: asTurnId(turnId),
                  checkpointRef:
                    options?.pendingRevertStaleCheckpointRefs?.[index] ??
                    checkpointRefForThreadTurn(THREAD_ID, index + 2),
                }),
              ),
              createdAt: CREATED_AT,
            },
          },
        }),
      );
    }

    if (options?.replacementBeforeRecovery) {
      await runtime.runPromise(
        providerSessionDirectory.upsert({
          threadId: THREAD_ID,
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
          THREAD_ID,
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
    let startupRecovery: Promise<void> | undefined;
    startStartupRecovery = () => {
      startupRecovery ??= Effect.runPromise(reactor.recoverPersistedIntents());
      return startupRecovery;
    };

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

    const dispatch = (command: Parameters<typeof engine.dispatch>[0]) =>
      managedRuntime.runPromise(engine.dispatch(command));
    const setSession = (input?: {
      readonly status?: "ready" | "running";
      readonly activeTurnId?: TurnId | null;
      readonly providerName?: string;
    }) =>
      dispatch({
        type: "thread.session.set",
        commandId: CommandId.make(
          `cmd-session-${input?.providerName ?? options?.providerName ?? "codex"}-${input?.activeTurnId ?? "idle"}`,
        ),
        threadId: THREAD_ID,
        session: {
          threadId: THREAD_ID,
          status: input?.status ?? "ready",
          providerName: input?.providerName ?? options?.providerName ?? "codex",
          runtimeMode: "approval-required",
          activeTurnId: input?.activeTurnId ?? null,
          lastError: null,
          updatedAt: CREATED_AT,
        },
        createdAt: CREATED_AT,
      });
    const completeDiff = (turnId: string, checkpointTurnCount: number) =>
      dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make(`cmd-diff-${turnId}`),
        threadId: THREAD_ID,
        turnId: asTurnId(turnId),
        completedAt: CREATED_AT,
        checkpointRef: checkpointRef(checkpointTurnCount),
        status: "ready",
        files: [],
        checkpointTurnCount,
        createdAt: CREATED_AT,
      });
    const prepareRevert = async (withSession = true) => {
      await seedFilesystemCheckpoints();
      if (withSession) {
        await setSession();
      }
      await completeDiff("turn-1", 1);
      await completeDiff("turn-2", 2);
    };
    const requestRevert = (turnCount: number, suffix = String(turnCount)) =>
      dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make(`cmd-revert-${suffix}`),
        threadId: THREAD_ID,
        turnCount,
        createdAt: CREATED_AT,
      });
    const emitTurn = (
      type: "turn.started" | "turn.completed",
      turnId: string,
      eventOptions?: {
        readonly provider?: ProviderDriverKind;
        readonly sessionLease?: ProviderSessionLease;
      },
    ) =>
      provider.emit({
        type,
        eventId: EventId.make(`evt-${type}-${turnId}`),
        provider:
          eventOptions?.provider ?? options?.providerName ?? ProviderDriverKind.make("codex"),
        ...(eventOptions?.sessionLease ? { sessionLease: eventOptions.sessionLease } : {}),
        createdAt: CREATED_AT,
        threadId: THREAD_ID,
        turnId: asTurnId(turnId),
        ...(type === "turn.completed" ? { payload: { state: "completed" as const } } : {}),
      });
    const getBinding = () =>
      managedRuntime.runPromise(providerSessionDirectory.getBinding(THREAD_ID));
    const getThread = async () => {
      const snapshot = await Effect.runPromise(snapshotQuery.getSnapshot());
      return snapshot.threads.find((entry) => entry.id === THREAD_ID);
    };
    const eventsOfType = async (type: string) =>
      managedRuntime.runPromise(
        Stream.runCollect(engine.readEvents(0)).pipe(
          Effect.map((chunk) => Array.from(chunk).filter((event) => event.type === type)),
        ),
      );

    return {
      engine,
      dispatch,
      setSession,
      completeDiff,
      prepareRevert,
      requestRevert,
      emitTurn,
      getBinding,
      getThread,
      eventsOfType,
      readModel: () => Effect.runPromise(snapshotQuery.getSnapshot()),
      provider,
      providerSessionDirectory,
      runtime: managedRuntime,
      sessionLease,
      cwd,
      drain,
      seedFilesystemCheckpoints,
      releaseLifecycleLock,
      restartReactor,
      recoverPersistedIntents: () => Effect.runPromise(reactor.recoverPersistedIntents()),
      awaitStartupRecovery: startStartupRecovery,
    };
  }

  it("captures pre-turn baseline on turn.started and post-turn checkpoint on turn.completed", async () => {
    const harness = await createHarness();
    await harness.setSession();
    harness.emitTurn("turn.started", "turn-1");
    await waitForGitRefExists(harness.cwd, checkpointRef(0));

    expect(
      await harness.runtime.runPromise(
        harness.providerSessionDirectory.matchesOwnership({
          threadId: THREAD_ID,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: harness.sessionLease,
        }),
      ),
    ).toBe(true);
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.emitTurn("turn.completed", "turn-1");

    await harness.drain();
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-1" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(gitRefExists(harness.cwd, checkpointRef(0))).toBe(true);
    expect(gitRefExists(harness.cwd, checkpointRef(1))).toBe(true);
    expect(gitShowFileAtRef(harness.cwd, checkpointRef(0), "README.md")).toBe("v1\n");
    expect(gitShowFileAtRef(harness.cwd, checkpointRef(1), "README.md")).toBe("v2\n");
  });

  it("ignores lifecycle events emitted by a stale provider session incarnation", async () => {
    const harness = await createHarness();
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "stale\n", "utf8");
    harness.emitTurn("turn.completed", "turn-stale", {
      sessionLease: ProviderSessionLease.make("lease-stale"),
    });
    await harness.drain();
    expect(gitRefExists(harness.cwd, checkpointRef(1))).toBe(false);
  });

  it("refreshes local git status state on turn completion using the session cwd", async () => {
    const gitStatusRefreshCalls: string[] = [];
    const harness = await createHarness({ gitStatusRefreshCalls });
    harness.emitTurn("turn.completed", "turn-refresh-local-status");
    await harness.drain();
    expect(gitStatusRefreshCalls).toEqual([harness.cwd]);
  });

  it.each([
    {
      name: "adopts drift on a dedicated worktree",
      localStatusRefName: "t3code/renamed-by-agent",
      secondThreadSharingWorktree: false,
      expected: "t3code/renamed-by-agent",
    },
    {
      name: "rejects drift on a shared worktree",
      localStatusRefName: "t3code/renamed-by-agent",
      secondThreadSharingWorktree: true,
      expected: "t3code/original-branch",
    },
    {
      name: "rejects a temporary placeholder checkout",
      localStatusRefName: "t3code/0a1b2c3d",
      secondThreadSharingWorktree: false,
      expected: "t3code/original-branch",
    },
  ])("$name", async ({ localStatusRefName, secondThreadSharingWorktree, expected }) => {
    const harness = await createHarness({
      threadBranch: "t3code/original-branch",
      localStatusRefName,
      secondThreadSharingWorktree,
    });
    harness.emitTurn("turn.completed", "turn-branch-drift");
    await harness.drain();
    if (expected === localStatusRefName) {
      await waitForEvent(
        harness.engine,
        (event) =>
          event.type === "thread.meta-updated" &&
          (event as unknown as { payload: { branch?: string } }).payload.branch === expected,
      );
    }
    expect((await harness.getThread())?.branch).toBe(expected);
  });

  it("ignores auxiliary thread turn completion while primary turn is active", async () => {
    const harness = await createHarness();
    await harness.setSession({ status: "running", activeTurnId: asTurnId("turn-main") });
    harness.emitTurn("turn.started", "turn-main");
    await waitForGitRefExists(harness.cwd, checkpointRef(0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.emitTurn("turn.completed", "turn-aux");
    await harness.drain();
    expect((await harness.getThread())?.checkpoints).toHaveLength(0);

    harness.emitTurn("turn.completed", "turn-main");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-main" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
  });

  it("captures pre-turn and completion checkpoints for claude runtime events", async () => {
    const harness = await createHarness({ providerName: ProviderDriverKind.make("claudeAgent") });
    await harness.setSession();
    harness.emitTurn("turn.started", "turn-claude-1");
    await waitForGitRefExists(harness.cwd, checkpointRef(0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.emitTurn("turn.completed", "turn-claude-1");
    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-claude-1" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(gitRefExists(harness.cwd, checkpointRef(1))).toBe(true);
  });

  it("appends capture failure activity when turn diff summary cannot be derived", async () => {
    const harness = await createHarness();
    await harness.setSession();
    harness.emitTurn("turn.completed", "turn-missing-baseline");
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
    const harness = await createHarness({ providerSessionCwd: "", threadWorktreePath: null });
    await harness.dispatch({
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-turn-start-for-baseline"),
      threadId: THREAD_ID,
      message: {
        messageId: MessageId.make("message-user-1"),
        role: "user",
        text: "start turn",
        attachments: [],
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      createdAt: CREATED_AT,
    });
    await waitForGitRefExists(harness.cwd, checkpointRef(0));
    expect(gitShowFileAtRef(harness.cwd, checkpointRef(0), "README.md")).toBe("v1\n");
  });

  it("captures turn completion checkpoint from project workspace root when provider session cwd is unavailable", async () => {
    const harness = await createHarness({ providerSessionCwd: "", threadWorktreePath: null });
    await harness.setSession({ status: "running", activeTurnId: asTurnId("turn-missing-cwd") });
    expect(
      await harness.runtime.runPromise(
        harness.providerSessionDirectory.matchesOwnership({
          threadId: THREAD_ID,
          providerInstanceId: ProviderInstanceId.make("codex"),
          sessionLease: harness.sessionLease,
        }),
      ),
    ).toBe(true);
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.emitTurn("turn.completed", "turn-missing-cwd");
    await harness.drain();
    expect(gitRefExists(harness.cwd, checkpointRef(1))).toBe(true);
    expect(gitShowFileAtRef(harness.cwd, checkpointRef(1), "README.md")).toBe("v2\n");
  });

  it("continues processing runtime events after a single checkpoint runtime failure", async () => {
    const nonRepositorySessionCwd = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-runtime-non-repo-"),
    );
    tempDirs.push(nonRepositorySessionCwd);
    const harness = await createHarness({ providerSessionCwd: nonRepositorySessionCwd });
    await harness.setSession();
    harness.emitTurn("turn.completed", "turn-runtime-failure");
    harness.emitTurn("turn.started", "turn-after-runtime-failure");
    await waitForGitRefExists(harness.cwd, checkpointRef(0));
    expect(gitRefExists(harness.cwd, checkpointRef(0))).toBe(true);
  });

  it("executes provider revert and emits thread.reverted for checkpoint revert requests", async () => {
    const harness = await createHarness();
    await harness.prepareRevert();
    await harness.requestRevert(1);

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.checkpoints.length === 1,
    );
    expect(thread.latestTurn?.turnId).toBe("turn-1");
    expect(thread.checkpoints).toEqual([expect.objectContaining({ checkpointTurnCount: 1 })]);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: THREAD_ID,
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(gitRefExists(harness.cwd, checkpointRef(2))).toBe(false);
  });

  it("rejects unsupported provider rollback before intent persistence or destruction", async () => {
    const harness = await createHarness({ providerName: ProviderDriverKind.make("claudeAgent") });
    await harness.seedFilesystemCheckpoints();
    await harness.setSession();
    await harness.completeDiff("turn-claude-1", 1);
    await harness.completeDiff("turn-claude-2", 2);
    await harness.requestRevert(1);
    await harness.drain();

    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(gitRefExists(harness.cwd, checkpointRef(2))).toBe(true);
    const binding = await harness.getBinding();
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).not.toMatchObject({
        checkpointRevertIntent: expect.anything(),
      });
    }
    expect(
      (await harness.getThread())?.activities.some(
        (activity) => activity.kind === "checkpoint.revert.failed",
      ),
    ).toBe(true);
  });

  it("parks persisted intents until startup recovery is invoked", async () => {
    // Recovery is a dedicated startup phase sequenced after provider-session
    // reconciliation. Replaying from start() would race the orphan sweep,
    // which can stomp a freshly recovered lease with its stale liveness
    // snapshot.
    const rollbackEntered = makeGate();
    const allowRollback = makeGate();
    const harness = await createHarness({
      pendingRevertRecovery: true,
      skipStartupRecovery: true,
      onRollbackConversation: () =>
        Effect.sync(rollbackEntered.open).pipe(
          Effect.andThen(Effect.promise(() => allowRollback.promise)),
        ),
    });
    await harness.seedFilesystemCheckpoints();

    await harness.drain();
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    const parked = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(parked)).toBe(true);
    if (Option.isSome(parked)) {
      expect(parked.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { turnCount: 1 },
      });
    }

    let recoverySettled = false;
    const recovery = harness.recoverPersistedIntents().then(() => {
      recoverySettled = true;
    });
    await rollbackEntered.promise;
    expect(recoverySettled).toBe(false);
    allowRollback.open();
    await recovery;
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
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(false);
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    const harness = await createHarness({
      pendingRevertRecovery: true,
    });
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: THREAD_ID,
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(false);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    const harness = await createHarness({ hasSession: false, persistedSessionBinding: true });
    await harness.prepareRevert(false);
    await harness.requestRevert(1);
    await harness.drain();

    expect(harness.provider.recoverSession).toHaveBeenCalledWith(THREAD_ID);
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
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: THREAD_ID,
      turnIds: [asTurnId("turn-1")],
      anchorTurnId: asTurnId("turn-2"),
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(false);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    await harness.seedFilesystemCheckpoints();

    await rollbackEntered.promise;
    verifyRollbackOwnership = harness.providerSessionDirectory.getBinding(THREAD_ID).pipe(
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
        THREAD_ID,
        harness.providerSessionDirectory.upsert({
          threadId: THREAD_ID,
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
    await harness.awaitStartupRecovery();
    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    await harness.prepareRevert();
    await harness.requestRevert(1, "sequenced-1");
    await harness.requestRevert(0, "sequenced-0");
    await harness.drain();

    expect(harness.provider.rollbackConversation.mock.calls.map(([input]) => input)).toEqual([
      { threadId: THREAD_ID, turnIds: [asTurnId("turn-1")], anchorTurnId: asTurnId("turn-2") },
      { threadId: THREAD_ID, turnIds: [], anchorTurnId: asTurnId("turn-1") },
    ]);
  });

  it("completes a turn-zero revert on a thread with no checkpoints without a provider rollback", async () => {
    const harness = await createHarness();
    await harness.setSession();
    await harness.requestRevert(0);
    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    await harness.drain();

    // An empty provider target must remain a pure tree restore because ProviderService rejects it.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(
      (await harness.getThread())?.activities.some(
        (activity) => activity.kind === "checkpoint.revert.failed",
      ),
    ).toBe(false);
  });

  it("appends an error activity when revert is requested without an active session", async () => {
    const harness = await createHarness({ hasSession: false });
    await harness.requestRevert(1);
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
    await harness.requestRevert(0);
    await harness.drain();
    expect(
      (await harness.getThread())?.activities.some(
        (activity) => activity.kind === "checkpoint.revert.failed",
      ),
    ).toBe(true);
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
  });

  it("retains a recovered revert intent when its rewind cursor fails to persist", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failResumeCursorUpdate: true,
    });
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    // The destructive phase completed but the rewound cursor never persisted.
    // The intent is the recovery state that reconciles the rewound provider
    // with the stored cursor, so it survives for the startup replay (which is
    // safe: the rollback replay is a no-op, the restore is idempotent, and
    // the execute-time progress fence blocks replay over newer work).
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.resumeCursor).toEqual({ sessionId: "session-a" });
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: { threadId: "thread-1" },
      });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("replays a retained intent to convergence after a crash between restore and bookkeeping", async () => {
    const harness = await createHarness({
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failFirstRevertCompleteDispatch: true,
    });
    await harness.seedFilesystemCheckpoints();

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-crash-replay"),
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: CREATED_AT,
      },
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-crash-replay-diff-1"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-1"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-crash-replay-diff-2"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-2"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-crash-replay-revert"),
      threadId: THREAD_ID,
      turnCount: 1,
      createdAt: CREATED_AT,
    });
    await harness.drain();

    // First pass: the destructive phase finished (provider rolled back, tree
    // restored, stale ref deleted) and the injected crash landed before the
    // read model learned about the revert. The intent is the durable recovery
    // state a real SIGKILL would leave behind.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const retained = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(converged)).toBe(true);
    if (Option.isSome(converged)) {
      expect(converged.value.resumeCursor).toEqual({ sessionId: "session-rewound" });
      expect(converged.value.runtimePayload).toMatchObject({ checkpointRevertIntent: null });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
    expect(thread?.checkpoints.every((checkpoint) => checkpoint.checkpointTurnCount <= 1)).toBe(
      true,
    );
  });

  it("replays to convergence after a crash when recovery re-leased the session mid-rollback", async () => {
    const rollbackEntered = makeGate();
    const allowRollback = makeGate();
    const harness = await createHarness({
      rollbackResumeCursor: { sessionId: "session-rewound" },
      failFirstRevertCompleteDispatch: true,
      reLeaseDuringRollback: true,
      onRollbackConversation: () =>
        Effect.sync(rollbackEntered.open).pipe(
          Effect.andThen(Effect.promise(() => allowRollback.promise)),
        ),
    });
    await harness.seedFilesystemCheckpoints();

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-release-crash"),
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "ready",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: null,
        lastError: null,
        updatedAt: CREATED_AT,
      },
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-release-crash-diff-1"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-1"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-release-crash-diff-2"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-2"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-release-crash-revert"),
      threadId: THREAD_ID,
      turnCount: 1,
      createdAt: CREATED_AT,
    });
    await rollbackEntered.promise;
    const beforeReLease = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(beforeReLease)).toBe(true);
    const originalPayload = Option.isSome(beforeReLease)
      ? beforeReLease.value.runtimePayload
      : undefined;
    const originalIntent =
      originalPayload !== null &&
      typeof originalPayload === "object" &&
      "checkpointRevertIntent" in originalPayload
        ? originalPayload.checkpointRevertIntent
        : undefined;
    expect(originalIntent).toMatchObject({
      retainedTurnIds: [asTurnId("turn-1")],
      staleCheckpoints: [
        {
          turnId: asTurnId("turn-2"),
          checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
        },
      ],
    });
    allowRollback.open();
    await harness.drain();

    // First pass: locked recovery re-leased the session mid-rollback, the
    // destructive phase finished, and the injected crash landed before
    // bookkeeping. The directory carried the canonical intent unchanged
    // across the incarnation wipe, so the restart replay still has the
    // target it must reconcile.
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    const retained = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(retained)).toBe(true);
    if (Option.isSome(retained)) {
      expect(retained.value.sessionLease).toBe(ProviderSessionLease.make("lease-recovered"));
      const retainedPayload = retained.value.runtimePayload;
      expect(
        retainedPayload !== null &&
          typeof retainedPayload === "object" &&
          "checkpointRevertIntent" in retainedPayload
          ? retainedPayload.checkpointRevertIntent
          : undefined,
      ).toEqual(originalIntent);
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
      harness.providerSessionDirectory.getBinding(THREAD_ID),
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
    });

    await harness.drain();

    // The ref pre-check proves the checkpoint exists before anything
    // destructive runs: a deterministically missing ref clears the intent
    // terminally with the provider conversation untouched.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({ checkpointRevertIntent: null });
    }
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("clears a recovered revert intent terminally when the provider rollback fails", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      onRollbackConversation: () => Effect.die(new Error("rollback exploded")),
    });
    await harness.seedFilesystemCheckpoints();

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    // The rollback runs before the filesystem restore, so a failing rollback
    // leaves the working tree and the stale checkpoint refs untouched.
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });

  it("rejects a revert request while a turn is active", async () => {
    const harness = await createHarness();
    await harness.seedFilesystemCheckpoints();

    await harness.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set-active-turn"),
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "running",
        providerName: "codex",
        runtimeMode: "approval-required",
        activeTurnId: asTurnId("turn-live"),
        lastError: null,
        updatedAt: CREATED_AT,
      },
      createdAt: CREATED_AT,
    });
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-active-turn-diff-1"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-1"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 1),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt: CREATED_AT,
    });

    await harness.dispatch({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert-mid-turn"),
      threadId: THREAD_ID,
      turnCount: 1,
      createdAt: CREATED_AT,
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

  it("cancels before rollback or restore when the durable binding gains an active turn", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      holdLifecycleLockOnStart: true,
    });
    await harness.seedFilesystemCheckpoints();

    await harness.runtime.runPromise(
      harness.providerSessionDirectory.updateRuntimePayloadIfOwned({
        threadId: THREAD_ID,
        providerInstanceId: ProviderInstanceId.make("codex"),
        sessionLease: harness.sessionLease,
        runtimePayload: { activeTurnId: asTurnId("turn-became-active") },
      }),
    );
    harness.releaseLifecycleLock?.();
    await harness.awaitStartupRecovery();

    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(true);
    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        activeTurnId: asTurnId("turn-became-active"),
        checkpointRevertIntent: null,
      });
    }
  });

  it("cancels a recovered revert intent after the thread progressed past it", async () => {
    const harness = await createHarness({
      pendingRevertRecovery: true,
      pendingRevertStaleCheckpointRefs: [],
      pendingRevertStaleTurnIds: [],
      holdLifecycleLockOnStart: true,
    });
    await harness.seedFilesystemCheckpoints();

    // The recovery replay is parked on the lifecycle lock; land a checkpoint
    // the intent never accounted for before letting it run.
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-progressed-diff-2"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-2"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt: CREATED_AT,
    });
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length >= 1);
    harness.releaseLifecycleLock?.();
    await harness.awaitStartupRecovery();
    await harness.drain();

    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
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
    await harness.seedFilesystemCheckpoints();

    // The recovery replay is parked on the lifecycle lock; land a NEW turn's
    // checkpoint on the ref the intent recorded as stale.
    await harness.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-recycled-ref-diff"),
      threadId: THREAD_ID,
      turnId: asTurnId("turn-2-replacement"),
      completedAt: CREATED_AT,
      checkpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
      status: "ready",
      files: [],
      checkpointTurnCount: 2,
      createdAt: CREATED_AT,
    });
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length >= 1);
    harness.releaseLifecycleLock?.();
    await harness.awaitStartupRecovery();
    await harness.drain();

    // Turn identity, not the recycled ref, decides progress: the replay must
    // cancel without rolling back the provider or the working tree.
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v3\n");
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(THREAD_ID, 2))).toBe(true);

    const binding = await harness.runtime.runPromise(
      harness.providerSessionDirectory.getBinding(THREAD_ID),
    );
    expect(Option.isSome(binding)).toBe(true);
    if (Option.isSome(binding)) {
      expect(binding.value.runtimePayload).toMatchObject({
        checkpointRevertIntent: null,
      });
    }

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === THREAD_ID);
    expect(
      thread?.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    ).toBe(true);
  });
});

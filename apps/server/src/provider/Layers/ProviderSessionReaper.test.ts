import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  ProviderInstanceId,
  ProviderSessionLease,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProviderValidationError } from "../Errors.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";
import { makeProviderServiceMock } from "../testUtils/providerServiceMock.ts";

const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<void> => {
    if (await predicate()) {
      return;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for expectation.");
    }
    await Effect.runPromise(Effect.yieldNow);
    return poll();
  };

  return poll();
}

const drainFibers = Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
  discard: true,
});

function makeReadModel(
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly session: {
      readonly threadId: ThreadId;
      readonly status: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly providerName: "codex" | "claudeAgent";
      readonly runtimeMode: "approval-required" | "full-access" | "auto-accept-edits";
      readonly activeTurnId: TurnId | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
    readonly backgroundLiveness?: "working" | "monitoring" | null;
  }>,
) {
  const now = "2026-01-01T00:00:00.000Z";
  const projectId = ProjectId.make("project-provider-session-reaper");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Project",
        workspaceRoot: "/tmp/provider-reaper-project",
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: threads.map((thread) => ({
      id: thread.id,
      projectId,
      title: `Thread ${thread.id}`,
      modelSelection: defaultModelSelection,
      interactionMode: "default" as const,
      runtimeMode: "full-access" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      latestTurn: null,
      messages: [],
      session: thread.session,
      backgroundLiveness: thread.backgroundLiveness ?? null,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
    })),
  };
}

describe("ProviderSessionReaper", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    ProviderSessionReaper | ProviderSessionRuntime.ProviderSessionRuntimeRepository,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;

  const disposeHarness = async () => {
    if (scope) await Effect.runPromise(Scope.close(scope, Exit.void));
    scope = null;
    if (runtime) await runtime.dispose();
    runtime = null;
  };

  afterEach(disposeHarness);

  async function startReaper() {
    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(reaper.start().pipe(Scope.provide(scope)));
  }

  async function createHarness(input: {
    readonly readModel: ReturnType<typeof makeReadModel>;
    readonly stopSessionImplementation?: (input: {
      readonly threadId: ThreadId;
    }) => ReturnType<ProviderServiceShape["stopSession"]>;
  }) {
    const logMessages: unknown[] = [];
    const stoppedThreadIds = new Set<ThreadId>();
    const stopSession = vi.fn<ProviderServiceShape["stopSession"]>((request) =>
      input.stopSessionImplementation
        ? input.stopSessionImplementation(request)
        : Effect.sync(() => {
            stoppedThreadIds.add(request.threadId);
            return "stopped" as const;
          }),
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const snapshotQuery = Layer.succeed(ProjectionSnapshotQuery, {
      getCommandReadModel: () => Effect.die("unused"),
      getSnapshot: () => Effect.die("unused"),
      getShellSnapshot: () => Effect.die("unused"),
      getArchivedShellSnapshot: () => Effect.die("unused"),
      getSnapshotSequence: () =>
        Effect.succeed({ snapshotSequence: input.readModel.snapshotSequence }),
      getCounts: () => Effect.die("unused"),
      getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
      getProjectShellById: () => Effect.die("unused"),
      getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
      getThreadCheckpointContext: () => Effect.die("unused"),
      getFullThreadDiffContext: () => Effect.die("unused"),
      getThreadShellById: (threadId: ThreadId) => {
        const thread = input.readModel.threads.find((candidate) => candidate.id === threadId);
        return Effect.succeed(thread ? Option.some(thread) : Option.none());
      },
      getThreadDetailById: () => Effect.die("unused"),
      getThreadDetailSnapshot: () => Effect.die("unused"),
      searchThreads: () => Effect.succeed({ matches: [] }),
    });
    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: 1_000,
      sweepIntervalMs: 60_000,
    }).pipe(
      Layer.provideMerge(directoryLayer),
      Layer.provideMerge(runtimeRepositoryLayer),
      Layer.provideMerge(
        Layer.succeed(
          ProviderService,
          makeProviderServiceMock(ProviderDriverKind.make("codex"), { stopSession }),
        ),
      ),
      Layer.provideMerge(snapshotQuery),
      Layer.provideMerge(NodeServices.layer),
      Layer.provideMerge(
        Layer.succeed(
          Logger.CurrentLoggers,
          new Set([Logger.make((options) => logMessages.push(options.message))]),
        ),
      ),
    );

    runtime = ManagedRuntime.make(layer);
    const repository = await runtime.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );
    const seed = (...rows: ReadonlyArray<ProviderSessionRuntime.ProviderSessionRuntime>) =>
      runtime!.runPromise(Effect.forEach(rows, repository.upsert, { discard: true }));
    return { repository, seed, stopSession, stoppedThreadIds, logMessages };
  }

  const staleAt = "2026-04-14T00:00:00.000Z";
  const snapshotAt = "2026-01-01T00:00:00.000Z";

  function readModelThread(
    threadId: ThreadId,
    input: {
      readonly status?: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly activeTurnId?: TurnId | null;
      readonly providerName?: "codex" | "claudeAgent";
      readonly backgroundLiveness?: "working" | "monitoring" | null;
      readonly updatedAt?: string;
    } = {},
  ) {
    return {
      id: threadId,
      session: {
        threadId,
        status: input.status ?? "ready",
        providerName: input.providerName ?? "codex",
        runtimeMode: "full-access" as const,
        activeTurnId: input.activeTurnId ?? null,
        lastError: null,
        updatedAt: input.updatedAt ?? snapshotAt,
      },
      ...(input.backgroundLiveness !== undefined
        ? { backgroundLiveness: input.backgroundLiveness }
        : {}),
    };
  }

  function runtimeRow(
    threadId: ThreadId,
    input: Partial<ProviderSessionRuntime.ProviderSessionRuntime> = {},
  ): ProviderSessionRuntime.ProviderSessionRuntime {
    return {
      threadId,
      providerName: "codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      sessionLease: null,
      adapterKey: "codex",
      runtimeMode: "full-access",
      status: "running",
      lastSeenAt: staleAt,
      resumeCursor: null,
      runtimePayload: null,
      ...input,
    };
  }

  it.each([
    { name: "unleased", lease: null },
    { name: "leased", lease: ProviderSessionLease.make("lease-reaper-cas") },
  ])("reaps stale $name sessions with the persisted lease", async ({ name, lease }) => {
    const threadId = ThreadId.make(`thread-reaper-stale-${name}`);
    const harness = await createHarness({
      readModel: makeReadModel([readModelThread(threadId)]),
    });
    await harness.seed(runtimeRow(threadId, { sessionLease: lease }));

    await startReaper();
    await waitFor(() => harness.stopSession.mock.calls.length === 1);

    expect(harness.stopSession.mock.calls[0]?.[0]).toEqual({
      threadId,
      expectedSessionLease: lease,
    });
    expect(harness.stoppedThreadIds.has(threadId)).toBe(true);
  });

  it("does not count or log an ownership-mismatch as a reap", async () => {
    const threadId = ThreadId.make("thread-reaper-ownership-mismatch");
    const sessionLease = ProviderSessionLease.make("lease-reaper-stale-snapshot");
    const harness = await createHarness({
      readModel: makeReadModel([readModelThread(threadId)]),
      stopSessionImplementation: () => Effect.succeed("ownership-mismatch"),
    });
    await harness.seed(runtimeRow(threadId, { sessionLease }));

    await startReaper();
    await waitFor(() => harness.stopSession.mock.calls.length === 1);
    await Effect.runPromise(drainFibers);

    expect(harness.stopSession.mock.calls[0]?.[0]).toEqual({
      threadId,
      expectedSessionLease: sessionLease,
    });
    expect(
      harness.logMessages.some((message) =>
        ["provider.session.reaped", "provider.session.reaper.sweep-complete"].some((event) =>
          String(message).includes(event),
        ),
      ),
    ).toBe(false);
  });

  it.each([
    {
      name: "an active turn",
      thread: { status: "running" as const, activeTurnId: TurnId.make("turn-reaper-active") },
      row: {},
      fresh: false,
    },
    {
      name: "live background work",
      thread: { backgroundLiveness: "working" as const },
      row: {},
      fresh: false,
    },
    {
      name: "the inactivity threshold",
      thread: {},
      row: {},
      fresh: true,
    },
    {
      name: "an already-stopped runtime",
      thread: { status: "stopped" as const },
      row: { status: "stopped" as const },
      fresh: false,
    },
  ])("skips sessions with $name", async ({ name, thread, row, fresh }) => {
    const threadId = ThreadId.make(`thread-reaper-skip-${name.replaceAll(" ", "-")}`);
    const lastSeenAt = fresh ? DateTime.formatIso(await Effect.runPromise(DateTime.now)) : staleAt;
    const harness = await createHarness({
      readModel: makeReadModel([readModelThread(threadId, { ...thread, updatedAt: lastSeenAt })]),
    });
    await harness.seed(runtimeRow(threadId, { ...row, lastSeenAt }));

    await startReaper();
    await Effect.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(
      Option.isSome(await runtime!.runPromise(harness.repository.getByThreadId({ threadId }))),
    ).toBe(true);
  });

  it.each([
    {
      name: "failure",
      stop: () =>
        Effect.fail(
          new ProviderValidationError({
            operation: "ProviderSessionReaper.test",
            issue: "simulated stop failure",
          }),
        ),
    },
    { name: "defect", stop: () => Effect.die(new Error("simulated stop defect")) },
  ])("continues reaping after one stop $name", async ({ name, stop }) => {
    const blockedThreadId = ThreadId.make(`thread-reaper-stop-${name}`);
    const reapedThreadId = ThreadId.make(`thread-reaper-after-${name}`);
    const harness = await createHarness({
      readModel: makeReadModel([
        readModelThread(blockedThreadId, { providerName: "claudeAgent" }),
        readModelThread(reapedThreadId),
      ]),
      stopSessionImplementation: (request) =>
        request.threadId === blockedThreadId ? stop() : Effect.succeed("stopped"),
    });
    await harness.seed(
      runtimeRow(blockedThreadId, {
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
      }),
      runtimeRow(reapedThreadId, { lastSeenAt: "2026-04-14T00:01:00.000Z" }),
    );

    await startReaper();
    await waitFor(() => harness.stopSession.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.map(([request]) => request)).toEqual([
      { threadId: blockedThreadId, expectedSessionLease: null },
      { threadId: reapedThreadId, expectedSessionLease: null },
    ]);
  });
});

// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import type {
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderUploadFeedbackInput,
} from "@t3tools/contracts";
import {
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionLease,
  ProviderSessionStartInput,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it, assert, describe, vi } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionInvalidatedError,
  ProviderAdapterSessionNotFoundError,
  ProviderSessionDirectoryPersistenceError,
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type {
  ProviderAdapterSession,
  ProviderAdapterShape,
  ProviderThreadRollbackTarget,
} from "../Services/ProviderAdapter.ts";
import {
  makeRequireActiveProviderSession,
  rollbackTargetMatchesKnownHistory,
  rollbackTargetMatchesTurnPrefix,
} from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { makeAdapterRegistryMock } from "../testUtils/providerAdapterRegistryMock.ts";
import {
  makeProviderRuntimeEvent,
  type ProviderRuntimeEventFixture,
} from "../testUtils/providerRuntimeEvent.ts";

const serverConfigTestLayer = ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(
  Layer.provide(NodeServices.layer),
);

const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const codexInstanceId = ProviderInstanceId.make("codex");
const claudeAgentInstanceId = ProviderInstanceId.make("claudeAgent");
const droidInstanceId = ProviderInstanceId.make("droid");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_DRIVER = ProviderDriverKind.make("claudeAgent");
const CURSOR_DRIVER = ProviderDriverKind.make("cursor");
const DROID_DRIVER = ProviderDriverKind.make("droid");
const noOpEventLoggersLayer = Layer.succeed(
  ProviderEventLoggers.ProviderEventLoggers,
  ProviderEventLoggers.NoOpProviderEventLoggers,
);

describe("provider rollback matchers", () => {
  const turns = [asTurnId("turn-1"), asTurnId("turn-2"), asTurnId("turn-3")].map((id) => ({
    id,
  }));

  it("keeps prefix matching distinct from known-history anchor matching", () => {
    const matrix: ReadonlyArray<{
      readonly target: ProviderThreadRollbackTarget;
      readonly prefix: boolean;
      readonly knownHistory: boolean;
    }> = [
      {
        target: { turnIds: [asTurnId("turn-1")] },
        prefix: true,
        knownHistory: true,
      },
      {
        target: {
          turnIds: [asTurnId("turn-1")],
          anchorTurnId: asTurnId("turn-2"),
        },
        prefix: true,
        knownHistory: true,
      },
      {
        target: {
          turnIds: [asTurnId("turn-1")],
          anchorTurnId: asTurnId("turn-3"),
        },
        prefix: true,
        knownHistory: false,
      },
      {
        target: { turnIds: [asTurnId("turn-2")] },
        prefix: false,
        knownHistory: false,
      },
      {
        target: {
          turnIds: turns.map((turn) => turn.id),
          anchorTurnId: asTurnId("turn-missing"),
        },
        prefix: true,
        knownHistory: true,
      },
    ];

    for (const testCase of matrix) {
      assert.equal(rollbackTargetMatchesTurnPrefix(turns, testCase.target), testCase.prefix);
      assert.equal(
        rollbackTargetMatchesKnownHistory(turns, testCase.target),
        testCase.knownHistory,
      );
    }
  });
});

describe("provider active session resolver", () => {
  it.effect("observes a session added after the effect is constructed", () =>
    Effect.gen(function* () {
      const threadId = asThreadId("thread-deferred-session-lookup");
      const session = { stopped: false, value: "late-session" };
      const sessions = new Map<ThreadId, typeof session>();
      const requireSession = makeRequireActiveProviderSession(sessions, CODEX_DRIVER);
      const lookup = requireSession(threadId);

      sessions.set(threadId, session);

      assert.strictEqual(yield* lookup, session);
    }),
  );
});

function makeFakeCodexAdapter(provider: ProviderDriverKind = CODEX_DRIVER) {
  const sessions = new Map<ThreadId, ProviderAdapterSession>();
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  let nextSessionLease = 0;

  const startSession = vi.fn<ProviderAdapterShape<ProviderAdapterError>["startSession"]>(
    (input: ProviderSessionStartInput) =>
      Effect.sync(() => {
        const now = "2026-01-01T00:00:00.000Z";
        const session: ProviderAdapterSession = {
          provider,
          ...(input.providerInstanceId !== undefined
            ? { providerInstanceId: input.providerInstanceId }
            : {}),
          status: "ready",
          runtimeMode: input.runtimeMode,
          threadId: input.threadId,
          sessionLease: ProviderSessionLease.make(`lease-${++nextSessionLease}`),
          resumeCursor: input.resumeCursor ?? {
            opaque: `resume-${String(input.threadId)}`,
          },
          cwd: input.cwd ?? process.cwd(),
          createdAt: now,
          updatedAt: now,
        };
        sessions.set(session.threadId, session);
        return session;
      }),
  );

  const sendTurn = vi.fn<ProviderAdapterShape<ProviderAdapterError>["sendTurn"]>(
    (input: ProviderSendTurnInput) =>
      sessions.has(input.threadId)
        ? Effect.succeed({
            threadId: input.threadId,
            turnId: TurnId.make(`turn-${String(input.threadId)}`),
          })
        : Effect.fail(
            new ProviderAdapterSessionNotFoundError({
              provider,
              threadId: input.threadId,
            }),
          ),
  );
  const interruptTurn = vi.fn((_threadId: ThreadId, _turnId?: TurnId) => Effect.void);
  const respondToRequest = vi.fn(
    (_threadId: ThreadId, _requestId: string, _decision: ProviderApprovalDecision) => Effect.void,
  );
  const respondToUserInput = vi.fn(
    (_threadId: ThreadId, _requestId: string, _answers: Record<string, unknown>) => Effect.void,
  );
  const stopSession = vi.fn((threadId: ThreadId) =>
    Effect.sync(() => {
      sessions.delete(threadId);
    }),
  );
  const listSessions = vi.fn(() => Effect.sync(() => Array.from(sessions.values())));
  const hasSession = vi.fn((threadId: ThreadId) => Effect.succeed(sessions.has(threadId)));
  const readThread = vi.fn((threadId: ThreadId) =>
    Effect.succeed({
      threadId,
      turns: [{ id: asTurnId("turn-1"), items: [] as const }],
    }),
  );
  const rollbackThread = vi.fn<
    NonNullable<ProviderAdapterShape<ProviderAdapterError>["rollbackThread"]>
  >((threadId: ThreadId, target: ProviderThreadRollbackTarget) =>
    Effect.succeed({
      threadId,
      turns: target.turnIds.map((id) => ({ id, items: [] as const })),
    }),
  );
  const uploadFeedback = vi.fn((input: ProviderUploadFeedbackInput) =>
    Effect.succeed({ feedbackId: `feedback-${input.threadId}` }),
  );
  const stopAll = vi.fn<ProviderAdapterShape<ProviderAdapterError>["stopAll"]>(() =>
    Effect.sync(() => {
      sessions.clear();
    }),
  );

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
      conversationRollback:
        provider === CODEX_DRIVER || provider === DROID_DRIVER ? "supported" : "unsupported",
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    ...(provider === CODEX_DRIVER || provider === DROID_DRIVER ? { rollbackThread } : {}),
    ...(provider === CODEX_DRIVER ? { uploadFeedback } : {}),
    stopAll,
    get streamEvents() {
      return Stream.fromPubSub(runtimeEventPubSub);
    },
  };

  const publish = (
    event: ProviderRuntimeEventFixture,
    sessionLease: ProviderSessionLease,
  ): void => {
    Effect.runSync(
      PubSub.publish(runtimeEventPubSub, makeProviderRuntimeEvent(event, sessionLease)),
    );
  };

  const emit = (event: ProviderRuntimeEventFixture): void => {
    const threadId = ThreadId.make(event.threadId);
    const sessionLease = sessions.get(threadId)?.sessionLease;
    if (sessionLease === undefined) {
      throw new Error(`No active test provider session for thread '${threadId}'.`);
    }
    publish(event, sessionLease);
  };

  const emitRaw = (
    event: ProviderRuntimeEventFixture & { sessionLease: ProviderSessionLease },
  ): void => {
    publish(event, event.sessionLease);
  };

  const updateSession = (
    threadId: ThreadId,
    update: (session: ProviderSession) => ProviderSession,
  ): void => {
    const existing = sessions.get(threadId);
    if (!existing) {
      return;
    }
    sessions.set(threadId, {
      ...update(existing),
      sessionLease: existing.sessionLease,
    });
  };

  return {
    adapter,
    emit,
    emitRaw,
    updateSession,
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    uploadFeedback,
    stopAll,
  };
}

type ProviderServiceLiveOptions = Parameters<typeof makeProviderServiceLive>[0];
type TestServerSettings = Parameters<typeof ServerSettings.ServerSettingsService.layerTest>[0];

function makeProviderLayers({
  registry,
  dbPath,
  directoryOverride,
  serviceOptions,
  settings,
}: {
  readonly registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"];
  readonly dbPath?: string;
  readonly directoryOverride?: (
    directory: ProviderSessionDirectory.ProviderSessionDirectoryShape,
  ) => ProviderSessionDirectory.ProviderSessionDirectoryShape;
  readonly serviceOptions?: ProviderServiceLiveOptions;
  readonly settings?: TestServerSettings;
}) {
  const persistenceLayer =
    dbPath === undefined ? SqlitePersistenceMemory : makeSqlitePersistenceLive(dbPath);
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(Layer.provide(persistenceLayer));
  const baseDirectoryLayer = ProviderSessionDirectoryLive.pipe(
    Layer.provide(runtimeRepositoryLayer),
  );
  const directoryLayer =
    directoryOverride === undefined
      ? baseDirectoryLayer
      : Layer.effect(
          ProviderSessionDirectory.ProviderSessionDirectory,
          Effect.map(ProviderSessionDirectory.ProviderSessionDirectory, directoryOverride),
        ).pipe(Layer.provide(baseDirectoryLayer));
  const serviceLayer = makeProviderServiceLive(serviceOptions).pipe(
    Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
    Layer.provide(directoryLayer),
    Layer.provide(ServerSettings.ServerSettingsService.layerTest(settings)),
    Layer.provide(serverConfigTestLayer),
    Layer.provide(AnalyticsService.layerTest),
    Layer.provide(noOpEventLoggersLayer),
  );

  return {
    persistenceLayer,
    runtimeRepositoryLayer,
    directoryLayer,
    serviceLayer,
    fullLayer: Layer.mergeAll(serviceLayer, directoryLayer, runtimeRepositoryLayer).pipe(
      Layer.provideMerge(NodeServices.layer),
    ),
  };
}

function makeSingleInstanceRegistry({
  adapter,
  driverKind,
  enabled,
  instanceId,
}: {
  readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
  readonly driverKind: ProviderDriverKind;
  readonly enabled: boolean;
  readonly instanceId: ProviderInstanceId;
}): ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] {
  const unsupported = () => new ProviderUnsupportedError({ provider: driverKind });
  return {
    getByInstance: (requested) =>
      requested === instanceId ? Effect.succeed(adapter) : Effect.fail(unsupported()),
    getInstanceInfo: (requested) =>
      requested === instanceId
        ? Effect.succeed({
            instanceId,
            driverKind,
            displayName: "Codex Personal",
            enabled,
            continuationIdentity: {
              driverKind,
              continuationKey: "codex:/Users/example/.codex",
            },
          })
        : Effect.fail(unsupported()),
    listInstances: () => Effect.succeed([instanceId]),
    listProviders: () => Effect.succeed([driverKind]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
  };
}

const expectSome = <A>(option: Option.Option<A>): A => {
  assert.equal(Option.isSome(option), true);
  if (Option.isNone(option)) {
    throw new Error("Expected Some");
  }
  return option.value;
};

const getPersistedSessionLease = (
  directory: ProviderSessionDirectory.ProviderSessionDirectory["Service"],
  threadId: ThreadId,
) =>
  directory.getBinding(threadId).pipe(
    Effect.map((binding) => {
      const sessionLease = expectSome(binding).sessionLease;
      if (sessionLease === null || sessionLease === undefined) {
        throw new Error(`Expected an active session lease for thread '${threadId}'.`);
      }
      return sessionLease;
    }),
  );

const assertStartCalledWith = (
  calls: ReadonlyArray<ReadonlyArray<unknown>>,
  expected: Partial<ProviderSessionStartInput>,
) => {
  assert.equal(calls.length, 1);
  assert.deepInclude(calls[0]?.[0], expected);
};

type TestSessionOptions = Omit<
  Partial<ProviderSessionStartInput>,
  "provider" | "providerInstanceId" | "threadId"
> & {
  readonly driver?: ProviderDriverKind;
  readonly instanceId?: ProviderInstanceId;
};

const startTestSession = (
  service: ProviderService.ProviderService["Service"],
  thread: string,
  options: TestSessionOptions = {},
) => {
  const { driver = CODEX_DRIVER, instanceId = ProviderInstanceId.make(driver), ...input } = options;
  const threadId = asThreadId(thread);
  return service.startSession(threadId, {
    ...input,
    provider: driver,
    providerInstanceId: instanceId,
    threadId,
    runtimeMode: input.runtimeMode ?? "full-access",
  });
};

const advanceTestClock = (ms: number) =>
  TestClock.adjust(`${ms} millis`).pipe(Effect.andThen(Effect.yieldNow));

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

function makeProviderServiceLayer() {
  const codex = makeFakeCodexAdapter();
  const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const droid = makeFakeCodexAdapter(DROID_DRIVER);
  const registry = makeAdapterRegistryMock({
    [ProviderDriverKind.make("codex")]: codex.adapter,
    [ProviderDriverKind.make("claudeAgent")]: claude.adapter,
    [ProviderDriverKind.make("cursor")]: cursor.adapter,
    [ProviderDriverKind.make("droid")]: droid.adapter,
  });

  const { fullLayer } = makeProviderLayers({ registry });
  const layer = it.layer(fullLayer);

  return {
    codex,
    claude,
    cursor,
    droid,
    layer,
  };
}

it.effect("ProviderServiceLive catches stopAll failures during shutdown", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    codex.stopAll.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: String(CODEX_DRIVER),
          method: "stopAll",
          detail: "simulated stopAll failure",
        }),
      ),
    );
    const registry = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
    });
    const { fullLayer } = makeProviderLayers({ registry });
    const scope = yield* Scope.make();
    const runtimeServices = yield* Layer.build(fullLayer).pipe(Scope.provide(scope));

    yield* ProviderService.ProviderService.pipe(Effect.provide(runtimeServices));
    const closeExit = yield* Scope.close(scope, Exit.void).pipe(Effect.exit);

    assert.equal(Exit.isSuccess(closeExit), true);
    assert.equal(codex.stopAll.mock.calls.length, 1);
  }),
);

it.effect("ProviderServiceLive rejects disabled default and custom instances", () =>
  Effect.gen(function* () {
    const cases = [
      {
        driverKind: CLAUDE_AGENT_DRIVER,
        instanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-disabled"),
      },
      {
        driverKind: CODEX_DRIVER,
        instanceId: ProviderInstanceId.make("codex_personal"),
        threadId: asThreadId("thread-disabled-instance"),
      },
    ] as const;

    for (const testCase of cases) {
      const adapter = makeFakeCodexAdapter(testCase.driverKind);
      const registry = makeSingleInstanceRegistry({
        adapter: adapter.adapter,
        driverKind: testCase.driverKind,
        enabled: false,
        instanceId: testCase.instanceId,
      });
      const { serviceLayer } = makeProviderLayers({ registry });
      const failure = yield* Effect.flip(
        Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          return yield* startTestSession(provider, testCase.threadId, {
            driver: testCase.driverKind,
            instanceId: testCase.instanceId,
          });
        }).pipe(Effect.provide(serviceLayer)),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, `Provider instance '${testCase.instanceId}' is disabled`);
      assert.equal(adapter.startSession.mock.calls.length, 0);
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive allows enabled custom instances when legacy driver is disabled",
  () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("codex_personal");
      const driverKind = CODEX_DRIVER;
      const codex = makeFakeCodexAdapter();
      const registry = makeSingleInstanceRegistry({
        adapter: codex.adapter,
        driverKind,
        enabled: true,
        instanceId,
      });
      const { serviceLayer } = makeProviderLayers({
        registry,
        settings: {
          providers: {
            codex: {
              enabled: false,
            },
          },
        },
      });

      const session = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* startTestSession(provider, asThreadId("thread-enabled-custom"), {
          driver: driverKind,
          instanceId: instanceId,
        });
      }).pipe(Effect.provide(serviceLayer));

      assert.equal(session.providerInstanceId, instanceId);
      assert.equal(codex.startSession.mock.calls.length, 1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

const routing = makeProviderServiceLayer();

it.effect(
  "ProviderServiceLive uploads feedback through the adapter that recovered the session",
  () =>
    Effect.gen(function* () {
      const original = makeFakeCodexAdapter();
      const replacement = makeFakeCodexAdapter();
      const baseRegistry = makeAdapterRegistryMock({ [CODEX_DRIVER]: original.adapter });
      let swapAfterFirstLookup = false;
      let feedbackLookupCount = 0;
      const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
        ...baseRegistry,
        getByInstance: (instanceId) => {
          if (instanceId !== codexInstanceId) {
            return baseRegistry.getByInstance(instanceId);
          }
          const useReplacement = swapAfterFirstLookup && feedbackLookupCount++ > 0;
          return Effect.succeed(useReplacement ? replacement.adapter : original.adapter);
        },
      };
      const { serviceLayer } = makeProviderLayers({ registry });

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const threadId = asThreadId("thread-feedback-adapter-replacement");
        yield* startTestSession(provider, threadId);
        yield* original.stopSession(threadId);
        original.uploadFeedback.mockClear();
        replacement.uploadFeedback.mockClear();
        swapAfterFirstLookup = true;

        const result = yield* provider.uploadFeedback({ threadId });

        assert.deepStrictEqual(result, { feedbackId: `feedback-${threadId}` });
        assert.strictEqual(original.uploadFeedback.mock.calls.length, 0);
        assert.deepStrictEqual(replacement.uploadFeedback.mock.calls, [[{ threadId }]]);
      }).pipe(Effect.provide(serviceLayer));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive writes canonical events to the emitting thread segment", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const canonicalEvents: ProviderRuntimeEvent[] = [];
    const canonicalThreadIds: Array<string | null> = [];
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });
    const { serviceLayer } = makeProviderLayers({
      registry,
      serviceOptions: {
        canonicalEventLogger: {
          filePath: "memory://provider-canonical-events",
          write: (event, threadId) => {
            canonicalEvents.push(event as ProviderRuntimeEvent);
            canonicalThreadIds.push(threadId ?? null);
            return Effect.void;
          },
          close: () => Effect.void,
        },
      },
    });

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-canonical-thread-segment");
      yield* startTestSession(provider, threadId);
      yield* advanceTestClock(10);
      codex.emit({
        eventId: asEventId("evt-canonical-thread-segment"),
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:00.000Z",
        type: "turn.completed",
        payload: {
          state: "completed",
        },
      });
      yield* advanceTestClock(20);
    }).pipe(Effect.provide(serviceLayer));

    assert.equal(canonicalEvents.length, 1);
    assert.equal(canonicalEvents[0]?.threadId, "thread-canonical-thread-segment");
    assert.deepEqual(canonicalThreadIds, ["thread-canonical-thread-segment"]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive keeps persisted resumable sessions on startup", () =>
  Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-service-"));
    const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

    const codex = makeFakeCodexAdapter();
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });

    const { persistenceLayer, runtimeRepositoryLayer, directoryLayer, serviceLayer } =
      makeProviderLayers({ registry, dbPath });

    yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: ThreadId.make("thread-stale"),
      });
    }).pipe(Effect.provide(directoryLayer));

    yield* ProviderService.ProviderService.pipe(Effect.provide(serviceLayer));

    const persistedProvider = yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      return yield* directory.getProvider(asThreadId("thread-stale"));
    }).pipe(Effect.provide(directoryLayer));
    assert.equal(persistedProvider, "codex");

    const runtime = yield* Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      return yield* repository.getByThreadId({
        threadId: asThreadId("thread-stale"),
      });
    }).pipe(Effect.provide(runtimeRepositoryLayer));
    assert.equal(Option.isSome(runtime), true);

    const legacyTableRows = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'provider_sessions'
      `;
    }).pipe(Effect.provide(persistenceLayer));
    assert.equal(legacyTableRows.length, 0);

    NodeFS.rmSync(tempDir, { recursive: true, force: true });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive restores rollback routing after restart using persisted thread mapping",
  () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-restart-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

      const firstCodex = makeFakeCodexAdapter();
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: firstCodex.adapter,
      });
      const firstLayers = makeProviderLayers({ registry: firstRegistry, dbPath });
      const updatedResumeCursor = {
        threadId: asThreadId("thread-1"),
        resume: "resume-session-1",
        resumeSessionAt: "assistant-message-1",
        turnCount: 1,
      };

      const startedSession = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const threadId = asThreadId("thread-1");
        const session = yield* startTestSession(provider, threadId, { cwd: "/tmp/project" });
        firstCodex.updateSession(threadId, (existing) => ({
          ...existing,
          status: "ready",
          resumeCursor: updatedResumeCursor,
          updatedAt: "2026-01-01T00:00:01.000Z",
        }));
        return session;
      }).pipe(Effect.provide(firstLayers.serviceLayer));

      const persistedAfterStopAll = expectSome(
        yield* Effect.gen(function* () {
          const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
          return yield* repository.getByThreadId({
            threadId: startedSession.threadId,
          });
        }).pipe(Effect.provide(firstLayers.runtimeRepositoryLayer)),
      );
      assert.equal(persistedAfterStopAll.status, "stopped");
      assert.deepEqual(persistedAfterStopAll.resumeCursor, updatedResumeCursor);

      const secondCodex = makeFakeCodexAdapter();
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: secondCodex.adapter,
      });
      const secondLayers = makeProviderLayers({ registry: secondRegistry, dbPath });

      secondCodex.startSession.mockClear();
      secondCodex.rollbackThread.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.withSessionLifecycleLock(
          startedSession.threadId,
          provider.rollbackConversation({
            threadId: startedSession.threadId,
            turnIds: [],
            anchorTurnId: asTurnId("turn-1"),
          }),
        );
      }).pipe(Effect.provide(secondLayers.serviceLayer));

      assertStartCalledWith(secondCodex.startSession.mock.calls, {
        provider: CODEX_DRIVER,
        cwd: "/tmp/project",
        resumeCursor: updatedResumeCursor,
        threadId: startedSession.threadId,
      });
      assert.equal(secondCodex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = secondCodex.rollbackThread.mock.calls[0];
      assert.equal(typeof rollbackCall?.[0], "string");
      assert.deepEqual(rollbackCall?.[1], {
        turnIds: [],
        anchorTurnId: asTurnId("turn-1"),
      });

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
);

routing.layer("ProviderServiceLive routing", (it) => {
  it.effect("routes provider operations and rollback conversation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* startTestSession(provider, asThreadId("thread-1"), {
        cwd: "/tmp/project",
      });
      assert.equal(session.provider, "codex");

      const sessions = yield* provider.listSessions();
      assert.equal(sessions.length, 1);

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);

      yield* provider.interruptTurn({ threadId: session.threadId });
      assert.deepEqual(routing.codex.interruptTurn.mock.calls, [[session.threadId, undefined]]);

      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-1"),
        decision: "accept",
      });
      assert.deepEqual(routing.codex.respondToRequest.mock.calls, [
        [session.threadId, asRequestId("req-1"), "accept"],
      ]);

      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-user-input-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      assert.deepEqual(routing.codex.respondToUserInput.mock.calls, [
        [
          session.threadId,
          asRequestId("req-user-input-1"),
          {
            sandbox_mode: "workspace-write",
          },
        ],
      ]);

      yield* provider.withSessionLifecycleLock(
        session.threadId,
        provider.rollbackConversation({
          threadId: session.threadId,
          turnIds: [asTurnId("turn-1")],
        }),
      );

      yield* provider.stopSession({ threadId: session.threadId });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "after-stop",
        attachments: [],
      });

      assertStartCalledWith(routing.codex.startSession.mock.calls, {
        provider: CODEX_DRIVER,
        cwd: "/tmp/project",
        resumeCursor: session.resumeCursor,
        threadId: session.threadId,
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("holds the session lifecycle lock until an approval response is committed", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-approval-interrupt-order");
      yield* startTestSession(provider, threadId);
      const responseEntered = yield* Deferred.make<void>();
      const releaseResponse = yield* Deferred.make<void>();
      routing.codex.respondToRequest.mockImplementationOnce(() =>
        Deferred.succeed(responseEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseResponse)),
        ),
      );
      routing.codex.interruptTurn.mockClear();

      const response = yield* provider
        .respondToRequest({
          threadId,
          requestId: asRequestId("req-approval-interrupt-order"),
          decision: "accept",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(responseEntered);
      const interrupt = yield* provider
        .interruptTurn({ threadId })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      assert.equal(routing.codex.interruptTurn.mock.calls.length, 0);

      yield* Deferred.succeed(releaseResponse, undefined);
      yield* Fiber.join(response);
      yield* Fiber.join(interrupt);
      assert.deepEqual(routing.codex.interruptTurn.mock.calls, [[threadId, undefined]]);
    }),
  );

  it.effect("persists an invalidated adapter session as non-resumable", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-adapter-session-invalidated");
      yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      const sessionLease = yield* getPersistedSessionLease(directory, threadId);
      routing.droid.sendTurn.mockImplementationOnce((input) =>
        routing.droid.adapter.stopSession(input.threadId).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapterSessionInvalidatedError({
                provider: DROID_DRIVER,
                threadId,
                operation: "sendTurn",
                cause: new Error("uncertain Droid mutation"),
              }),
            ),
          ),
        ),
      );

      const failure = yield* provider
        .sendTurn({
          threadId,
          input: "invalidate this session",
          attachments: [],
        })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterSessionInvalidatedError");

      const persisted = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(persisted.status, "stopped");
      assert.equal(persisted.sessionLease, null);
      assert.equal(persisted.resumeCursor, null);
      assert.deepInclude(persisted.runtimePayload, {
        activeTurnId: null,
        lastRuntimeEvent: "provider.session.invalidated",
      });
      assert.isFalse(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: droidInstanceId,
          sessionLease,
        }),
      );
    }),
  );

  it.effect("persists rollback session invalidation before returning the adapter error", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-rollback-session-invalidated");
      yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      routing.droid.rollbackThread.mockImplementationOnce((stoppedThreadId) =>
        routing.droid.adapter.stopSession(stoppedThreadId).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapterSessionInvalidatedError({
                provider: DROID_DRIVER,
                threadId,
                operation: "rollbackThread",
                cause: new Error("uncertain Droid rewind load"),
              }),
            ),
          ),
        ),
      );

      const failure = yield* provider.withSessionLifecycleLock(
        threadId,
        provider
          .rollbackConversation({
            threadId,
            turnIds: [],
            anchorTurnId: asTurnId("turn-rollback-session-invalidated"),
          })
          .pipe(Effect.flip),
      );
      assert.equal(failure._tag, "ProviderAdapterSessionInvalidatedError");

      const persisted = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(persisted.status, "stopped");
      assert.equal(persisted.sessionLease, null);
      assert.equal(persisted.resumeCursor, null);
    }),
  );

  it.effect("routes feedback to the Codex adapter and returns its feedback ID", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-feedback-route");
      yield* startTestSession(provider, threadId);
      routing.codex.uploadFeedback.mockClear();

      const result = yield* provider.uploadFeedback({
        threadId,
        reason: "The agent stopped early.",
      });

      assert.deepStrictEqual(result, { feedbackId: `feedback-${threadId}` });
      assert.deepStrictEqual(routing.codex.uploadFeedback.mock.calls, [
        [{ threadId, reason: "The agent stopped early." }],
      ]);
    }),
  );

  it.effect("recovers a stopped Codex session before uploading feedback", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-feedback-recover");
      yield* startTestSession(provider, threadId, { cwd: "/tmp/feedback-project" });
      yield* routing.codex.stopSession(threadId);
      routing.codex.startSession.mockClear();
      routing.codex.uploadFeedback.mockClear();

      const result = yield* provider.uploadFeedback({ threadId });

      assert.deepStrictEqual(result, { feedbackId: `feedback-${threadId}` });
      assert.strictEqual(routing.codex.startSession.mock.calls.length, 1);
      assert.deepStrictEqual(routing.codex.uploadFeedback.mock.calls, [[{ threadId }]]);
    }),
  );

  it.effect("rejects feedback for providers that do not support uploads", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-feedback-claude");
      yield* startTestSession(provider, threadId, { driver: CLAUDE_AGENT_DRIVER });

      const error = yield* provider.uploadFeedback({ threadId }).pipe(Effect.flip);

      assert.instanceOf(error, ProviderValidationError);
      assert.include(error.issue, "does not support feedback uploads");
      routing.claude.startSession.mockClear();
    }),
  );

  it.effect("does not restart an unsupported provider before rejecting feedback", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-feedback-unsupported-stopped");
      yield* startTestSession(provider, threadId, { driver: CLAUDE_AGENT_DRIVER });
      yield* routing.claude.stopSession(threadId);
      routing.claude.startSession.mockClear();

      const error = yield* provider.uploadFeedback({ threadId }).pipe(Effect.flip);

      assert.instanceOf(error, ProviderValidationError);
      assert.include(error.issue, "does not support feedback uploads");
      assert.strictEqual(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("appends attachment file paths to the turn input text", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* startTestSession(provider, asThreadId("thread-attach"), {
        cwd: "/tmp/project",
      });

      const attachment = {
        type: "image" as const,
        id: "thread-attach-12345678-1234-1234-1234-123456789abc",
        name: "screenshot.png",
        mimeType: "image/png",
        sizeBytes: 123,
      };

      routing.codex.sendTurn.mockClear();
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "use this screenshot",
        attachments: [attachment],
      });

      const turnInput = routing.codex.sendTurn.mock.calls[0]?.[0] as ProviderSendTurnInput;
      assert.equal(typeof turnInput.input, "string");
      const turnText = turnInput.input ?? "";
      assert.equal(turnText.startsWith("use this screenshot"), true);
      assert.include(turnText, '[Attached image "screenshot.png" is saved at: ');
      assert.equal(turnText.endsWith(`${attachment.id}.png]`), true);

      routing.codex.sendTurn.mockClear();
      yield* provider.sendTurn({
        threadId: session.threadId,
        attachments: [attachment],
      });
      const imageOnlyInput = routing.codex.sendTurn.mock.calls[0]?.[0] as ProviderSendTurnInput;
      assert.equal(imageOnlyInput.input?.startsWith('[Attached image "screenshot.png"'), true);

      yield* provider.stopSession({ threadId: session.threadId });
    }),
  );

  it.effect("recovers stale persisted sessions for rollback by resuming thread identity", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* startTestSession(provider, asThreadId("thread-1"), {
        cwd: "/tmp/project",
      });
      yield* routing.codex.stopSession(initial.threadId);
      routing.codex.startSession.mockClear();
      routing.codex.rollbackThread.mockClear();

      yield* provider.withSessionLifecycleLock(
        initial.threadId,
        provider.rollbackConversation({
          threadId: initial.threadId,
          turnIds: [],
          anchorTurnId: asTurnId("turn-1"),
        }),
      );

      assertStartCalledWith(routing.codex.startSession.mock.calls, {
        provider: CODEX_DRIVER,
        cwd: "/tmp/project",
        resumeCursor: initial.resumeCursor,
        threadId: initial.threadId,
      });
      assert.equal(routing.codex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = routing.codex.rollbackThread.mock.calls[0];
      assert.deepEqual(rollbackCall?.[1], {
        turnIds: [],
        anchorTurnId: asTurnId("turn-1"),
      });
    }),
  );

  it.effect("does not rewrite the binding when rollback returns no resume cursor", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-rollback-snapshot");
      const session = yield* startTestSession(provider, threadId);
      const beforeRollback = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));

      yield* advanceTestClock(50);
      yield* provider.withSessionLifecycleLock(
        threadId,
        provider.rollbackConversation({
          threadId,
          turnIds: [],
          anchorTurnId: asTurnId("turn-1"),
        }),
      );

      const afterRollback = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(afterRollback.lastSeenAt, beforeRollback.lastSeenAt);
      assert.deepEqual(afterRollback.resumeCursor, session.resumeCursor);
    }),
  );

  it.effect("preserves the persisted binding when stopping a session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const initial = yield* startTestSession(provider, asThreadId("thread-reap-preserve"), {
        cwd: "/tmp/project-reap-preserve",
      });

      yield* provider.stopSession({ threadId: initial.threadId });

      const persistedAfterStop = expectSome(
        yield* runtimeRepository.getByThreadId({ threadId: initial.threadId }),
      );
      assert.equal(persistedAfterStop.status, "stopped");
      assert.deepEqual(persistedAfterStop.resumeCursor, initial.resumeCursor);

      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume after reap",
        attachments: [],
      });

      assertStartCalledWith(routing.codex.startSession.mock.calls, {
        provider: CODEX_DRIVER,
        cwd: "/tmp/project-reap-preserve",
        resumeCursor: initial.resumeCursor,
        threadId: initial.threadId,
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("routes explicit claudeAgent provider session starts to the claude adapter", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* startTestSession(provider, asThreadId("thread-claude"), {
        driver: ProviderDriverKind.make("claudeAgent"),
        cwd: "/tmp/project-claude",
      });

      assert.equal(session.provider, "claudeAgent");
      assertStartCalledWith(routing.claude.startSession.mock.calls, {
        provider: CLAUDE_AGENT_DRIVER,
        providerInstanceId: claudeAgentInstanceId,
        cwd: "/tmp/project-claude",
      });
    }),
  );

  it.effect("dies when an active session conflicts with its persisted binding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-binding-mismatch");

      yield* startTestSession(provider, threadId, { cwd: "/tmp/project-binding-mismatch" });
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        runtimeMode: "full-access",
      });

      const exit = yield* Effect.exit(provider.listSessions());
      assert.equal(Exit.hasDies(exit), true);
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
      });
    }),
  );

  it.effect("stops stale sessions in other providers after a successful replacement start", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-provider-replacement");

      const codexSession = yield* startTestSession(provider, threadId, {
        cwd: "/tmp/project-provider-replacement",
      });
      const codexLease = yield* getPersistedSessionLease(directory, threadId);
      let ownershipAtReplacementStart = true;

      routing.codex.stopSession.mockClear();
      routing.claude.stopSession.mockClear();
      routing.claude.startSession.mockImplementationOnce((input) =>
        directory
          .matchesOwnership({
            threadId,
            providerInstanceId: codexInstanceId,
            sessionLease: codexLease,
          })
          .pipe(
            Effect.tap((matches) =>
              Effect.sync(() => {
                ownershipAtReplacementStart = matches;
              }),
            ),
            Effect.andThen(
              Effect.suspend(() => routing.claude.adapter.startSession(input)).pipe(Effect.orDie),
            ),
          ),
      );

      const claudeSession = yield* startTestSession(provider, threadId, {
        driver: ProviderDriverKind.make("claudeAgent"),
        cwd: "/tmp/project-provider-replacement",
      });
      const claudeLease = yield* getPersistedSessionLease(directory, threadId);

      assert.equal(codexSession.provider, "codex");
      assert.equal(claudeSession.provider, "claudeAgent");
      assert.isFalse(ownershipAtReplacementStart);
      assert.deepEqual(routing.codex.stopSession.mock.calls, [[threadId]]);
      assert.equal(routing.claude.stopSession.mock.calls.length, 0);
      assert.isTrue(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: claudeAgentInstanceId,
          sessionLease: claudeLease,
        }),
      );

      const sessions = yield* provider.listSessions();
      assert.deepEqual(
        sessions
          .filter((session) => session.threadId === threadId)
          .map((session) => session.provider),
        ["claudeAgent"],
      );
    }),
  );

  it.effect("leaves volatile ownership invalid when a replacement start fails", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-provider-replacement-failure");
      yield* startTestSession(provider, threadId);
      const originalLease = yield* getPersistedSessionLease(directory, threadId);
      let ownershipAtReplacementStart = true;

      routing.claude.startSession.mockImplementationOnce(() =>
        directory
          .matchesOwnership({
            threadId,
            providerInstanceId: codexInstanceId,
            sessionLease: originalLease,
          })
          .pipe(
            Effect.tap((matches) =>
              Effect.sync(() => {
                ownershipAtReplacementStart = matches;
              }),
            ),
            Effect.andThen(
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: String(CLAUDE_AGENT_DRIVER),
                  method: "startSession",
                  detail: "simulated replacement failure",
                }),
              ),
            ),
          ),
      );

      const failure = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);

      assert.instanceOf(failure, ProviderAdapterRequestError);
      assert.isFalse(ownershipAtReplacementStart);
      assert.isFalse(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: codexInstanceId,
          sessionLease: originalLease,
        }),
      );
      const persisted = expectSome(yield* directory.getBinding(threadId));
      assert.equal(persisted.providerInstanceId, codexInstanceId);
      assert.equal(persisted.sessionLease, originalLease);

      const received: ProviderRuntimeEvent[] = [];
      const subscriber = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Effect.sync(() => received.push(event)),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);
      routing.codex.emitRaw({
        type: "content.delta",
        sessionLease: originalLease,
        eventId: asEventId("evt-invalidated-durable-fallback"),
        provider: CODEX_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-invalidated-durable-fallback"),
        payload: {
          streamKind: "assistant_text",
          delta: "must stay dropped",
        },
      });
      yield* advanceTestClock(50);
      assert.isEmpty(received);
      yield* Fiber.interrupt(subscriber);
    }),
  );

  it.effect("invalidates ownership before recovering a replacement session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-recovery-ownership-invalidation");
      const original = yield* startTestSession(provider, threadId);
      const originalLease = yield* getPersistedSessionLease(directory, threadId);
      yield* routing.codex.stopSession(threadId);
      let ownershipAtRecoveryStart = true;

      routing.codex.startSession.mockImplementationOnce((input) =>
        directory
          .matchesOwnership({
            threadId,
            providerInstanceId: codexInstanceId,
            sessionLease: originalLease,
          })
          .pipe(
            Effect.tap((matches) =>
              Effect.sync(() => {
                ownershipAtRecoveryStart = matches;
              }),
            ),
            Effect.andThen(
              Effect.suspend(() => routing.codex.adapter.startSession(input)).pipe(Effect.orDie),
            ),
          ),
      );

      const recovered = yield* provider.recoverSession(threadId);
      const recoveredLease = yield* getPersistedSessionLease(directory, threadId);

      assert.isFalse(ownershipAtRecoveryStart);
      assert.notEqual(recoveredLease, originalLease);
      assert.isTrue(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: codexInstanceId,
          sessionLease: recoveredLease,
        }),
      );
      assert.equal(recovered.provider, original.provider);
    }),
  );

  it.effect("invalidates ownership after stopping a session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-stop-ownership-invalidation");
      yield* startTestSession(provider, threadId);
      const sessionLease = yield* getPersistedSessionLease(directory, threadId);
      let ownershipAtStop = true;
      const exited = yield* provider.streamEvents.pipe(
        Stream.filter(
          (event) => event.type === "session.exited" && String(event.threadId) === String(threadId),
        ),
        Stream.runHead,
        Effect.forkChild({ startImmediately: true }),
      );

      routing.codex.stopSession.mockImplementationOnce((stoppedThreadId) =>
        directory
          .matchesOwnership({
            threadId,
            providerInstanceId: codexInstanceId,
            sessionLease,
          })
          .pipe(
            Effect.tap((matches) =>
              Effect.sync(() => {
                ownershipAtStop = matches;
              }),
            ),
            Effect.andThen(
              Effect.sync(() =>
                routing.codex.emitRaw({
                  type: "session.exited",
                  sessionLease,
                  eventId: asEventId("evt-stop-session"),
                  provider: CODEX_DRIVER,
                  createdAt: "2026-01-01T00:00:00.000Z",
                  threadId,
                  payload: { exitKind: "graceful" },
                }),
              ),
            ),
            Effect.andThen(
              Effect.suspend(() => routing.codex.adapter.stopSession(stoppedThreadId)).pipe(
                Effect.orDie,
              ),
            ),
          ),
      );

      assert.equal(yield* provider.stopSession({ threadId }), "stopped");
      assert.isTrue(ownershipAtStop);
      assert.isTrue(Option.isSome(yield* Fiber.join(exited)));
      assert.isFalse(
        yield* directory.matchesOwnership({
          threadId,
          providerInstanceId: codexInstanceId,
          sessionLease,
        }),
      );
    }),
  );

  it.effect("routes interrupt to replacement ownership after lifecycle mutation completes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-interrupt-replacement-race");
      yield* startTestSession(provider, threadId);

      const replacementStartEntered = yield* Deferred.make<void>();
      const allowReplacementStart = yield* Deferred.make<void>();
      routing.claude.startSession.mockImplementationOnce((input) =>
        Deferred.succeed(replacementStartEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowReplacementStart)),
          Effect.andThen(
            Effect.suspend(() => routing.claude.adapter.startSession(input)).pipe(Effect.orDie),
          ),
        ),
      );
      routing.codex.interruptTurn.mockClear();
      routing.claude.interruptTurn.mockClear();

      const replacementStart = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(replacementStartEntered);

      const interruptAttempted = yield* Deferred.make<void>();
      const interrupt = yield* Deferred.succeed(interruptAttempted, undefined).pipe(
        Effect.andThen(provider.interruptTurn({ threadId })),
        Effect.forkChild,
      );
      yield* Deferred.await(interruptAttempted);
      yield* Effect.yieldNow;
      assert.equal(routing.codex.interruptTurn.mock.calls.length, 0);
      assert.equal(routing.claude.interruptTurn.mock.calls.length, 0);

      yield* Deferred.succeed(allowReplacementStart, undefined);
      yield* Fiber.join(replacementStart);
      yield* Fiber.join(interrupt);

      assert.equal(routing.codex.interruptTurn.mock.calls.length, 0);
      assert.deepEqual(routing.claude.interruptTurn.mock.calls, [[threadId, undefined]]);
    }),
  );

  it.effect("does not persist a delayed stop over a replacement session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-stop-replacement-overlap");

      yield* startTestSession(provider, threadId);

      const stopEntered = yield* Deferred.make<void>();
      const allowStop = yield* Deferred.make<void>();
      routing.codex.stopSession.mockImplementationOnce(() =>
        Deferred.succeed(stopEntered, undefined).pipe(Effect.andThen(Deferred.await(allowStop))),
      );

      const delayedStop = yield* provider.stopSession({ threadId }).pipe(Effect.forkChild);
      yield* Deferred.await(stopEntered);

      const replacementStart = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.succeed(allowStop, undefined);
      yield* Fiber.join(delayedStop);
      yield* Fiber.join(replacementStart);

      const binding = expectSome(yield* directory.getBinding(threadId));
      assert.equal(binding.provider, CLAUDE_AGENT_DRIVER);
      assert.equal(binding.providerInstanceId, claudeAgentInstanceId);
      assert.notEqual(binding.sessionLease, null);
      assert.equal(binding.status, "running");
    }),
  );

  it.effect("does not persist a delayed send over a replacement session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-send-replacement-overlap");

      yield* startTestSession(provider, threadId);

      const sendEntered = yield* Deferred.make<void>();
      const allowSend = yield* Deferred.make<void>();
      routing.codex.sendTurn.mockImplementationOnce((input) =>
        Deferred.succeed(sendEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowSend)),
          Effect.andThen(
            Effect.succeed({
              threadId: input.threadId,
              turnId: asTurnId("turn-delayed-send"),
              resumeCursor: { opaque: "delayed-send-cursor" },
            }),
          ),
        ),
      );

      const delayedSend = yield* provider
        .sendTurn({
          threadId,
          input: "delayed send",
          attachments: [],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(sendEntered);

      const replacementStart = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      const replacementBeforeSendFiber = yield* Fiber.join(replacementStart).pipe(
        Effect.timeoutOption("1 second"),
        Effect.forkChild,
      );
      yield* advanceTestClock(1_000);
      const replacementBeforeSend = yield* Fiber.join(replacementBeforeSendFiber);

      yield* Deferred.succeed(allowSend, undefined);
      yield* Fiber.join(delayedSend);
      if (Option.isNone(replacementBeforeSend)) {
        yield* Fiber.join(replacementStart);
      }

      assert.equal(Option.isNone(replacementBeforeSend), true);
      const binding = expectSome(yield* directory.getBinding(threadId));
      assert.equal(binding.provider, CLAUDE_AGENT_DRIVER);
      assert.equal(binding.providerInstanceId, claudeAgentInstanceId);
      assert.notEqual(binding.sessionLease, null);
      assert.equal(binding.status, "running");
      assert.notDeepEqual(binding.resumeCursor, { opaque: "delayed-send-cursor" });
    }),
  );

  it.effect("serializes overlapping replacement session starts per thread", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-start-replacement-overlap");
      const startEntered = yield* Deferred.make<void>();
      const allowStart = yield* Deferred.make<void>();

      routing.codex.startSession.mockImplementationOnce((input) =>
        Deferred.succeed(startEntered, undefined).pipe(
          Effect.andThen(Deferred.await(allowStart)),
          Effect.andThen(
            Effect.suspend(() => routing.codex.adapter.startSession(input)).pipe(Effect.orDie),
          ),
        ),
      );

      const originalStart = yield* provider
        .startSession(threadId, {
          provider: CODEX_DRIVER,
          providerInstanceId: codexInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startEntered);
      const replacementStart = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: claudeAgentInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);

      yield* Deferred.succeed(allowStart, undefined);
      yield* Fiber.join(originalStart);
      yield* Fiber.join(replacementStart);

      const binding = expectSome(yield* directory.getBinding(threadId));
      assert.equal(binding.provider, CLAUDE_AGENT_DRIVER);
      assert.equal(binding.providerInstanceId, claudeAgentInstanceId);
      assert.notEqual(binding.sessionLease, null);
      assert.equal(binding.status, "running");
    }),
  );

  it.effect("recovers stale sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* startTestSession(provider, asThreadId("thread-1"), {
        cwd: "/tmp/project-send-turn",
      });

      yield* routing.codex.stopAll();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume",
        attachments: [],
      });

      assertStartCalledWith(routing.codex.startSession.mock.calls, {
        provider: CODEX_DRIVER,
        cwd: "/tmp/project-send-turn",
        resumeCursor: initial.resumeCursor,
        threadId: initial.threadId,
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("recovers stale claudeAgent sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* startTestSession(provider, asThreadId("thread-claude-send-turn"), {
        driver: ProviderDriverKind.make("claudeAgent"),
        cwd: "/tmp/project-claude-send-turn",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "effort", value: "max" }],
        ),
      });

      yield* routing.claude.stopAll();
      routing.claude.startSession.mockClear();
      routing.claude.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume with claude",
        attachments: [],
      });

      assertStartCalledWith(routing.claude.startSession.mock.calls, {
        provider: CLAUDE_AGENT_DRIVER,
        cwd: "/tmp/project-claude-send-turn",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "effort", value: "max" }],
        ),
        resumeCursor: initial.resumeCursor,
        threadId: initial.threadId,
      });
      assert.equal(routing.claude.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("lists no sessions after adapter runtime clears", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      yield* startTestSession(provider, asThreadId("thread-1"));
      yield* startTestSession(provider, asThreadId("thread-2"));

      yield* routing.codex.stopAll();
      yield* routing.claude.stopAll();

      const remaining = yield* provider.listSessions();
      assert.equal(remaining.length, 0);
    }),
  );

  it.effect("persists runtime status transitions in provider_session_runtime", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = asThreadId("thread-runtime-status");
      const session = yield* startTestSession(provider, threadId);
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      const runningRuntime = expectSome(
        yield* runtimeRepository.getByThreadId({ threadId: session.threadId }),
      );
      assert.equal(runningRuntime.status, "running");
      assert.deepEqual(runningRuntime.resumeCursor, session.resumeCursor);
      const payload = runningRuntime.runtimePayload;
      assert.equal(payload !== null && typeof payload === "object", true);
      if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
        const runtimePayload = payload as {
          cwd: string;
          model: string | null;
          activeTurnId: string | null;
          lastError: string | null;
          lastRuntimeEvent: string | null;
        };
        assert.equal(runtimePayload.cwd, session.cwd);
        assert.equal(runtimePayload.model, null);
        assert.equal(runtimePayload.activeTurnId, `turn-${String(session.threadId)}`);
        assert.equal(runtimePayload.lastError, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      }
    }),
  );

  it.effect("reuses persisted resume cursor when startSession is called after a restart", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-start-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

      const firstClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("claudeAgent")]: firstClaude.adapter,
      });
      const firstLayers = makeProviderLayers({ registry: firstRegistry, dbPath });

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* startTestSession(provider, asThreadId("thread-claude-start"), {
          driver: ProviderDriverKind.make("claudeAgent"),
          cwd: "/tmp/project-claude-start",
        });
      }).pipe(Effect.provide(firstLayers.serviceLayer));

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.listSessions();
      }).pipe(Effect.provide(firstLayers.serviceLayer));

      const secondClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("claudeAgent")]: secondClaude.adapter,
      });
      const secondLayers = makeProviderLayers({ registry: secondRegistry, dbPath });

      secondClaude.startSession.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* startTestSession(provider, initial.threadId, {
          driver: ProviderDriverKind.make("claudeAgent"),
          cwd: "/tmp/project-claude-start",
        });
      }).pipe(Effect.provide(secondLayers.serviceLayer));

      assertStartCalledWith(secondClaude.startSession.mock.calls, {
        provider: CLAUDE_AGENT_DRIVER,
        cwd: "/tmp/project-claude-start",
        resumeCursor: initial.resumeCursor,
        threadId: initial.threadId,
      });

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "reuses persisted cwd when startSession resumes a claude session without cwd input",
    () =>
      Effect.gen(function* () {
        const tempDir = NodeFS.mkdtempSync(
          NodePath.join(NodeOS.tmpdir(), "t3-provider-service-cwd-"),
        );
        const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

        const firstClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const firstRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: firstClaude.adapter,
        });
        const firstLayers = makeProviderLayers({ registry: firstRegistry, dbPath });

        const initial = yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          return yield* startTestSession(provider, asThreadId("thread-claude-cwd"), {
            driver: ProviderDriverKind.make("claudeAgent"),
            cwd: "/tmp/project-claude-cwd",
          });
        }).pipe(Effect.provide(firstLayers.serviceLayer));

        const secondClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const secondRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: secondClaude.adapter,
        });
        const secondLayers = makeProviderLayers({ registry: secondRegistry, dbPath });

        secondClaude.startSession.mockClear();

        yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          yield* startTestSession(provider, initial.threadId, {
            driver: ProviderDriverKind.make("claudeAgent"),
          });
        }).pipe(Effect.provide(secondLayers.serviceLayer));

        assertStartCalledWith(secondClaude.startSession.mock.calls, {
          provider: CLAUDE_AGENT_DRIVER,
          cwd: "/tmp/project-claude-cwd",
          resumeCursor: initial.resumeCursor,
          threadId: initial.threadId,
        });

        NodeFS.rmSync(tempDir, { recursive: true, force: true });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

it.effect("withholds a terminal event when its resume cursor cannot be persisted", () =>
  Effect.gen(function* () {
    const droid = makeFakeCodexAdapter(DROID_DRIVER);
    const registry = makeAdapterRegistryMock({ [DROID_DRIVER]: droid.adapter });
    const { fullLayer } = makeProviderLayers({
      registry,
      directoryOverride: (directory) => ({
        ...directory,
        updateResumeCursorIfOwned: () =>
          Effect.fail(
            new ProviderSessionDirectoryPersistenceError({
              operation: "updateResumeCursorIfOwned",
              detail: "simulated cursor persistence failure",
            }),
          ),
      }),
    });

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-cursor-persistence-failure");
      yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      const received: ProviderRuntimeEvent[] = [];
      const subscriber = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Effect.sync(() => received.push(event)),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);

      droid.emit({
        type: "turn.completed",
        eventId: asEventId("evt-cursor-persistence-failure"),
        provider: DROID_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-cursor-persistence-failure"),
        payload: {
          state: "completed",
          resumeCursor: { opaque: "must-not-publish" },
        },
      });
      yield* advanceTestClock(50);

      assert.isEmpty(received);
      yield* Fiber.interrupt(subscriber);
    }).pipe(Effect.provide(fullLayer));
  }).pipe(Effect.provide(NodeServices.layer)),
);

const fanout = makeProviderServiceLayer();
fanout.layer("ProviderServiceLive fanout", (it) => {
  it.effect("fans out adapter turn completion events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* startTestSession(provider, asThreadId("thread-1"));

      const eventsRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Ref.update(eventsRef, (current) => [...current, event]),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);

      const completedEvent: ProviderRuntimeEventFixture = {
        type: "turn.completed",
        eventId: asEventId("evt-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        payload: { state: "completed" },
      };

      const listSessionsCallCount = fanout.codex.listSessions.mock.calls.length;
      fanout.codex.emit(completedEvent);
      yield* advanceTestClock(50);

      const events = yield* Ref.get(eventsRef);
      yield* Fiber.interrupt(consumer);

      assert.equal(
        events.some((entry) => entry.type === "turn.completed"),
        true,
      );
      assert.equal(
        events.some(
          (entry) =>
            entry.type === "turn.completed" && entry.providerInstanceId === codexInstanceId,
        ),
        true,
      );
      assert.equal(fanout.codex.listSessions.mock.calls.length, listSessionsCallCount);
    }),
  );

  it.effect("skips completed-turn cursor writes when the event carries no cursor", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-snapshot-unchanged");
      const session = yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });

      const initialRuntime = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));

      for (const eventId of ["evt-snapshot-unchanged-1", "evt-snapshot-unchanged-2"]) {
        fanout.droid.emit({
          type: "turn.completed",
          eventId: asEventId(eventId),
          provider: DROID_DRIVER,
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId,
          turnId: asTurnId(`turn-${eventId}`),
          payload: { state: "completed" },
        });
        yield* advanceTestClock(50);
      }

      const unchangedRuntime = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(unchangedRuntime.lastSeenAt, initialRuntime.lastSeenAt);
      assert.deepEqual(unchangedRuntime.resumeCursor, session.resumeCursor);
    }),
  );

  it.effect("persists the resume cursor carried by turn completion", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-snapshot-changed");
      const session = yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      const changedResumeCursor = {
        opaque: `resume-successor-${String(threadId)}`,
      };
      fanout.droid.emit({
        type: "turn.completed",
        eventId: asEventId("evt-snapshot-changed"),
        provider: DROID_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-snapshot-changed"),
        payload: {
          state: "completed",
          resumeCursor: changedResumeCursor,
        },
      });
      yield* advanceTestClock(50);

      const changedRuntime = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.notDeepEqual(changedRuntime.resumeCursor, session.resumeCursor);
      assert.deepEqual(changedRuntime.resumeCursor, changedResumeCursor);
    }),
  );

  it.effect("suppresses stale completion from a replaced session under the same instance", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-stale-terminal-cursor");
      yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      const originalRuntime = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));

      const replacementCursor = { opaque: "replacement-cursor" };
      const replacementLease = ProviderSessionLease.make("lease-replacement");
      const publishedEvents: ProviderRuntimeEvent[] = [];
      const eventFiber = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Effect.sync(() => publishedEvents.push(event)),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);
      yield* directory.upsert({
        threadId,
        provider: DROID_DRIVER,
        providerInstanceId: droidInstanceId,
        sessionLease: replacementLease,
        resumeCursor: replacementCursor,
      });

      fanout.droid.emit({
        type: "turn.completed",
        eventId: asEventId("evt-stale-terminal-cursor"),
        provider: DROID_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-stale-terminal-cursor"),
        payload: {
          state: "completed",
          resumeCursor: { opaque: "stale-cursor" },
        },
      });
      yield* advanceTestClock(50);

      const persisted = expectSome(yield* runtimeRepository.getByThreadId({ threadId }));
      assert.equal(persisted.providerInstanceId, droidInstanceId);
      assert.equal(persisted.sessionLease, replacementLease);
      assert.deepEqual(persisted.resumeCursor, replacementCursor);
      assert.notEqual(originalRuntime.sessionLease, replacementLease);
      assert.isEmpty(publishedEvents);
      yield* Fiber.interrupt(eventFiber);
    }),
  );

  it.effect("persists a dynamic resume cursor before publishing turn completion", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-snapshot-before-terminal");
      yield* startTestSession(provider, threadId, { driver: DROID_DRIVER });
      const expectedCursor = { opaque: "successor-before-terminal" };
      const persistedBeforeTerminal = yield* Deferred.make<boolean>();
      const runtimeEventsFiber = yield* Stream.runForEach(provider.streamEvents, (event) =>
        event.type === "turn.completed" && event.threadId === threadId
          ? runtimeRepository.getByThreadId({ threadId }).pipe(
              Effect.flatMap((binding) =>
                Deferred.succeed(
                  persistedBeforeTerminal,
                  Option.isSome(binding) &&
                    JSON.stringify(binding.value.resumeCursor) === JSON.stringify(expectedCursor),
                ),
              ),
              Effect.asVoid,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);

      fanout.droid.emit({
        type: "turn.completed",
        eventId: asEventId("evt-snapshot-before-terminal"),
        provider: DROID_DRIVER,
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId,
        turnId: asTurnId("turn-snapshot-before-terminal"),
        payload: {
          state: "completed",
          resumeCursor: expectedCursor,
        },
      });
      assert.equal(yield* Deferred.await(persistedBeforeTerminal), true);
      yield* Fiber.interrupt(runtimeEventsFiber);
    }),
  );

  it.effect("fans out canonical runtime events in emission order", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* startTestSession(provider, asThreadId("thread-seq"));

      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "item.started",
        eventId: asEventId("evt-seq-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        payload: {
          itemType: "command_execution",
          title: "Ran command",
        },
      });
      fanout.codex.emit({
        type: "item.completed",
        eventId: asEventId("evt-seq-2"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        payload: {
          itemType: "command_execution",
          status: "completed",
          title: "Ran command",
        },
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-seq-3"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        payload: { state: "completed" },
      });

      yield* Fiber.join(consumer);
      const received = yield* Ref.get(receivedRef);
      assert.deepEqual(
        received.map((event) => event.eventId),
        [asEventId("evt-seq-1"), asEventId("evt-seq-2"), asEventId("evt-seq-3")],
      );
    }),
  );

  it.effect("keeps subscriber delivery ordered and isolates failing subscribers", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const session = yield* startTestSession(provider, asThreadId("thread-1"));

      const receivedByHealthy: string[] = [];
      const expectedEventIds = new Set<string>(["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"]);
      const healthyFiber = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            receivedByHealthy.push(event.eventId);
          }),
        ),
        Effect.forkChild,
      );
      const failingFiber = yield* Stream.take(provider.streamEvents, 1).pipe(
        Stream.runForEach(() => Effect.fail("listener crash")),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      const events: ReadonlyArray<ProviderRuntimeEventFixture> = [
        {
          type: "item.completed",
          eventId: asEventId("evt-ordered-1"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          payload: {
            itemType: "command_execution",
            status: "completed",
            title: "Ran command",
            detail: "echo one",
          },
        },
        {
          type: "content.delta",
          eventId: asEventId("evt-ordered-2"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          payload: {
            streamKind: "assistant_text",
            delta: "hello",
          },
        },
        {
          type: "turn.completed",
          eventId: asEventId("evt-ordered-3"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          payload: { state: "completed" },
        },
      ];

      for (const event of events) {
        fanout.codex.emit(event);
      }
      const failingResult = yield* Effect.result(Fiber.join(failingFiber));
      assert.equal(failingResult._tag, "Failure");
      yield* Fiber.join(healthyFiber);

      assert.deepEqual(
        receivedByHealthy.filter((eventId) => expectedEventIds.has(eventId)).slice(0, 3),
        ["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"],
      );
    }),
  );

  it.effect("records provider metrics with the routed provider label", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* startTestSession(provider, asThreadId("thread-metrics"), {
        cwd: "/tmp/project",
      });

      yield* provider.interruptTurn({ threadId: session.threadId });
      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-1"),
        decision: "accept",
      });
      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-2"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      yield* provider.withSessionLifecycleLock(
        session.threadId,
        provider.rollbackConversation({
          threadId: session.threadId,
          turnIds: [asTurnId("turn-1")],
        }),
      );
      yield* provider.stopSession({ threadId: session.threadId });

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: CODEX_DRIVER,
          operation: "interrupt",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: CODEX_DRIVER,
          operation: "approval-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: CODEX_DRIVER,
          operation: "user-input-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: CODEX_DRIVER,
          operation: "rollback",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_sessions_total", {
          provider: CODEX_DRIVER,
          operation: "stop",
          outcome: "success",
        }),
        true,
      );
    }),
  );

  it.effect(
    "records sendTurn metrics with the resolved provider when modelSelection is omitted",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;

        const session = yield* startTestSession(provider, asThreadId("thread-send-metrics"), {
          driver: ProviderDriverKind.make("claudeAgent"),
          cwd: "/tmp/project-send-metrics",
        });

        yield* provider.sendTurn({
          threadId: session.threadId,
          input: "hello",
          attachments: [],
        });

        const snapshots = yield* Metric.snapshot;

        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
            outcome: "success",
          }),
          true,
        );
        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turn_duration", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
          }),
          true,
        );
      }),
  );
});

const ownership = makeProviderServiceLayer();
ownership.layer("ProviderServiceLive ownership", (it) => {
  it.effect("skips stopping a session when the expected lease does not match", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-stop-lease-mismatch");

      yield* startTestSession(provider, threadId);

      const activeLease = expectSome(yield* directory.getBinding(threadId)).sessionLease;
      assert.notEqual(activeLease, null);
      if (activeLease === null || activeLease === undefined) {
        return;
      }

      ownership.codex.stopSession.mockClear();
      const mismatchOutcome = yield* provider.stopSession({
        threadId,
        expectedSessionLease: ProviderSessionLease.make("some-other-lease"),
      });
      assert.equal(mismatchOutcome, "ownership-mismatch");

      assert.equal(ownership.codex.stopSession.mock.calls.length, 0);
      const bindingAfterSkip = expectSome(yield* directory.getBinding(threadId));
      assert.equal(bindingAfterSkip.sessionLease, activeLease);
      assert.notEqual(bindingAfterSkip.status, "stopped");

      const stoppedOutcome = yield* provider.stopSession({
        threadId,
        expectedSessionLease: activeLease,
      });
      assert.equal(stoppedOutcome, "stopped");

      assert.equal(ownership.codex.stopSession.mock.calls.length, 1);
      const bindingAfterStop = expectSome(yield* directory.getBinding(threadId));
      assert.equal(bindingAfterStop.sessionLease, null);
      assert.equal(bindingAfterStop.status, "stopped");

      const bareThreadId = asThreadId("thread-stop-no-expectation");
      yield* startTestSession(provider, bareThreadId);
      ownership.codex.stopSession.mockClear();
      assert.equal(yield* provider.stopSession({ threadId: bareThreadId }), "stopped");

      assert.equal(ownership.codex.stopSession.mock.calls.length, 1);
      const bindingAfterBareStop = expectSome(yield* directory.getBinding(bareThreadId));
      assert.equal(bindingAfterBareStop.sessionLease, null);
      assert.equal(bindingAfterBareStop.status, "stopped");
    }),
  );

  it.effect(
    "re-validates a leased event after an in-flight lifecycle operation persists the lease",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        const threadId = asThreadId("thread-inflight-lease-revalidation");

        yield* startTestSession(provider, threadId);

        const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
        const consumer = yield* Stream.take(provider.streamEvents, 2).pipe(
          Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
          Effect.forkChild,
        );
        yield* advanceTestClock(50);

        const inFlightLease = ProviderSessionLease.make("lease-inflight-upsert");
        const staleLease = ProviderSessionLease.make("lease-never-persisted");

        const firstLockHeld = yield* Deferred.make<void>();
        const releaseFirstLock = yield* Deferred.make<void>();
        const firstLock = yield* provider
          .withSessionLifecycleLock(
            threadId,
            Deferred.succeed(firstLockHeld, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFirstLock)),
            ),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstLockHeld);

        ownership.codex.emitRaw({
          type: "content.delta",
          sessionLease: inFlightLease,
          eventId: asEventId("evt-inflight-lease"),
          provider: CODEX_DRIVER,
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId,
          turnId: asTurnId("turn-inflight-lease"),
          payload: {
            streamKind: "assistant_text",
            delta: "in-flight",
          },
        });
        yield* advanceTestClock(10);

        assert.equal((yield* Ref.get(receivedRef)).length, 0);

        yield* directory.upsert({
          threadId,
          provider: CODEX_DRIVER,
          providerInstanceId: codexInstanceId,
          sessionLease: inFlightLease,
        });
        yield* Deferred.succeed(releaseFirstLock, undefined);
        yield* Fiber.join(firstLock);

        const secondLockHeld = yield* Deferred.make<void>();
        const releaseSecondLock = yield* Deferred.make<void>();
        const secondLock = yield* provider
          .withSessionLifecycleLock(
            threadId,
            Deferred.succeed(secondLockHeld, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSecondLock)),
            ),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(secondLockHeld);

        ownership.codex.emitRaw({
          type: "content.delta",
          sessionLease: staleLease,
          eventId: asEventId("evt-stale-lease"),
          provider: CODEX_DRIVER,
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId,
          turnId: asTurnId("turn-inflight-lease"),
          payload: {
            streamKind: "assistant_text",
            delta: "stale",
          },
        });
        yield* advanceTestClock(10);
        yield* Deferred.succeed(releaseSecondLock, undefined);
        yield* Fiber.join(secondLock);

        ownership.codex.emitRaw({
          type: "content.delta",
          sessionLease: inFlightLease,
          eventId: asEventId("evt-current-lease"),
          provider: CODEX_DRIVER,
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId,
          turnId: asTurnId("turn-inflight-lease"),
          payload: {
            streamKind: "assistant_text",
            delta: "current",
          },
        });

        yield* Fiber.join(consumer);
        const received = yield* Ref.get(receivedRef);
        assert.deepEqual(
          received.map((event) => event.eventId),
          [asEventId("evt-inflight-lease"), asEventId("evt-current-lease")],
        );
      }),
  );
});

const validation = makeProviderServiceLayer();
validation.layer("ProviderServiceLive validation", (it) => {
  it.effect("rejects session starts without an explicit provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-missing-instance-id"), {
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-missing-instance-id"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "Provider instance id is required for provider 'codex'.");
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects mismatched provider kind and provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      validation.claude.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-instance-mismatch"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-instance-mismatch"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(
        failure.issue,
        "Provider instance 'claudeAgent' belongs to driver 'claudeAgent', not 'codex'.",
      );
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
      assert.equal(validation.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("returns ProviderValidationError for invalid input payloads", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const failure = yield* Effect.result(
        provider.startSession(asThreadId("thread-validation"), {
          threadId: asThreadId("thread-validation"),
          provider: "invalid-provider",
          runtimeMode: "full-access",
        } as never),
      );

      assert.equal(failure._tag, "Failure");
      if (failure._tag !== "Failure") {
        return;
      }
      assert.equal(failure.failure._tag, "ProviderValidationError");
      if (failure.failure._tag !== "ProviderValidationError") {
        return;
      }
      assert.equal(failure.failure.operation, "ProviderService.startSession");
      assert.equal(failure.failure.issue.includes("invalid-provider"), true);
    }),
  );

  it.effect("rejects empty rollback targets before session recovery", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-empty-rollback-target");

      yield* startTestSession(provider, threadId);
      yield* validation.codex.stopSession(threadId);
      validation.codex.startSession.mockClear();
      validation.codex.rollbackThread.mockClear();

      const result = yield* Effect.result(
        provider.rollbackConversation({
          threadId,
          turnIds: [],
        }),
      );

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.instanceOf(result.failure, ProviderValidationError);
        assert.equal(result.failure.operation, "ProviderService.rollbackConversation");
        assert.equal(
          result.failure.issue,
          "Rollback target must include at least one turn ID or an anchor turn ID.",
        );
      }
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
      assert.equal(validation.codex.rollbackThread.mock.calls.length, 0);
    }),
  );

  it.effect("rejects conversation rollback for unsupported providers", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-unsupported-rollback");

      yield* startTestSession(provider, threadId, { driver: CLAUDE_AGENT_DRIVER });
      validation.claude.rollbackThread.mockClear();

      const failure = yield* provider.withSessionLifecycleLock(
        threadId,
        provider
          .rollbackConversation({
            threadId,
            turnIds: [asTurnId("turn-1")],
          })
          .pipe(Effect.flip),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "does not support conversation rollback");
      assert.equal(validation.claude.rollbackThread.mock.calls.length, 0);
    }),
  );

  it.effect("dies when rollbackConversation runs without the session lifecycle lock", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-unlocked-rollback");

      yield* startTestSession(provider, threadId);
      validation.codex.rollbackThread.mockClear();

      const exit = yield* Effect.exit(
        provider.rollbackConversation({
          threadId,
          turnIds: [asTurnId("turn-1")],
        }),
      );

      assert.equal(Exit.hasDies(exit), true);
      assert.equal(validation.codex.rollbackThread.mock.calls.length, 0);

      const locked = yield* provider.withSessionLifecycleLock(
        threadId,
        provider.rollbackConversation({
          threadId,
          turnIds: [asTurnId("turn-1")],
        }),
      );
      assert.equal(locked.threadId, threadId);
      assert.equal(validation.codex.rollbackThread.mock.calls.length, 1);
    }),
  );

  it.effect("accepts startSession when adapter has not emitted provider thread id yet", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      validation.codex.startSession.mockImplementationOnce((input: ProviderSessionStartInput) =>
        Effect.sync(() => {
          const now = "2026-01-01T00:00:00.000Z";
          return {
            provider: ProviderDriverKind.make("codex"),
            sessionLease: ProviderSessionLease.make("lease-missing-provider-thread"),
            status: "ready",
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          } satisfies ProviderAdapterSession;
        }),
      );

      const session = yield* startTestSession(provider, asThreadId("thread-missing"), {
        cwd: "/tmp/project",
      });

      assert.equal(session.threadId, asThreadId("thread-missing"));

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.threadId, session.threadId);
      }
    }),
  );
});

describe("agent browser access", () => {
  const revokedThreads: Array<ThreadId> = [];

  const startSessionWith = (enableAgentBrowserAccess: boolean, threadId: ThreadId) =>
    Effect.gen(function* () {
      const issued: Array<ThreadId> = [];
      const codex = makeFakeCodexAdapter();
      const { serviceLayer } = makeProviderLayers({
        registry: makeAdapterRegistryMock({ [CODEX_DRIVER]: codex.adapter }),
        serviceOptions: {
          issueMcpCredential: (request) =>
            Effect.sync(() => {
              issued.push(request.threadId);
              return undefined;
            }),
          revokeMcpCredential: (revoked) => Effect.sync(() => void revokedThreads.push(revoked)),
        },
        settings: { enableAgentBrowserAccess },
      });

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* startTestSession(provider, threadId);
      }).pipe(Effect.provide(serviceLayer));

      return issued;
    });

  it.effect("issues or revokes MCP credentials according to browser access", () =>
    Effect.gen(function* () {
      for (const enabled of [false, true]) {
        const threadId = asThreadId(`thread-browser-${enabled ? "on" : "off"}`);
        revokedThreads.length = 0;
        const issued = yield* startSessionWith(enabled, threadId);

        assert.deepEqual(issued, enabled ? [threadId] : []);
        assert.deepEqual(revokedThreads, enabled ? [] : [threadId]);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

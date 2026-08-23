// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ApprovalRequestId,
  DroidSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { droidTokenUsageSnapshot, makeDroidAdapter } from "./DroidAdapter.ts";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/droid-mock-agent.ts");
const mockAgentCommand = process.execPath;

async function makeMockDroidWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "droid-jsonrpc-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-droid.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
exec ${JSON.stringify(mockAgentCommand)} ${JSON.stringify(mockAgentPath)} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

const droidAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-droid-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string, options?: Parameters<typeof makeDroidAdapter>[1]) =>
  makeDroidAdapter(decodeDroidSettings({ binaryPath }), options).pipe(Effect.orDie);

const eventsForThread = (events: ReadonlyArray<ProviderRuntimeEvent>, threadId: ThreadId) =>
  events.filter((event) => String(event.threadId) === String(threadId));

it("counts cache creation as processed spend but not live context", () => {
  const usage = {
    inputTokens: 20,
    outputTokens: 8,
    cacheCreationTokens: 6,
    cacheReadTokens: 4,
    thinkingTokens: 3,
  };
  assert.deepInclude(droidTokenUsageSnapshot(usage), {
    usedTokens: 32,
    totalProcessedTokens: 38,
  });
  assert.deepInclude(
    droidTokenUsageSnapshot(usage, {
      inputTokens: 7,
      cacheReadTokens: 2,
      outputTokens: 3,
    }),
    {
      usedTokens: 12,
      totalProcessedTokens: 38,
      lastUsedTokens: 12,
    },
  );
});

it.layer(droidAdapterTestLayer)("DroidAdapterLive", (it) => {
  it.effect("maps a Droid turn to ordered reasoning, assistant, usage, and completion events", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-turn-lifecycle");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("droid"),
          model: "mock-deep",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      });

      assert.equal(session.provider, "droid");
      assert.equal(session.model, "mock-deep");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });

      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "hello droid",
        attachments: [],
      });
      const terminal = yield* Deferred.await(turnCompleted);
      const threadEvents = eventsForThread(runtimeEvents, threadId);
      const turnEvents = threadEvents.filter(
        (event) => event.turnId !== undefined && String(event.turnId) === String(sentTurn.turnId),
      );

      assert.deepEqual(
        turnEvents.map((event) => event.type),
        [
          "turn.started",
          "item.started",
          "content.delta",
          "item.completed",
          "item.started",
          "content.delta",
          "content.delta",
          "item.completed",
          "turn.completed",
        ],
      );
      const contentDeltas = turnEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
          event.type === "content.delta",
      );
      assert.deepEqual(
        contentDeltas.map((event) => [event.payload.streamKind, event.payload.delta]),
        [
          ["reasoning_text", "Mock thinking"],
          ["assistant_text", "hello from "],
          ["assistant_text", "droid mock"],
        ],
      );
      assert.isTrue(contentDeltas.every((event) => event.raw === undefined));
      const startedItems = turnEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "item.started" }> =>
          event.type === "item.started",
      );
      assert.deepEqual(
        startedItems.map((event) => event.payload.itemType),
        ["reasoning", "assistant_message"],
      );
      assert.equal(terminal.payload.state, "completed");
      assert.equal(terminal.payload.stopReason, "completed");

      const usage = threadEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "thread.token-usage.updated" }> =>
          event.type === "thread.token-usage.updated",
      );
      assert.lengthOf(
        threadEvents.filter((event) => event.type === "thread.token-usage.updated"),
        1,
      );
      assert.deepEqual(usage?.payload.usage, {
        usedTokens: 12,
        totalProcessedTokens: 33,
        inputTokens: 20,
        cachedInputTokens: 4,
        outputTokens: 8,
        reasoningOutputTokens: 3,
        lastUsedTokens: 12,
        lastInputTokens: 7,
        lastCachedInputTokens: 2,
        lastOutputTokens: 3,
        compactsAutomatically: true,
      });
      assert.isTrue(
        threadEvents.findIndex((event) => event === usage) <
          threadEvents.findIndex((event) => event === terminal),
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("emits terminal usage when no usage notification arrived", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_OMIT_USAGE_NOTIFICATION: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const threadId = ThreadId.make("droid-usage-terminal-fallback");
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "fallback usage", attachments: [] });
      yield* Deferred.await(turnCompleted);

      assert.lengthOf(
        eventsForThread(runtimeEvents, threadId).filter(
          (event) => event.type === "thread.token-usage.updated",
        ),
        1,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reaps thread locks after session teardown and failed startup", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const failingWrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_FAIL_INIT: "1" }),
      );
      let readDebugState = (_threadId: ThreadId) =>
        Effect.succeed({ threadLockCount: -1, interruptedTurnCount: -1 });
      let readFailingDebugState = (_threadId: ThreadId) =>
        Effect.succeed({ threadLockCount: -1, interruptedTurnCount: -1 });
      const adapter = yield* makeTestAdapter(wrapperPath, {
        registerDebugStateReader: (read) => {
          readDebugState = read;
        },
      });
      const failingAdapter = yield* makeTestAdapter(failingWrapperPath, {
        registerDebugStateReader: (read) => {
          readFailingDebugState = read;
        },
      });
      const threadId = ThreadId.make("droid-thread-lock-reaped");
      const failedThreadId = ThreadId.make("droid-failed-thread-lock-reaped");

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal((yield* readDebugState(threadId)).threadLockCount, 1);
      yield* adapter.stopSession(threadId);
      assert.equal((yield* readDebugState(threadId)).threadLockCount, 0);

      yield* Effect.flip(
        failingAdapter.startSession({
          threadId: failedThreadId,
          provider: ProviderDriverKind.make("droid"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        }),
      );
      assert.equal((yield* readFailingDebugState(failedThreadId)).threadLockCount, 0);
    }),
  );

  it.effect("closes incomplete streamed and tool items before terminal settlement", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-incomplete-items");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock incomplete items",
        attachments: [],
      });
      const terminal = yield* Deferred.await(turnCompleted);
      const turnEvents = eventsForThread(runtimeEvents, threadId).filter(
        (event) => event.turnId !== undefined && String(event.turnId) === String(sentTurn.turnId),
      );
      const terminalIndex = turnEvents.findIndex((event) => event === terminal);
      const started = turnEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "item.started" }> =>
          event.type === "item.started",
      );
      const completed = turnEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "item.completed" }> =>
          event.type === "item.completed",
      );

      assert.deepEqual(
        started.map((event) => [String(event.itemId), event.payload.itemType]),
        [
          [`reasoning:assistant-${String(sentTurn.turnId)}`, "reasoning"],
          [`msg:assistant-${String(sentTurn.turnId)}`, "assistant_message"],
          [`incomplete-tool-${String(sentTurn.turnId)}`, "command_execution"],
        ],
      );
      assert.deepEqual(
        completed.map((event) => [String(event.itemId), event.payload.itemType]),
        started.map((event) => [String(event.itemId), event.payload.itemType]),
      );
      assert.isTrue(
        completed.every(
          (event) => turnEvents.findIndex((candidate) => candidate === event) < terminalIndex,
        ),
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("keeps tool-use names isolated between concurrent Droid sessions", () =>
    Effect.gen(function* () {
      const firstThreadId = ThreadId.make("droid-shared-tool-first");
      const secondThreadId = ThreadId.make("droid-shared-tool-second");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const firstToolStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.started" }>>();
      const firstTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const secondTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "item.started" &&
              String(event.threadId) === String(firstThreadId) &&
              String(event.itemId) === "shared-tool-use"
              ? Deferred.succeed(firstToolStarted, event).pipe(Effect.asVoid)
              : event.type === "turn.completed" && String(event.threadId) === String(firstThreadId)
                ? Deferred.succeed(firstTurnCompleted, event).pipe(Effect.asVoid)
                : event.type === "turn.completed" &&
                    String(event.threadId) === String(secondThreadId)
                  ? Deferred.succeed(secondTurnCompleted, event).pipe(Effect.asVoid)
                  : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId: firstThreadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.startSession({
        threadId: secondThreadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId: firstThreadId,
        input: "mock delayed shared tool",
        attachments: [],
      });
      yield* Deferred.await(firstToolStarted);

      yield* adapter.sendTurn({
        threadId: secondThreadId,
        input: "mock shared tool execute",
        attachments: [],
      });
      yield* Deferred.await(secondTurnCompleted);

      yield* adapter.sendTurn({
        threadId: firstThreadId,
        input: "mock release shared tool",
        attachments: [],
      });
      yield* Deferred.await(firstTurnCompleted);

      const firstToolCompleted = eventsForThread(runtimeEvents, firstThreadId).find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "item.completed" }> =>
          event.type === "item.completed" && String(event.itemId) === "shared-tool-use",
      );
      assert.equal(firstToolCompleted?.payload.itemType, "dynamic_tool_call");
      assert.equal(firstToolCompleted?.payload.title, "Read");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(firstThreadId);
      yield* adapter.stopSession(secondThreadId);
    }),
  );

  it.effect("round-trips an approved Droid permission and completes the turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-permission-approved");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_REQUEST_PERMISSION: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const requestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "request.opened" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(requestOpened, event).pipe(Effect.asVoid)
              : event.type === "turn.completed" && String(event.threadId) === String(threadId)
                ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "run the approved command",
        attachments: [],
      });

      const opened = yield* Deferred.await(requestOpened);
      assert.equal(String(opened.turnId), String(sentTurn.turnId));
      assert.equal(opened.payload.requestType, "exec_command_approval");
      assert.equal(opened.payload.detail, "echo mock");
      assert.equal(opened.raw?.method, "droid.request_permission");

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(String(opened.requestId)),
        "accept",
      );
      const terminal = yield* Deferred.await(turnCompleted);
      const resolved = eventsForThread(runtimeEvents, threadId).find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
          event.type === "request.resolved" && String(event.requestId) === String(opened.requestId),
      );

      assert.equal(resolved?.payload.requestType, "exec_command_approval");
      assert.equal(resolved?.payload.decision, "accept");
      assert.equal(terminal.payload.state, "completed");
      assert.equal(terminal.payload.stopReason, "completed");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("round-trips a denied Droid permission as a cancelled turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-permission-denied");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_REQUEST_PERMISSION: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const requestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const requestResolved =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.resolved" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (String(event.threadId) !== String(threadId)) return Effect.void;
        if (event.type === "request.opened") {
          return Deferred.succeed(requestOpened, event).pipe(Effect.ignore);
        }
        if (event.type === "request.resolved") {
          return Deferred.succeed(requestResolved, event).pipe(Effect.ignore);
        }
        if (event.type === "turn.completed") {
          return Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
        }
        return Effect.void;
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "deny the command",
        attachments: [],
      });

      const opened = yield* Deferred.await(requestOpened);
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(String(opened.requestId)),
        "decline",
      );
      const resolved = yield* Deferred.await(requestResolved);
      const terminal = yield* Deferred.await(turnCompleted);

      assert.equal(String(resolved.requestId), String(opened.requestId));
      assert.equal(resolved.payload.decision, "decline");
      assert.equal(terminal.payload.state, "cancelled");
      assert.equal(terminal.payload.stopReason, "permission_rejected");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("round-trips Droid ask_user answers and completes the turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-ask-user");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_ASK_USER: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const requested =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "user-input.requested" }>>();
      const resolved =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (String(event.threadId) !== String(threadId)) return Effect.void;
        if (event.type === "user-input.requested") {
          return Deferred.succeed(requested, event).pipe(Effect.ignore);
        }
        if (event.type === "user-input.resolved") {
          return Deferred.succeed(resolved, event).pipe(Effect.ignore);
        }
        if (event.type === "turn.completed") {
          return Deferred.succeed(turnCompleted, event).pipe(Effect.ignore);
        }
        return Effect.void;
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "ask for scope",
        attachments: [],
      });

      const requestedEvent = yield* Deferred.await(requested);
      assert.equal(String(requestedEvent.turnId), String(sentTurn.turnId));
      assert.deepEqual(requestedEvent.payload.questions, [
        {
          id: "1",
          header: "Scope",
          question: "Which scope?",
          options: [
            { label: "workspace", description: "workspace" },
            { label: "session", description: "session" },
          ],
          multiSelect: false,
        },
      ]);
      assert.equal(requestedEvent.raw?.method, "droid.ask_user");

      yield* adapter.respondToUserInput(
        threadId,
        ApprovalRequestId.make(String(requestedEvent.requestId)),
        { "1": "workspace" },
      );
      const resolvedEvent = yield* Deferred.await(resolved);
      const terminal = yield* Deferred.await(turnCompleted);

      assert.equal(String(resolvedEvent.requestId), String(requestedEvent.requestId));
      assert.deepEqual(resolvedEvent.payload.answers, { "1": "workspace" });
      assert.equal(terminal.payload.state, "completed");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("interrupts a hanging Droid turn once and drops its late terminal notification", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-interrupt");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_HANG_TURN: "1" }),
      );
      let readDebugState = (_threadId: ThreadId) =>
        Effect.succeed({ threadLockCount: -1, interruptedTurnCount: -1 });
      const adapter = yield* makeTestAdapter(wrapperPath, {
        registerDebugStateReader: (read) => {
          readDebugState = read;
        },
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const assistantCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.completed" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              String(event.threadId) === String(threadId)
              ? Deferred.succeed(assistantCompleted, event).pipe(Effect.asVoid)
              : event.type === "turn.completed" && String(event.threadId) === String(threadId)
                ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "hang until interrupted",
        attachments: [],
      });
      yield* Deferred.await(assistantCompleted);
      yield* adapter.interruptTurn(threadId, sentTurn.turnId);
      const terminal = yield* Deferred.await(turnCompleted);
      const threadEvents = eventsForThread(runtimeEvents, threadId);

      assert.equal(String(terminal.turnId), String(sentTurn.turnId));
      assert.equal(terminal.payload.state, "cancelled");
      assert.equal(terminal.payload.stopReason, "cancelled");
      assert.equal((yield* readDebugState(threadId)).interruptedTurnCount, 0);
      assert.lengthOf(
        threadEvents.filter(
          (event) =>
            event.type === "turn.completed" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(sentTurn.turnId),
        ),
        1,
      );

      const terminalIndex = threadEvents.findIndex((event) => event === terminal);
      const turnOutputTypes = new Set(["content.delta", "item.started", "item.completed"]);
      assert.deepEqual(
        threadEvents
          .slice(terminalIndex + 1)
          .filter(
            (event) =>
              event.turnId !== undefined &&
              String(event.turnId) === String(sentTurn.turnId) &&
              turnOutputTypes.has(event.type),
          ),
        [],
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("loads a known Droid resume cursor into a ready session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-resume-known");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const sessionStarted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "session.started" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "session.started" && String(event.threadId) === String(threadId)
          ? Deferred.succeed(sessionStarted, event).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "mock-session-known" },
      });
      const started = yield* Deferred.await(sessionStarted);

      assert.equal(session.status, "ready");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-known",
      });
      assert.equal(started.payload.resume, true);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("resets a resumed spec session before sending a normal turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-resume-spec-reset");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_LOAD_IN_SPEC_MODE: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "mock-session-known" },
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock report interaction mode",
        attachments: [],
      });
      yield* Deferred.await(turnCompleted);

      const assistantText = eventsForThread(runtimeEvents, threadId)
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
            event.type === "content.delta" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(sentTurn.turnId) &&
            event.payload.streamKind === "assistant_text",
        )
        .map((event) => event.payload.delta)
        .join("");
      assert.equal(assistantText, "auto");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects an unknown Droid resume cursor with a typed process error", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-resume-unknown");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      const error = yield* Effect.flip(
        adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("droid"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionId: "mock-session-missing" },
        }),
      );

      assert.equal(error._tag, "ProviderAdapterProcessError");
      if (error._tag === "ProviderAdapterProcessError") {
        assert.include(error.detail, "Mock session not found");
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("fails resume when approval-required settings cannot be reasserted", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-resume-settings-failure");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_FAIL_UPDATE_SETTINGS: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const error = yield* Effect.flip(
        adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("droid"),
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          resumeCursor: { schemaVersion: 1, sessionId: "mock-session-known" },
        }),
      );

      assert.equal(error._tag, "ProviderAdapterProcessError");
      if (error._tag === "ProviderAdapterProcessError") {
        assert.include(error.detail, "Mock settings update failure");
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("surfaces Droid initialization failure as a typed process error", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-init-failure");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_FAIL_INIT: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const error = yield* Effect.flip(
        adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("droid"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        }),
      );

      assert.equal(error._tag, "ProviderAdapterProcessError");
      if (error._tag === "ProviderAdapterProcessError") {
        assert.include(error.detail, "Mock initialization failure");
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("fails the active turn and emits session.exited when Droid dies", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-process-death");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_EXIT_MID_TURN: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const sessionExited =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "session.exited" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "session.exited" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(sessionExited, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "exit during this turn",
        attachments: [],
      });
      const exited = yield* Deferred.await(sessionExited);
      const failedTurn = eventsForThread(runtimeEvents, threadId).find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" &&
          event.turnId !== undefined &&
          String(event.turnId) === String(sentTurn.turnId),
      );

      assert.equal(failedTurn?.payload.state, "failed");
      assert.include(failedTurn?.payload.errorMessage ?? "", "Droid exited unexpectedly");
      assert.equal(exited.payload.exitKind, "error");
      assert.isFalse(yield* adapter.hasSession(threadId));

      yield* Fiber.interrupt(runtimeEventsFiber);
    }),
  );

  it.effect("rolls back a turn by forking the Droid session and re-anchoring on the fork", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-rollback");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const firstTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const secondTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "turn.completed" && String(event.threadId) === String(threadId)
          ? Deferred.succeed(firstTurnCompleted, event).pipe(
              Effect.flatMap((wasFirst) =>
                wasFirst ? Effect.void : Deferred.succeed(secondTurnCompleted, event),
              ),
              Effect.asVoid,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "first turn to roll back",
        attachments: [],
      });
      yield* Deferred.await(firstTurnCompleted);

      const snapshot = yield* adapter.rollbackThread(threadId, 1);
      assert.deepEqual(snapshot.turns, []);

      // The live process re-anchored on the fork: the resume cursor points at
      // the rewound session and the session still takes turns.
      const nextTurn = yield* adapter.sendTurn({
        threadId,
        input: "turn after the rewind",
        attachments: [],
      });
      assert.deepStrictEqual(nextTurn.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-rewound",
      });
      yield* Deferred.await(secondTurnCompleted);

      // Rolling back past the turns tracked in this process is refused
      // rather than mis-anchored (rollback of the post-rewind turn is fine,
      // two turns is not).
      const tooDeep = yield* Effect.flip(adapter.rollbackThread(threadId, 2));
      assert.equal(tooDeep._tag, "ProviderAdapterRequestError");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("drops a pre-rewind session straggler after re-anchoring on the fork", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-rewind-straggler");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const firstTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const secondTurnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(firstTurnCompleted, event).pipe(
                  Effect.flatMap((wasFirst) =>
                    wasFirst ? Effect.void : Deferred.succeed(secondTurnCompleted, event),
                  ),
                  Effect.asVoid,
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "turn before straggler rewind",
        attachments: [],
      });
      yield* Deferred.await(firstTurnCompleted);
      yield* adapter.rollbackThread(threadId, 1);

      const nextTurn = yield* adapter.sendTurn({
        threadId,
        input: "turn after straggler rewind",
        attachments: [],
      });
      const terminal = yield* Deferred.await(secondTurnCompleted);
      const nextTurnEvents = eventsForThread(runtimeEvents, threadId).filter(
        (event) => event.turnId !== undefined && String(event.turnId) === String(nextTurn.turnId),
      );

      assert.equal(terminal.payload.state, "completed");
      assert.notInclude(
        nextTurnEvents
          .filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
              event.type === "content.delta",
          )
          .map((event) => event.payload.delta)
          .join(""),
        "stale pre-rewind output",
      );
      assert.lengthOf(
        nextTurnEvents.filter((event) => event.type === "turn.completed"),
        1,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("lets interrupt cancellation win a queued completed-terminal race", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-interrupt-completion-race");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({
          T3_DROID_MOCK_HANG_TURN: "1",
          T3_DROID_MOCK_INTERRUPT_RACE: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const assistantCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.completed" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "item.completed" &&
              event.payload.itemType === "assistant_message" &&
              String(event.threadId) === String(threadId)
              ? Deferred.succeed(assistantCompleted, event).pipe(Effect.asVoid)
              : event.type === "turn.completed" && String(event.threadId) === String(threadId)
                ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "race completion against interrupt",
        attachments: [],
      });
      yield* Deferred.await(assistantCompleted);
      yield* adapter.interruptTurn(threadId, sentTurn.turnId);
      const terminal = yield* Deferred.await(turnCompleted);

      assert.equal(terminal.payload.state, "cancelled");
      assert.lengthOf(
        eventsForThread(runtimeEvents, threadId).filter(
          (event) =>
            event.type === "turn.completed" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(sentTurn.turnId),
        ),
        1,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("adopts a spec-handoff successor after streaming it into the plan turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-spec-handoff");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock spec handoff",
        attachments: [],
        interactionMode: "plan",
      });
      const terminal = yield* Deferred.await(turnCompleted);
      const sessions = yield* adapter.listSessions();
      const successorText = eventsForThread(runtimeEvents, threadId)
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
            event.type === "content.delta" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(sentTurn.turnId) &&
            event.payload.streamKind === "assistant_text",
        )
        .map((event) => event.payload.delta)
        .join("");

      assert.include(successorText, "implementation successor");
      assert.equal(terminal.payload.state, "completed");
      assert.equal(terminal.payload.stopReason, "spec_handoff");
      assert.deepStrictEqual(sessions[0]?.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-spec-successor",
      });

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("treats compaction as a no-op and reports the last-call context meter", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-compaction");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock compaction",
        attachments: [],
      });
      const terminal = yield* Deferred.await(turnCompleted);
      const threadEvents = eventsForThread(runtimeEvents, threadId);
      const compactedUsage = threadEvents
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "thread.token-usage.updated" }> =>
            event.type === "thread.token-usage.updated",
        )
        .find((event) => event.payload.usage.lastUsedTokens === 8);

      assert.equal(terminal.payload.state, "completed");
      assert.lengthOf(
        threadEvents.filter(
          (event) =>
            event.type === "turn.completed" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(sentTurn.turnId),
        ),
        1,
      );
      assert.deepInclude(compactedUsage?.payload.usage, {
        usedTokens: 8,
        totalProcessedTokens: 33,
        lastUsedTokens: 8,
        lastInputTokens: 5,
        lastCachedInputTokens: 1,
        lastOutputTokens: 2,
        compactsAutomatically: true,
      });

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("maps child sessions to tasks without leaking child deltas into the main turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-child-session");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const sentTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock child session",
        attachments: [],
      });
      yield* Deferred.await(turnCompleted);
      const threadEvents = eventsForThread(runtimeEvents, threadId);

      assert.lengthOf(
        threadEvents.filter((event) => event.type === "task.started"),
        1,
      );
      assert.lengthOf(
        threadEvents.filter((event) => event.type === "task.completed"),
        1,
      );
      const progress = threadEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "tool.progress" }> =>
          event.type === "tool.progress",
      );
      assert.lengthOf(progress, 1);
      assert.equal(String(progress[0]?.payload.taskId), "mock-session-child");
      assert.equal(progress[0]?.payload.toolUseId, `child-task-${String(sentTurn.turnId)}`);
      assert.equal(progress[0]?.payload.toolName, "Task");
      assert.equal(progress[0]?.payload.summary, "Inspecting delegated files");
      assert.notInclude(
        threadEvents
          .filter(
            (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
              event.type === "content.delta",
          )
          .map((event) => event.payload.delta)
          .join(""),
        "child-only output",
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("drops Droid tool progress without an owning subagent session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-taskless-tool-progress");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "mock taskless progress",
        attachments: [],
      });
      yield* Deferred.await(turnCompleted);

      assert.lengthOf(
        eventsForThread(runtimeEvents, threadId).filter((event) => event.type === "tool.progress"),
        0,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("refuses to guess rollback anchors from resumed steering messages", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-resumed-rollback");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_LOAD_STEERING_MESSAGES: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "mock-session-known" },
      });
      const error = yield* Effect.flip(adapter.rollbackThread(threadId, 1));

      assert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag === "ProviderAdapterRequestError") {
        assert.include(error.detail, "only 0 tracked in this session");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("settles a coalesced steering turn exactly once", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-steering-coalesced");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const steeringReady =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.started" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "item.started" &&
              event.payload.itemType === "command_execution" &&
              String(event.threadId) === String(threadId)
              ? Deferred.succeed(steeringReady, event).pipe(Effect.asVoid)
              : event.type === "turn.completed" && String(event.threadId) === String(threadId)
                ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const openingTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock steering original",
        attachments: [],
      });
      yield* Deferred.await(steeringReady);
      const steeredTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock steering coalesced",
        attachments: [],
      });
      const terminal = yield* Deferred.await(turnCompleted);

      assert.equal(String(steeredTurn.turnId), String(openingTurn.turnId));
      assert.equal(terminal.payload.state, "completed");
      assert.lengthOf(
        eventsForThread(runtimeEvents, threadId).filter(
          (event) =>
            event.type === "turn.completed" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(openingTurn.turnId),
        ),
        1,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("keeps a steered turn open when the queued message runs separately", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-steering-separate");
      const wrapperPath = yield* Effect.promise(() => makeMockDroidWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const steeringReady =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.started" }>>();
      const separateAssistantCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "item.completed" }>>();
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "item.started" &&
              event.payload.itemType === "command_execution" &&
              String(event.threadId) === String(threadId)
              ? Deferred.succeed(steeringReady, event).pipe(Effect.asVoid)
              : event.type === "item.completed" &&
                  event.payload.itemType === "assistant_message" &&
                  String(event.threadId) === String(threadId)
                ? Deferred.succeed(separateAssistantCompleted, event).pipe(Effect.asVoid)
                : event.type === "turn.completed" && String(event.threadId) === String(threadId)
                  ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
                  : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const openingTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock steering original",
        attachments: [],
      });
      yield* Deferred.await(steeringReady);
      const steeredTurn = yield* adapter.sendTurn({
        threadId,
        input: "mock steering separate",
        attachments: [],
      });
      yield* Deferred.await(separateAssistantCompleted);
      const terminal = yield* Deferred.await(turnCompleted);

      assert.equal(String(steeredTurn.turnId), String(openingTurn.turnId));
      assert.equal(terminal.payload.state, "completed");
      assert.lengthOf(
        eventsForThread(runtimeEvents, threadId).filter(
          (event) =>
            event.type === "turn.completed" &&
            event.turnId !== undefined &&
            String(event.turnId) === String(openingTurn.turnId),
        ),
        1,
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("ignores unknown Droid notifications and still completes the turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("droid-unknown-notification");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockDroidWrapper({ T3_DROID_MOCK_EMIT_UNKNOWN_NOTIFICATION: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "turn.completed" }>>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnCompleted, event).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("droid"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "tolerate future notifications",
        attachments: [],
      });
      const terminal = yield* Deferred.await(turnCompleted);
      const assistantText = eventsForThread(runtimeEvents, threadId)
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
            event.type === "content.delta" && event.payload.streamKind === "assistant_text",
        )
        .map((event) => event.payload.delta)
        .join("");

      assert.equal(assistantText, "hello from droid mock");
      assert.equal(terminal.payload.state, "completed");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );
});

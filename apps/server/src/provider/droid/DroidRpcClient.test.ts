// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  DroidRpcError,
  makeDroidRpcClient,
  parseJsonRpcLine,
  splitNdjsonChunks,
} from "./DroidRpcClient.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/droid-mock-agent.ts");

const within = <A>(effect: Effect.Effect<A>, message: string | (() => string)) =>
  effect.pipe(
    Effect.timeoutOption("5 seconds"),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.die(new Error(typeof message === "string" ? message : message())),
        onSome: Effect.succeed,
      }),
    ),
  );

describe("DroidRpcClient helpers", () => {
  it("splits chunks across arbitrary boundaries and preserves a partial tail", () => {
    const splitter = splitNdjsonChunks();

    assert.deepStrictEqual(splitter.push('{"a":1}\n{"b"'), ['{"a":1}']);
    assert.deepStrictEqual(splitter.push(':2}\n{"c":3'), ['{"b":2}']);
    assert.deepStrictEqual(splitter.end(), ['{"c":3']);
  });

  it("handles CRLF and skips blank lines", () => {
    const splitter = splitNdjsonChunks();

    assert.deepStrictEqual(splitter.push('{"a":1}\r\n\r\n{"b":2}\r\n'), ['{"a":1}', '{"b":2}']);
    assert.deepStrictEqual(splitter.end(), []);
  });

  it("parses valid JSON-RPC and reports invalid lines without throwing", () => {
    const valid = parseJsonRpcLine(
      '{"jsonrpc":"2.0","type":"response","id":"1","result":{"ok":true}}',
    );
    const invalid = parseJsonRpcLine("not json");

    assert.equal(valid._tag, "Message");
    assert.equal(invalid._tag, "Invalid");
  });
});

it.effect("correlates RPCs, decodes notifications, handles server requests, and detects exit", () =>
  Effect.gen(function* () {
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: [mockAgentPath],
      env: {
        T3_DROID_MOCK_EMIT_TOOL_CALL: "1",
        T3_DROID_MOCK_REQUEST_PERMISSION: "1",
        T3_DROID_MOCK_ASK_USER: "1",
        T3_DROID_MOCK_EMIT_UNKNOWN_NOTIFICATION: "1",
      },
    });

    const notifications: Array<{ readonly type: string }> = [];
    const notificationSessionIds: Array<string | undefined> = [];
    const diagnostics: string[] = [];
    const turnCompleted = yield* Deferred.make<void>();
    const notificationFiber = yield* Stream.runForEach(client.notifications, (envelope) =>
      Effect.sync(() => {
        notifications.push(envelope.notification);
        notificationSessionIds.push(envelope.sessionId);
      }).pipe(
        Effect.andThen(
          envelope.notification.type === "agent_turn_completed"
            ? Deferred.succeed(turnCompleted, undefined).pipe(Effect.ignore)
            : Effect.void,
        ),
      ),
    ).pipe(Effect.forkChild({ startImmediately: true }));
    const diagnosticFiber = yield* Stream.runForEach(client.diagnostics, (diagnostic) =>
      Effect.sync(() => {
        diagnostics.push(diagnostic.message);
      }),
    ).pipe(Effect.forkChild({ startImmediately: true }));

    const initialized = (yield* client.request(
      "droid.initialize_session",
      {
        machineId: "t3-test",
        cwd: process.cwd(),
      },
      { timeoutMs: 5_000 },
    )) as { readonly sessionId?: unknown };
    assert.equal(initialized.sessionId, "mock-session-1");

    const [modelResult, commandResult, skillResult] = yield* Effect.all(
      [
        client.request("droid.list_models", {}),
        client.request("droid.list_commands", {}),
        client.request("droid.list_skills", {}),
      ],
      { concurrency: "unbounded" },
    );
    assert.lengthOf((modelResult as { models: unknown[] }).models, 2);
    assert.lengthOf((commandResult as { commands: unknown[] }).commands, 1);
    assert.lengthOf((skillResult as { skills: unknown[] }).skills, 1);

    yield* client.request(
      "droid.add_user_message",
      {
        messageId: "turn-1",
        text: "hello",
      },
      { timeoutMs: undefined },
    );

    const permission = yield* within(
      Stream.runHead(client.serverRequests),
      () =>
        `permission request did not arrive; notifications=${notifications.map((notification) => notification.type).join(",")}; diagnostics=${diagnostics.join(" | ")}`,
    );
    assert.isTrue(Option.isSome(permission));
    if (Option.isNone(permission)) {
      return;
    }
    assert.equal(permission.value.method, "droid.request_permission");
    assert.equal(permission.value.sessionId, undefined);
    if (permission.value.method === "droid.request_permission") {
      assert.equal(permission.value.params.options[0]?.outcome, "proceed_once");
      assert.deepStrictEqual(
        (permission.value.params.raw as { toolUses?: unknown }).toolUses,
        permission.value.params.toolUses.map((toolUse) => ({
          ...toolUse,
          details: toolUse.details,
        })),
      );
    }
    yield* permission.value.respond({ selectedOption: "proceed_once" });

    const ask = yield* within(
      Stream.runHead(client.serverRequests),
      () =>
        `ask-user request did not arrive; notifications=${notifications.map((notification) => notification.type).join(",")}; diagnostics=${diagnostics.join(" | ")}`,
    );
    assert.isTrue(Option.isSome(ask));
    if (Option.isNone(ask)) {
      return;
    }
    assert.equal(ask.value.method, "droid.ask_user");
    assert.equal(ask.value.sessionId, undefined);
    yield* ask.value.respond({
      answers: [{ index: 1, question: "Which scope?", answer: "workspace" }],
    });

    yield* within(Deferred.await(turnCompleted), "turn notification did not complete");

    assert.includeMembers(
      notifications.map((notification) => notification.type),
      [
        "thinking_text_delta",
        "tool_call",
        "tool_result",
        "assistant_text_delta",
        "agent_turn_completed",
        "__unknown__",
      ],
    );
    assert.isTrue(notificationSessionIds.every((sessionId) => sessionId === "mock-session-1"));

    yield* within(client.shutdown, "client shutdown did not complete");
    const exit = yield* within(client.exits, "process exit was not detected");
    assert.equal(exit.code, 0);

    yield* Fiber.interrupt(notificationFiber);
    yield* Fiber.interrupt(diagnosticFiber);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("preserves optional notification and server-request session envelopes", () =>
  Effect.gen(function* () {
    const script = `
      const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
      write({
        jsonrpc: "2.0",
        type: "notification",
        method: "droid.session_notification",
        params: {
          notification: {
            type: "assistant_text_delta",
            messageId: "bare",
            blockIndex: 0,
            textDelta: "bare"
          }
        }
      });
      write({
        jsonrpc: "2.0",
        type: "request",
        id: "permission-with-session",
        method: "droid.request_permission",
        params: {
          sessionId: "permission-session",
          toolUses: [{
            toolUse: {
              type: "tool_use",
              id: "tool-1",
              input: { command: "echo hi" },
              name: "Execute"
            },
            confirmationType: "exec",
            details: {
              type: "exec",
              fullCommand: "echo hi",
              command: "echo"
            }
          }],
          options: [{ label: "Allow once", value: "proceed_once" }]
        }
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const notification = yield* within(
      Stream.runHead(client.notifications),
      "bare notification did not arrive",
    );
    assert.isTrue(Option.isSome(notification));
    if (Option.isSome(notification)) {
      assert.equal(notification.value.sessionId, undefined);
      assert.equal(notification.value.notification.type, "assistant_text_delta");
    }

    const request = yield* within(
      Stream.runHead(client.serverRequests),
      "server request did not arrive",
    );
    assert.isTrue(Option.isSome(request));
    if (Option.isSome(request)) {
      assert.equal(request.value.sessionId, "permission-session");
      assert.equal(request.value.method, "droid.request_permission");
      yield* request.value.respond({ selectedOption: "proceed_once" });
    }

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("fails registration after exit and ends every public stream", () =>
  Effect.gen(function* () {
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", "process.exit(7)"],
    });

    const exit = yield* within(client.exits, "process exit was not detected");
    assert.equal(exit.code, 7);

    const requestResult = yield* Effect.result(
      client.request("droid.list_models", {}, { timeoutMs: undefined }),
    );
    assert.equal(requestResult._tag, "Failure");
    if (requestResult._tag === "Failure") {
      assert.instanceOf(requestResult.failure, DroidRpcError);
      assert.equal(requestResult.failure.kind, "process-exit");
      assert.deepStrictEqual(requestResult.failure.data, exit);
    }

    const [notifications, serverRequests, diagnostics] = yield* within(
      Effect.all([
        Stream.runCollect(client.notifications),
        Stream.runCollect(client.serverRequests),
        Stream.runCollect(client.diagnostics),
      ]),
      "public streams did not end after process exit",
    );
    assert.isEmpty(notifications);
    assert.isEmpty(serverRequests);
    assert.isEmpty(diagnostics);
    yield* within(client.shutdown, "shutdown after exit did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("diagnoses a response that arrives after its request timed out", () =>
  Effect.gen(function* () {
    const script = `
      let pending = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (chunk) => {
        pending += chunk;
        const line = pending.split("\\n")[0];
        if (!line) return;
        const request = JSON.parse(line);
        setTimeout(() => {
          process.stdout.write(JSON.stringify({
            jsonrpc: "2.0",
            type: "response",
            id: request.id,
            result: { late: true }
          }) + "\\n");
        }, 40);
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const requestResult = yield* Effect.result(
      client.request("droid.list_models", {}, { timeoutMs: 10 }),
    );
    assert.equal(requestResult._tag, "Failure");
    if (requestResult._tag === "Failure") {
      assert.equal(requestResult.failure.kind, "timeout");
    }

    const diagnostic = yield* within(
      Stream.runHead(client.diagnostics),
      "late-response diagnostic did not arrive",
    );
    assert.isTrue(Option.isSome(diagnostic));
    if (Option.isSome(diagnostic)) {
      assert.instanceOf(diagnostic.value.cause, DroidRpcError);
      if (diagnostic.value.cause instanceof DroidRpcError) {
        assert.equal(diagnostic.value.cause.kind, "duplicate-response");
        assert.equal(diagnostic.value.cause.requestId, "1");
      }
    }

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("keeps only the latest 256 undrained diagnostics", () =>
  Effect.gen(function* () {
    const script = `
      for (let index = 0; index < 260; index += 1) {
        process.stdout.write("invalid-" + index + "\\n");
      }
      process.stdout.write(JSON.stringify({
        jsonrpc: "2.0",
        type: "notification",
        method: "droid.session_notification",
        params: {
          notification: {
            type: "diagnostic_sentinel"
          }
        }
      }) + "\\n");
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    yield* within(Stream.runHead(client.notifications), "sentinel notification did not arrive");
    yield* within(client.shutdown, "client shutdown did not complete");
    const diagnostics = yield* within(
      Stream.runCollect(client.diagnostics),
      "diagnostic stream did not end",
    );

    assert.lengthOf(diagnostics, 256);
    assert.equal(diagnostics[0]?.line, "invalid-4");
    assert.equal(diagnostics[255]?.line, "invalid-259");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

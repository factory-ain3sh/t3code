// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { DroidRpcError, DroidRpcSpawnError, makeDroidRpcClient } from "./DroidRpcClient.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/droid-mock-agent.ts");

interface CapturedLog {
  readonly message: ReadonlyArray<unknown>;
}

const withCapturedLogs = <A, E, R>(logs: CapturedLog[], effect: Effect.Effect<A, E, R>) => {
  const logger = Logger.make(({ message }) => {
    logs.push({
      message: Array.isArray(message) ? message : [message],
    });
  });
  return effect.pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
};

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
  it("derives transport messages from structured error attributes", () => {
    const spawnError = new DroidRpcSpawnError({
      command: "droid",
      cause: new Error("ENOENT"),
    });
    const timeoutError = new DroidRpcError({
      kind: "timeout",
      method: "droid.list_models",
      requestId: "7",
      timeoutMs: 25,
    });

    assert.equal(spawnError.message, "Failed to spawn Droid process for command: droid");
    assert.equal(timeoutError.message, "Droid request droid.list_models timed out after 25ms");
  });
});

it.effect("handles a final response without a trailing newline", () =>
  Effect.gen(function* () {
    const script = `
      let pending = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (chunk) => {
        pending += chunk;
        const line = pending.split("\\n")[0];
        if (!line) return;
        const request = JSON.parse(line);
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          type: "response",
          id: request.id,
          result: { complete: true }
        }), () => process.exit(0));
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 5_000 });
    assert.deepStrictEqual(result, { complete: true });

    const exit = yield* within(client.exits, "process exit was not detected");
    assert.equal(exit.code, 0);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("handles responses delimited by a bare carriage return", () =>
  Effect.gen(function* () {
    const script = `
      let pending = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (chunk) => {
        pending += chunk;
        const line = pending.split("\\n")[0];
        if (!line) return;
        const request = JSON.parse(line);
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          type: "response",
          id: request.id,
          result: { complete: true }
        }) + "\\r" + JSON.stringify({
          jsonrpc: "2.0",
          type: "notification",
          method: "droid.session_notification",
          params: {
            notification: {
              type: "diagnostic_sentinel"
            }
          }
        }) + "\\n");
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
    assert.deepStrictEqual(result, { complete: true });

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("rejects JSON-RPC envelopes without a supported type discriminator", () => {
  const logs: CapturedLog[] = [];
  return withCapturedLogs(
    logs,
    Effect.gen(function* () {
      const script = `
        process.stdin.setEncoding("utf8");
        process.stdin.once("data", (chunk) => {
          const request = JSON.parse(chunk.split("\\n")[0]);
          const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
          const response = {
            jsonrpc: "2.0",
            id: request.id,
            result: { accepted: false }
          };
          write(response);
          write({ ...response, type: "event" });
          write({ ...response, type: "response", result: { accepted: true } });
        });
        process.stdin.resume();
      `;
      const client = yield* makeDroidRpcClient({
        command: process.execPath,
        args: ["-e", script],
      });

      const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
      assert.deepStrictEqual(result, { accepted: true });
      assert.deepStrictEqual(
        logs
          .map((log) => log.message[0])
          .filter((message) => String(message).includes("valid type discriminator")),
        [
          "Unable to parse Droid JSON-RPC line: JSON-RPC line must include a valid type discriminator",
          "Unable to parse Droid JSON-RPC line: JSON-RPC line must include a valid type discriminator",
        ],
      );

      yield* within(client.shutdown, "client shutdown did not complete");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
  );
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
        `permission request did not arrive; notifications=${notifications.map((notification) => notification.type).join(",")}`,
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
        `ask-user request did not arrive; notifications=${notifications.map((notification) => notification.type).join(",")}`,
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

    const [notifications, serverRequests] = yield* within(
      Effect.all([
        Stream.runCollect(client.notifications),
        Stream.runCollect(client.serverRequests),
      ]),
      "public streams did not end after process exit",
    );
    assert.isEmpty(notifications);
    assert.isEmpty(serverRequests);
    yield* within(client.shutdown, "shutdown after exit did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("logs a response that arrives after its request timed out", () => {
  const logs: CapturedLog[] = [];
  return withCapturedLogs(
    logs,
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
          }) + "\\n", () => process.exit(0));
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

      yield* within(client.exits, "late response was not processed before exit");
      assert.include(
        logs.map((log) => String(log.message[0])),
        "Droid request droid.list_models responded after timing out",
      );

      yield* within(client.shutdown, "client shutdown did not complete");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
  );
});

it.effect("bounds timed-out request retention while still logging a recent late response", () => {
  const logs: CapturedLog[] = [];
  return withCapturedLogs(
    logs,
    Effect.gen(function* () {
      const requestCount = 257;
      const script = `
      const requests = [];
      let pending = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (chunk) => {
        pending += chunk;
        const lines = pending.split("\\n");
        pending = lines.pop() ?? "";
        for (const line of lines) {
          if (line) requests.push(JSON.parse(line));
        }
        if (requests.length !== ${requestCount}) return;
        setTimeout(() => {
          const output = [requests[0], requests[requests.length - 1]].map((request) =>
            JSON.stringify({
              jsonrpc: "2.0",
              type: "response",
              id: request.id,
              result: { late: true }
            })
          ).join("\\n") + "\\n";
          process.stdout.write(output, () => process.exit(0));
        }, 250);
      });
      process.stdin.resume();
    `;
      const client = yield* makeDroidRpcClient({
        command: process.execPath,
        args: ["-e", script],
      });

      const results = yield* Effect.all(
        Array.from({ length: requestCount }, () =>
          Effect.result(client.request("droid.list_models", {}, { timeoutMs: 100 })),
        ),
        { concurrency: "unbounded" },
      );
      assert.isTrue(
        results.every((result) => result._tag === "Failure" && result.failure.kind === "timeout"),
      );

      yield* within(client.exits, "late responses were not processed before exit");
      assert.includeMembers(
        logs.map((log) => String(log.message[0])),
        [
          "Ignoring response for unknown Droid request 1",
          `Droid request droid.list_models responded after timing out`,
        ],
      );

      yield* within(client.shutdown, "client shutdown did not complete");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
  );
});

it.effect("logs bounded stdout and stderr diagnostics", () => {
  const logs: CapturedLog[] = [];
  return withCapturedLogs(
    logs,
    Effect.gen(function* () {
      const script = `
      process.stdout.write("x".repeat(2500) + "\\n");
      process.stderr.write("y".repeat(2500));
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
      yield* within(client.exits, "process exit was not detected");

      const parseLog = logs.find((log) =>
        String(log.message[0]).startsWith("Unable to parse Droid JSON-RPC line"),
      );
      const parseDetails = parseLog?.message[1];
      assert.equal(
        String(
          typeof parseDetails === "object" && parseDetails !== null && "line" in parseDetails
            ? parseDetails.line
            : undefined,
        ).length,
        2000,
      );
      const stderrLog = logs.find((log) => String(log.message[0]).startsWith("Droid stderr:"));
      assert.equal(String(stderrLog?.message[0]).length, 2000);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
  );
});

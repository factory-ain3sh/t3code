// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
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

function isProcessAlive(processId: number): boolean {
  try {
    process.kill(processId, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ESRCH") {
      return false;
    }
    throw error;
  }
}

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

it.effect("rejects JSON-RPC envelopes with invalid id shapes", () => {
  const logs: CapturedLog[] = [];
  return withCapturedLogs(
    logs,
    Effect.gen(function* () {
      const script = `
        process.stdin.setEncoding("utf8");
        process.stdin.once("data", (chunk) => {
          const request = JSON.parse(chunk.split("\\n")[0]);
          const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
          for (const id of [true, { nested: true }, ["array"]]) {
            write({
              jsonrpc: "2.0",
              type: "response",
              id,
              result: { accepted: false }
            });
          }
          write({
            jsonrpc: "2.0",
            type: "response",
            id: request.id,
            result: { accepted: true }
          });
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
          .filter((message) => String(message).includes("JSON-RPC id must")),
        Array.from(
          { length: 3 },
          () =>
            "Unable to parse Droid JSON-RPC line: JSON-RPC id must be a string, number, null, or absent",
        ),
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
      // The parsed view is projected down to the fields the adapter summarises; the
      // RPC envelope retains droid's original request for approval-event diagnostics.
      const rawToolUse = (
        permission.value.rawParams as {
          toolUses: readonly {
            toolUse: { type?: unknown };
            details: { impactLevel?: unknown; riskLevelReason?: unknown };
          }[];
        }
      ).toolUses[0];
      assert.equal(rawToolUse?.toolUse.type, "tool_use");
      assert.equal(rawToolUse?.details.impactLevel, "low");
      assert.equal(rawToolUse?.details.riskLevelReason, "The mock command only prints text.");
      assert.deepStrictEqual(
        Object.keys(permission.value.params.toolUses[0]?.toolUse ?? {}).sort(),
        ["id", "input", "name"],
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

it.effect("echoes numeric server-request ids without coercing them to strings", () =>
  Effect.gen(function* () {
    const script = `
      const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
      write({
        jsonrpc: "2.0",
        type: "request",
        id: 42,
        method: "droid.ask_user",
        params: {
          toolCallId: "ask-numeric-id",
          questions: [{
            index: 0,
            topic: "Scope",
            question: "Which scope?",
            options: ["workspace"]
          }]
        }
      });
      process.stdin.setEncoding("utf8");
      process.stdin.once("data", (chunk) => {
        const response = JSON.parse(chunk.split("\\n")[0]);
        write({
          jsonrpc: "2.0",
          type: "notification",
          method: "droid.session_notification",
          params: {
            notification: {
              type: "assistant_text_delta",
              messageId: "numeric-id",
              textDelta: typeof response.id + ":" + response.id
            }
          }
        });
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const request = yield* within(
      Stream.runHead(client.serverRequests),
      "numeric-id server request did not arrive",
    );
    assert.isTrue(Option.isSome(request));
    if (Option.isSome(request)) {
      assert.equal(request.value.id, 42);
      yield* request.value.respond({
        answers: [{ index: 0, question: "Which scope?", answer: "workspace" }],
      });
    }

    const notification = yield* within(
      Stream.runHead(client.notifications),
      "numeric-id response was not observed",
    );
    assert.isTrue(Option.isSome(notification));
    if (
      Option.isSome(notification) &&
      notification.value.notification.type === "assistant_text_delta"
    ) {
      assert.equal(notification.value.notification.textDelta, "number:42");
    }

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("does not correlate numeric responses with string request ids", () =>
  Effect.gen(function* () {
    const script = `
      process.stdin.setEncoding("utf8");
      process.stdin.once("data", (chunk) => {
        const request = JSON.parse(chunk.split("\\n")[0]);
        process.stdout.write(JSON.stringify({
          jsonrpc: "2.0",
          type: "response",
          id: Number(request.id),
          result: { correlated: "numeric" }
        }) + "\\n");
        setTimeout(() => {
          process.stdout.write(JSON.stringify({
            jsonrpc: "2.0",
            type: "response",
            id: request.id,
            result: { correlated: "string" }
          }) + "\\n");
        }, 10);
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
    assert.deepStrictEqual(result, { correlated: "string" });

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("allows an error fallback after a server response fails to encode", () =>
  Effect.gen(function* () {
    const script = `
      const write = (message) => process.stdout.write(JSON.stringify(message) + "\\n");
      write({
        jsonrpc: "2.0",
        type: "request",
        id: "retry-response",
        method: "droid.ask_user",
        params: {
          toolCallId: "ask-retry",
          questions: [{
            index: 0,
            topic: "Scope",
            question: "Which scope?",
            options: ["workspace"]
          }]
        }
      });
      process.stdin.setEncoding("utf8");
      process.stdin.once("data", (chunk) => {
        const response = JSON.parse(chunk.split("\\n")[0]);
        write({
          jsonrpc: "2.0",
          type: "notification",
          method: "droid.session_notification",
          params: {
            notification: {
              type: "assistant_text_delta",
              messageId: "retry-response",
              textDelta: response.error.code + ":" + response.error.message
            }
          }
        });
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const request = yield* within(
      Stream.runHead(client.serverRequests),
      "retryable server request did not arrive",
    );
    assert.isTrue(Option.isSome(request));
    if (Option.isSome(request)) {
      const cyclic: { self?: unknown } = {};
      cyclic.self = cyclic;
      const failedResponse = yield* Effect.result(request.value.respond(cyclic));
      assert.equal(failedResponse._tag, "Failure");
      if (failedResponse._tag === "Failure") {
        assert.equal(failedResponse.failure.kind, "encode");
      }
      yield* request.value.fail(-32603, "response encoding failed");
    }

    const notification = yield* within(
      Stream.runHead(client.notifications),
      "fallback server response was not observed",
    );
    assert.isTrue(Option.isSome(notification));
    if (
      Option.isSome(notification) &&
      notification.value.notification.type === "assistant_text_delta"
    ) {
      assert.equal(notification.value.notification.textDelta, "-32603:response encoding failed");
    }

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("preserves terminal notifications when tool progress delivery is saturated", () =>
  Effect.gen(function* () {
    const script = `
      process.stdin.setEncoding("utf8");
      process.stdin.once("data", (chunk) => {
        const request = JSON.parse(chunk.split("\\n")[0]);
        const notifications = Array.from({ length: 128 }, (_, index) => ({
          jsonrpc: "2.0",
          type: "notification",
          method: "droid.session_notification",
          params: {
            notification: {
              type: "tool_progress_update",
              toolUseId: "queued-" + index,
              toolName: "Task",
              update: {
                type: "status",
                status: "running",
                text: String(index)
              }
            }
          }
        }));
        notifications.push({
          jsonrpc: "2.0",
          type: "notification",
          method: "droid.session_notification",
          params: {
            notification: {
              type: "agent_turn_completed",
              reason: "completed",
              turnId: "turn-saturated",
              tokenUsage: {
                inputTokens: 1,
                outputTokens: 2,
                cacheCreationTokens: 0,
                cacheReadTokens: 0,
                thinkingTokens: 0
              }
            }
          }
        });
        const response = {
          jsonrpc: "2.0",
          type: "response",
          id: request.id,
          result: { queueSaturated: true }
        };
        process.stdout.write(
          [...notifications, response].map((message) => JSON.stringify(message)).join("\\n") + "\\n"
        );
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
    assert.deepStrictEqual(result, { queueSaturated: true });

    const notifications = yield* within(
      Stream.runCollect(client.notifications.pipe(Stream.take(64))),
      "reserved terminal notification was not delivered",
    );
    assert.equal(notifications.length, 64);
    assert.equal(notifications[63]?.notification.type, "agent_turn_completed");

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("resolves responses queued behind a lossless notification burst", () =>
  Effect.gen(function* () {
    const script = `
      process.stdin.setEncoding("utf8");
      let input = "";
      let requestCount = 0;
      process.stdin.on("data", (chunk) => {
        input += chunk;
        const lines = input.split("\\n");
        input = lines.pop();
        for (const line of lines) {
          if (!line) continue;
          const request = JSON.parse(line);
          requestCount += 1;
          if (requestCount === 1) {
            const notifications = Array.from({ length: 65 }, (_, index) => ({
              jsonrpc: "2.0",
              type: "notification",
              method: "droid.session_notification",
              params: {
                notification: {
                  type: "assistant_text_delta",
                  messageId: "message-" + index,
                  textDelta: String(index)
                }
              }
            }));
            const response = {
              jsonrpc: "2.0",
              type: "response",
              id: request.id,
              result: { queueSaturated: true }
            };
            process.stdout.write(
              [...notifications, response].map((message) => JSON.stringify(message)).join("\\n") + "\\n"
            );
            continue;
          }
          process.stdout.write(JSON.stringify({
            jsonrpc: "2.0",
            type: "response",
            id: request.id,
            result: { transportAlive: true }
          }) + "\\n");
        }
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    // The response rides behind 65 lossless deltas; delivery must not depend
    // on the notification consumer making progress.
    const saturated = yield* client.request("droid.list_models", {}, { timeoutMs: 5_000 });
    assert.deepStrictEqual(saturated, { queueSaturated: true });

    const notifications = yield* within(
      Stream.runCollect(client.notifications.pipe(Stream.take(65))),
      "burst text deltas were not delivered",
    );
    assert.equal(notifications.length, 65);
    assert.isTrue(
      notifications.every(
        (notification) => notification.notification.type === "assistant_text_delta",
      ),
    );

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
    assert.deepStrictEqual(result, { transportAlive: true });

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("resolves responses queued behind a server-request burst", () =>
  Effect.gen(function* () {
    const script = `
      process.stdin.setEncoding("utf8");
      let input = "";
      let requestCount = 0;
      process.stdin.on("data", (chunk) => {
        input += chunk;
        const lines = input.split("\\n");
        input = lines.pop();
        for (const line of lines) {
          if (!line) continue;
          const request = JSON.parse(line);
          requestCount += 1;
          if (requestCount === 1) {
            const serverRequests = Array.from({ length: 17 }, (_, index) => ({
              jsonrpc: "2.0",
              type: "request",
              id: "queued-request-" + index,
              method: "droid.ask_user",
              params: {
                toolCallId: "ask-" + index,
                questions: [{
                  index: 0,
                  topic: "Scope",
                  question: "Which scope?",
                  options: ["workspace"]
                }]
              }
            }));
            const response = {
              jsonrpc: "2.0",
              type: "response",
              id: request.id,
              result: { queueSaturated: true }
            };
            process.stdout.write(
              [...serverRequests, response].map((message) => JSON.stringify(message)).join("\\n") + "\\n"
            );
            continue;
          }
          process.stdout.write(JSON.stringify({
            jsonrpc: "2.0",
            type: "response",
            id: request.id,
            result: { transportAlive: true }
          }) + "\\n");
        }
      });
      process.stdin.resume();
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    // The response rides behind 17 pending server requests; delivery must not
    // depend on the server-request consumer making progress.
    const saturated = yield* client.request("droid.list_models", {}, { timeoutMs: 5_000 });
    assert.deepStrictEqual(saturated, { queueSaturated: true });

    const serverRequests = yield* within(
      Stream.runCollect(client.serverRequests.pipe(Stream.take(17))),
      "burst server requests were not delivered",
    );
    assert.equal(serverRequests.length, 17);
    assert.isTrue(serverRequests.every((request) => request.method === "droid.ask_user"));

    const result = yield* client.request("droid.list_models", {}, { timeoutMs: 500 });
    assert.deepStrictEqual(result, { transportAlive: true });

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

it.effect("terminates the transport when stdout closes before the process exits", () =>
  Effect.gen(function* () {
    const markerDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "droid-rpc-exit-"));
    const pidPath = NodePath.join(markerDir, "pid");
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: [
        "-e",
        'require("node:fs").writeFileSync(process.argv[1], String(process.pid)); process.stdout.end(); setInterval(() => {}, 1_000)',
        pidPath,
      ],
    });

    const exit = yield* within(client.exits, "stdout closure did not terminate the transport");
    assert.equal(exit.code, null);
    assert.equal(exit.description, "Droid stdout stream closed before the process exited");

    const requestResult = yield* Effect.result(
      client.request("droid.list_models", {}, { timeoutMs: undefined }),
    );
    assert.equal(requestResult._tag, "Failure");
    if (requestResult._tag === "Failure") {
      assert.equal(requestResult.failure.kind, "process-exit");
      assert.deepStrictEqual(requestResult.failure.data, exit);
    }
    const processId = Number(NodeFS.readFileSync(pidPath, "utf8"));
    assert.isFalse(
      isProcessAlive(processId),
      "transport exit was published before the Droid child terminated",
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("terminates the transport when a frame exceeds the incoming line cap", () =>
  Effect.gen(function* () {
    const script = `
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", () => {
        process.stdout.write("x".repeat(2 * 1024 * 1024 + 64) + "\\n");
      });
      process.stdin.resume();
      setInterval(() => {}, 1_000);
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const requestResult = yield* Effect.result(
      client.request("droid.list_models", {}, { timeoutMs: undefined }),
    );
    assert.equal(requestResult._tag, "Failure");
    if (requestResult._tag === "Failure") {
      assert.equal(requestResult.failure.kind, "process-exit");
    }

    const exit = yield* within(client.exits, "oversized frame did not terminate the transport");
    assert.include(exit.description, "Droid stdout stream failed");

    const [notifications, serverRequests] = yield* within(
      Effect.all([
        Stream.runCollect(client.notifications),
        Stream.runCollect(client.serverRequests),
      ]),
      "public streams did not end after the oversized frame",
    );
    assert.isEmpty(notifications);
    assert.isEmpty(serverRequests);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("rejects a second answer to the same server request", () =>
  Effect.gen(function* () {
    const script = `
      const request = {
        jsonrpc: "2.0",
        type: "request",
        id: "ask-once",
        method: "droid.ask_user",
        params: {
          toolCallId: "ask-1",
          questions: [{
            index: 0,
            topic: "Scope",
            question: "Which scope?",
            options: ["workspace"]
          }]
        }
      };
      process.stdout.write(JSON.stringify(request) + "\\n");
      process.stdin.resume();
      setInterval(() => {}, 1_000);
    `;
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", script],
    });

    const serverRequests = yield* within(
      Stream.runCollect(client.serverRequests.pipe(Stream.take(1))),
      "server request was not delivered",
    );
    const serverRequest = serverRequests[0];
    assert.isDefined(serverRequest);
    if (serverRequest === undefined) {
      return;
    }

    yield* within(
      serverRequest.respond({ answers: {} }).pipe(Effect.orDie),
      "first server-request answer did not send",
    );

    const second = yield* Effect.result(serverRequest.respond({ answers: {} }));
    assert.equal(second._tag, "Failure");
    if (second._tag === "Failure") {
      assert.instanceOf(second.failure, DroidRpcError);
      assert.equal(second.failure.kind, "duplicate-server-response");
    }

    yield* within(client.shutdown, "client shutdown did not complete");
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("times out requests blocked by outbound backpressure", () =>
  Effect.gen(function* () {
    const client = yield* makeDroidRpcClient({
      command: process.execPath,
      args: ["-e", "setInterval(() => {}, 1_000)"],
    });
    const largePayload = { text: "x".repeat(1024 * 1024) };

    const results = yield* Effect.all(
      Array.from({ length: 8 }, () =>
        Effect.result(client.request("droid.blocked_write", largePayload, { timeoutMs: 20 })),
      ),
      { concurrency: "unbounded" },
    ).pipe(Effect.timeoutOption("1 second"));

    assert.isTrue(Option.isSome(results));
    if (Option.isSome(results)) {
      assert.isTrue(
        results.value.every(
          (result) => result._tag === "Failure" && result.failure.kind === "timeout",
        ),
      );
    }

    yield* within(client.shutdown, "client shutdown did not complete");
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

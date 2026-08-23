// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { DroidSettings, ProviderInstanceId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import { makeDroidTextGeneration } from "./DroidTextGeneration.ts";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);
const decodeInitializeParams = Schema.decodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      restrictToolIds: Schema.optional(Schema.Array(Schema.String)),
    }),
  ),
);

function makeTextGenerationDroid(options: {
  readonly outputChunks: ReadonlyArray<string>;
  readonly completionReason: string;
}) {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-droid-text-"));
  const scriptPath = NodePath.join(tempDir, "fake-droid.mjs");
  const binaryPath = NodePath.join(tempDir, "droid");
  const initializeParamsPath = NodePath.join(tempDir, "initialize-params.json");
  NodeFS.writeFileSync(
    scriptPath,
    `
      import * as fs from "node:fs";
      import * as readline from "node:readline";

      const write = (message) => process.stdout.write(JSON.stringify({
        jsonrpc: "2.0",
        factoryApiVersion: "1.0.0",
        ...message
      }) + "\\n");
      const respond = (id, result) => write({ type: "response", id, result });
      const notify = (notification) => write({
        type: "notification",
        method: "droid.session_notification",
        params: { sessionId: "text-session", notification }
      });

      const lines = readline.createInterface({ input: process.stdin });
      for await (const line of lines) {
        const request = JSON.parse(line);
        if (request.method === "droid.initialize_session") {
          fs.writeFileSync(
            ${JSON.stringify(initializeParamsPath)},
            JSON.stringify(request.params),
            "utf8"
          );
          respond(request.id, { sessionId: "text-session" });
          continue;
        }
        if (request.method === "droid.add_user_message") {
          if (typeof request.params?.messageId !== "string" || !request.params.messageId) {
            write({
              type: "response",
              id: request.id,
              error: { code: -32602, message: "add_user_message requires messageId" }
            });
            continue;
          }
          respond(request.id, {});
          for (const textDelta of ${JSON.stringify(options.outputChunks)}) {
            notify({
              type: "assistant_text_delta",
              messageId: "assistant-1",
              blockIndex: 0,
              textDelta
            });
          }
          notify({
            type: "agent_turn_completed",
            reason: ${JSON.stringify(options.completionReason)},
            turnId: "turn-1",
            tokenUsage: {
              inputTokens: 1,
              outputTokens: 1,
              cacheCreationTokens: 0,
              cacheReadTokens: 0,
              thinkingTokens: 0
            }
          });
        }
      }
    `,
    "utf8",
  );
  NodeFS.writeFileSync(
    binaryPath,
    `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(scriptPath)}\n`,
    "utf8",
  );
  NodeFS.chmodSync(binaryPath, 0o755);
  return { binaryPath, initializeParamsPath, tempDir };
}

it.effect("fails observably when streamed Droid output exceeds the one-shot limit", () =>
  Effect.gen(function* () {
    const { binaryPath, initializeParamsPath, tempDir } = makeTextGenerationDroid({
      outputChunks: ['{"title":"Bounded output"}', " ".repeat(300_000)],
      completionReason: "completed",
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true })),
    );
    const textGeneration = yield* makeDroidTextGeneration(decodeDroidSettings({ binaryPath }));

    const error = yield* Effect.flip(
      textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "Generate a concise title",
        modelSelection: createModelSelection(ProviderInstanceId.make("droid"), "mock-fast"),
      }),
    );

    assert.equal(error._tag, "TextGenerationError");
    assert.include(error.detail, "output exceeded the 262144-character limit");
    const initializeParams = decodeInitializeParams(
      NodeFS.readFileSync(initializeParamsPath, "utf8"),
    );
    assert.deepStrictEqual(initializeParams.restrictToolIds, ["t3_text_generation"]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

it.effect("rejects structured output from a turn that did not complete successfully", () =>
  Effect.gen(function* () {
    const { binaryPath, tempDir } = makeTextGenerationDroid({
      outputChunks: ['{"title":"Must not be accepted"}'],
      completionReason: "model_authentication_failed",
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true })),
    );
    const textGeneration = yield* makeDroidTextGeneration(decodeDroidSettings({ binaryPath }));

    const result = yield* Effect.result(
      textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "Generate a concise title",
        modelSelection: createModelSelection(ProviderInstanceId.make("droid"), "mock-fast"),
      }),
    );

    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure._tag, "TextGenerationError");
      assert.include(result.failure.detail, "model_authentication_failed");
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

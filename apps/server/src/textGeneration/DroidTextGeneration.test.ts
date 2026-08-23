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

function makeOversizedOutputDroid() {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-droid-text-"));
  const scriptPath = NodePath.join(tempDir, "fake-droid.mjs");
  const binaryPath = NodePath.join(tempDir, "droid");
  NodeFS.writeFileSync(
    scriptPath,
    `
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
          respond(request.id, { sessionId: "text-session" });
          continue;
        }
        if (request.method === "droid.add_user_message") {
          respond(request.id, {});
          notify({
            type: "assistant_text_delta",
            messageId: "assistant-1",
            blockIndex: 0,
            textDelta: JSON.stringify({ title: "Bounded output" })
          });
          notify({
            type: "assistant_text_delta",
            messageId: "assistant-1",
            blockIndex: 0,
            textDelta: " ".repeat(300_000)
          });
          notify({
            type: "agent_turn_completed",
            reason: "completed",
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
  return { binaryPath, tempDir };
}

it.effect("fails observably when streamed Droid output exceeds the one-shot limit", () =>
  Effect.gen(function* () {
    const { binaryPath, tempDir } = makeOversizedOutputDroid();
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
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), TestClock.withLive),
);

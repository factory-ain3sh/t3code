import * as NodeServices from "@effect/platform-node/NodeServices";
import { DroidSettings } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import type { DroidCommandInfo, DroidModelInfo, DroidSkillInfo } from "../droid/DroidProtocol.ts";
import { DroidModelInfo as DroidModelInfoSchema } from "../droid/DroidProtocol.ts";
import {
  buildDroidDiscoveredModels,
  buildDroidSkills,
  buildDroidSlashCommands,
  checkDroidProviderStatus,
  detectDroidAuth,
} from "./DroidProvider.ts";

const decodeModelInfo = Schema.decodeUnknownSync(DroidModelInfoSchema);
const decodeDroidSettings = Schema.decodeSync(DroidSettings);

const makeInventoryProbeBinary = Effect.fn("makeInventoryProbeBinary")(function* (
  mode: "concurrent" | "commands-error",
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3-droid-provider-",
  });
  const binaryPath = path.join(directory, "droid");
  const script = `#!/usr/bin/env node
const readline = require("node:readline");

if (process.argv[2] === "--version") {
  process.stdout.write("droid 0.200.0\\n");
  process.exit(0);
}

const mode = "${mode}";
const pending = new Map();
let releasedRequestCount;

function write(message) {
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", type: "response", ...message }) + "\\n");
}

function resultFor(method) {
  switch (method) {
    case "droid.list_models":
      return {
        models: [
          {
            id: mode === "concurrent" ? "concurrent-" + releasedRequestCount : "discovered-model",
            displayName: "Discovered Model"
          }
        ]
      };
    case "droid.list_commands":
      return { commands: [{ name: "review", description: "Review changes" }] };
    case "droid.list_skills":
      return {
        skills: [
          {
            name: "verify",
            filePath: "/skills/verify/SKILL.md",
            location: "personal",
            enabled: true
          }
        ]
      };
  }
}

function respond(request) {
  if (mode === "commands-error" && request.method === "droid.list_commands") {
    write({ id: request.id, error: { code: -32603, message: "command inventory failed" } });
    return;
  }
  write({ id: request.id, result: resultFor(request.method) });
}

function handle(request) {
  if (mode === "commands-error") {
    respond(request);
    return;
  }
  if (releasedRequestCount !== undefined) {
    respond(request);
    return;
  }
  pending.set(request.id, request);
  if (pending.size === 1) {
    setImmediate(() => {
      releasedRequestCount = pending.size;
      for (const pendingRequest of pending.values()) respond(pendingRequest);
      pending.clear();
    });
  }
}

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => handle(JSON.parse(line)));
input.once("close", () => process.exit(0));
`;
  yield* fileSystem.writeFileString(binaryPath, script);
  yield* fileSystem.chmod(binaryPath, 0o755);
  return binaryPath;
});

describe("buildDroidDiscoveredModels", () => {
  it("keeps enabled models, dedupes ids, and falls back to the id for a display name", () => {
    const models: ReadonlyArray<DroidModelInfo> = [
      { id: "claude-opus-5", displayName: "Claude Opus 5" },
      { id: "claude-opus-5", displayName: "Duplicate" },
      { id: " gpt-5-6-luna ", displayName: "  " },
      { id: "retired-model", displayName: "Retired", disabled: true },
    ];

    assert.deepEqual(buildDroidDiscoveredModels(models), [
      {
        slug: "claude-opus-5",
        name: "Claude Opus 5",
        isCustom: false,
        isDefault: true,
        capabilities: { optionDescriptors: [] },
      },
      {
        slug: "gpt-5-6-luna",
        name: "gpt-5-6-luna",
        isCustom: false,
        capabilities: { optionDescriptors: [] },
      },
    ]);
  });

  it("marks Droid's configured default independently of discovery order", () => {
    const models = buildDroidDiscoveredModels([
      { id: "gpt-5-6-luna", displayName: "GPT-5.6 Luna" },
      { id: "claude-opus-5", displayName: "Claude Opus 5" },
    ]);

    assert.equal(models[0]?.isDefault, undefined);
    assert.equal(models[1]?.isDefault, true);
  });

  it("surfaces Droid's own custom models as ordinary probe models", () => {
    // Verbatim `droid.list_models` shape for a BYOK entry, `isCustom` included.
    const model = decodeModelInfo({
      id: "custom:factory://kimi-k3",
      displayName: "factory://kimi-k3",
      modelProvider: "generic-chat-completion-api",
      isCustom: true,
      noImageSupport: false,
      disabled: false,
    });

    assert.deepEqual(buildDroidDiscoveredModels([model]), [
      {
        slug: "custom:factory://kimi-k3",
        name: "factory://kimi-k3",
        // T3 renders `isCustom` rows from its own custom-model config, so a true
        // here would hide the model from the provider's Models section.
        isCustom: false,
        capabilities: { optionDescriptors: [] },
      },
    ]);
  });

  it("exposes supported reasoning efforts as a select option descriptor", () => {
    const [model] = buildDroidDiscoveredModels([
      {
        id: "gpt-5-6-luna",
        displayName: "GPT-5.6 Luna",
        supportedReasoningEfforts: ["low", "high"],
        defaultReasoningEffort: "high",
      },
    ]);

    assert.deepEqual(model?.capabilities?.optionDescriptors, [
      {
        id: "reasoningEffort",
        label: "Reasoning effort",
        type: "select",
        currentValue: "high",
        options: [
          { id: "low", label: "low" },
          { id: "high", label: "high", isDefault: true },
        ],
      },
    ]);
  });
});

describe("buildDroidSlashCommands", () => {
  it("maps argument hints to command input, drops blanks, dedupes, and sorts by name", () => {
    const commands: ReadonlyArray<DroidCommandInfo> = [
      { name: "review", description: "Review the diff", argumentHint: "<path>" },
      { name: "deploy", description: "  " },
      { name: "review", description: "Shadowed duplicate" },
      { name: "  ", description: "Nameless" },
      { name: "release", description: "Cut a release", argumentHint: "   ", isExecutable: true },
    ];

    assert.deepEqual(buildDroidSlashCommands(commands), [
      { name: "deploy" },
      { name: "release", description: "Cut a release" },
      { name: "review", description: "Review the diff", input: { hint: "<path>" } },
    ]);
  });
});

describe("buildDroidSkills", () => {
  it("keeps user-invocable skills, carries disabled state, and maps location to scope", () => {
    const skills: ReadonlyArray<DroidSkillInfo> = [
      {
        name: "voice",
        description: "Write like a human.",
        location: "personal",
        filePath: "/home/dev/.factory/skills/voice/SKILL.md",
        enabled: true,
        userInvocable: true,
      },
      {
        name: "open-pr",
        location: "project",
        filePath: "/repo/.agents/skills/open-pr/SKILL.md",
        enabled: false,
      },
      {
        name: "runtime-internal",
        location: "builtin",
        filePath: "/opt/droid/skills/runtime/SKILL.md",
        userInvocable: false,
      },
    ];

    assert.deepEqual(buildDroidSkills(skills), [
      {
        name: "open-pr",
        path: "/repo/.agents/skills/open-pr/SKILL.md",
        enabled: false,
        scope: "project",
      },
      {
        name: "voice",
        path: "/home/dev/.factory/skills/voice/SKILL.md",
        enabled: true,
        scope: "personal",
        description: "Write like a human.",
        shortDescription: "Write like a human.",
      },
    ]);
  });

  it("treats a skill with no explicit invocability as user-invocable", () => {
    const skills = buildDroidSkills([{ name: "spec", filePath: "/skills/spec/SKILL.md" }]);

    assert.deepEqual(skills, [{ name: "spec", path: "/skills/spec/SKILL.md", enabled: true }]);
  });
});

it.layer(NodeServices.layer)("detectDroidAuth", (it) => {
  it.effect("reports an API key without touching the filesystem", () =>
    Effect.gen(function* () {
      const auth = yield* detectDroidAuth({ FACTORY_API_KEY: "fk-live", HOME: "/nonexistent" });

      assert.deepEqual(auth, { status: "authenticated", type: "api-key", label: "API key" });
    }),
  );

  // FACTORY_HOME_OVERRIDE replaces the *user home*, so the credential lives at
  // <override>/.factory — not at <override> itself. Getting this wrong reports a
  // signed-in user as unauthenticated.
  it.effect("resolves the stored login under FACTORY_HOME_OVERRIDE's .factory directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-auth-" });
      yield* fs.makeDirectory(path.join(home, ".factory"), { recursive: true });
      yield* fs.writeFileString(path.join(home, ".factory", "auth.v2.keyring"), "{}");

      const auth = yield* detectDroidAuth({ FACTORY_HOME_OVERRIDE: home, HOME: "/nonexistent" });

      assert.deepEqual(auth, {
        status: "authenticated",
        type: "oauth",
        label: "Factory account",
      });
    }),
  );

  it.effect("degrades to unknown when no credential is on disk", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-droid-auth-" });

      const auth = yield* detectDroidAuth({ HOME: home });

      assert.deepEqual(auth, { status: "unknown" });
    }),
  );
});

it.layer(NodeServices.layer)("checkDroidProviderStatus", (it) => {
  it.effect("issues the three inventory requests concurrently", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* makeInventoryProbeBinary("concurrent");
        const snapshot = yield* checkDroidProviderStatus(
          decodeDroidSettings({ enabled: true, binaryPath }),
          { FACTORY_API_KEY: "test-key", PATH: process.env.PATH },
        );

        assert.equal(snapshot.status, "ready");
        assert.deepEqual(
          snapshot.models.map((model) => model.slug),
          ["concurrent-3"],
        );
        assert.deepEqual(snapshot.slashCommands, [
          { name: "review", description: "Review changes" },
        ]);
        assert.deepEqual(snapshot.skills, [
          {
            name: "verify",
            path: "/skills/verify/SKILL.md",
            enabled: true,
            scope: "personal",
          },
        ]);
      }),
    ),
  );

  it.effect("points a user with no droid binary at the supported installer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = "/definitely/not/installed/t3-droid";
        const snapshot = yield* checkDroidProviderStatus(
          decodeDroidSettings({ enabled: true, binaryPath }),
          { PATH: process.env.PATH },
        );

        assert.equal(snapshot.status, "error");
        assert.equal(snapshot.installed, false);
        assert.equal(
          snapshot.message,
          [
            `Droid CLI command \`${binaryPath}\` was not found.`,
            `Install the Droid CLI, make sure \`${binaryPath}\` is on PATH, then restart T3 Code.`,
            "See https://docs.factory.ai/cli/getting-started/quickstart.",
          ].join(" "),
        );
      }),
    ),
  );

  it.effect("warns and uses the complete fallback when one inventory request fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* makeInventoryProbeBinary("commands-error");
        const snapshot = yield* checkDroidProviderStatus(
          decodeDroidSettings({
            enabled: true,
            binaryPath,
            customModels: ["custom:test-model"],
          }),
          { FACTORY_API_KEY: "test-key", PATH: process.env.PATH },
        );

        assert.equal(snapshot.status, "warning");
        assert.deepEqual(
          snapshot.models.map((model) => model.slug),
          ["claude-opus-5", "claude-sonnet-5", "custom:test-model"],
        );
        assert.deepEqual(snapshot.slashCommands, []);
        assert.deepEqual(snapshot.skills, []);
        assert.equal(
          snapshot.message,
          "Droid inventory discovery failed. Using fallback models; slash commands and skills are unavailable.",
        );
      }),
    ),
  );
});

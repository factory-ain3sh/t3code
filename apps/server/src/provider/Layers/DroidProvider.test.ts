import * as NodeServices from "@effect/platform-node/NodeServices";
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
  detectDroidAuth,
} from "./DroidProvider.ts";

const decodeModelInfo = Schema.decodeUnknownSync(DroidModelInfoSchema);

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

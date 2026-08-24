import {
  type DroidSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  type DroidCommandInfo,
  DroidListCommandsResult,
  DroidListModelsResult,
  DroidListSkillsResult,
  type DroidModelInfo,
  type DroidSkillInfo,
} from "../droid/DroidProtocol.ts";
import { makeDroidExecRpcClient } from "../droid/DroidRpcClient.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";

const DROID_PRESENTATION = {
  displayName: "Droid",
  badgeLabel: "Early Access",
  // Droid's Spec Mode maps onto the plan/build toggle, and models switch
  // in-session via droid.update_session_settings.
  showInteractionModeToggle: true,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const INVENTORY_DISCOVERY_TIMEOUT_MS = 15_000;
const DROID_DEFAULT_MODEL = "claude-opus-5";

export const DROID_LOGIN_MESSAGE = "Run `droid` in a terminal to sign in to Factory.";

const DROID_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: DROID_DEFAULT_MODEL,
    name: "Claude Opus 5",
    isCustom: false,
    isDefault: true,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "claude-sonnet-5",
    name: "Claude Sonnet 5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function droidModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = DROID_BUILT_IN_MODELS,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

function reasoningEffortCapabilities(model: DroidModelInfo): ModelCapabilities {
  const efforts = model.supportedReasoningEfforts;
  if (efforts.length === 0) return EMPTY_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "reasoningEffort",
        label: "Reasoning effort",
        options: efforts.map((effort) => ({
          value: effort,
          label: effort,
          ...(model.defaultReasoningEffort === effort ? { isDefault: true } : {}),
        })),
      }),
    ],
  });
}

/**
 * Every model the CLI reports is a probe result, including the `custom:` entries a
 * user configured in Droid's own settings. They stay `isCustom: false` because T3's
 * flag means "slug the user typed into T3's custom-model field": custom rows render
 * from that config list, so marking a probe model custom would drop it from the
 * Models section entirely instead of merely labelling it.
 */
export function buildDroidDiscoveredModels(
  models: ReadonlyArray<DroidModelInfo>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  return models
    .filter((model) => model.disabled !== true)
    .map((model): ServerProviderModel | undefined => {
      const slug = model.id.trim();
      if (!slug || seen.has(slug)) return undefined;
      seen.add(slug);
      return {
        slug,
        name: model.displayName.trim() || slug,
        shortName: model.shortDisplayName.trim(),
        isCustom: false,
        ...(slug === DROID_DEFAULT_MODEL ? { isDefault: true } : {}),
        capabilities: reasoningEffortCapabilities(model),
      };
    })
    .filter((model): model is ServerProviderModel => model !== undefined);
}

export function buildDroidSlashCommands(
  commands: ReadonlyArray<DroidCommandInfo>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set<string>();
  const slashCommands: ServerProviderSlashCommand[] = [];
  for (const command of commands) {
    const name = command.name.trim();
    if (!name || seen.has(name)) continue;
    seen.add(name);
    const description = command.description.trim();
    const hint = command.argumentHint?.trim();
    slashCommands.push({
      name,
      ...(description ? { description } : {}),
      ...(hint ? { input: { hint } } : {}),
    });
  }
  return slashCommands.toSorted((left, right) => left.name.localeCompare(right.name));
}

/**
 * Droid's own user-facing surfaces hide skills the user cannot invoke — built-ins
 * are authored `userInvocable: false` (factory-mono skills/builtin/loadBuiltinSkill.ts)
 * and filtered out of its command palette (acp/session/availableCommands.ts). We apply
 * the same rule, but carry `enabled` through instead of filtering on it: the skill
 * contract models disabled state, and clients render it.
 */
export function buildDroidSkills(
  skills: ReadonlyArray<DroidSkillInfo>,
): ReadonlyArray<ServerProviderSkill> {
  const seen = new Set<string>();
  const providerSkills: ServerProviderSkill[] = [];
  for (const skill of skills) {
    if (skill.userInvocable === false) continue;
    const name = skill.name.trim();
    const path = skill.filePath.trim();
    if (!name || !path || seen.has(name)) continue;
    seen.add(name);
    const description = skill.description?.trim();
    // SkillLocation is `project | personal | builtin | automation`; the first two
    // are already understood by the client's skill-source resolver.
    const scope = skill.location.trim();
    providerSkills.push({
      name,
      path,
      enabled: skill.enabled !== false,
      scope,
      ...(description ? { description, shortDescription: description } : {}),
    });
  }
  return providerSkills.toSorted((left, right) => left.name.localeCompare(right.name));
}

/**
 * Detect the credentials `droid exec` would resolve: FACTORY_API_KEY always
 * wins; otherwise the stored WorkOS login under the Factory home directory.
 * macOS keychain-only logins have no on-disk artifact, so absence degrades
 * to `unknown`, never a false `unauthenticated`.
 */
export const detectDroidAuth = Effect.fn("detectDroidAuth")(function* (
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderAuth, never, FileSystem.FileSystem | Path.Path> {
  if (environment.FACTORY_API_KEY?.trim()) {
    return { status: "authenticated", type: "api-key", label: "API key" };
  }
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // FACTORY_HOME_OVERRIDE replaces the *user home*, not the .factory dir
  // (factory-mono packages/environment/src/resolve.ts resolveHomeDir).
  const home =
    environment.FACTORY_HOME_OVERRIDE?.trim() ||
    environment.HOME?.trim() ||
    environment.USERPROFILE?.trim();
  const factoryHome = home ? path.join(home, ".factory") : undefined;
  if (!factoryHome) {
    return { status: "unknown" };
  }
  for (const candidate of ["auth.v2.keyring", "auth.v2.loginkeychain", "auth.v2.file"]) {
    const exists = yield* fileSystem
      .exists(path.join(factoryHome, candidate))
      .pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      return { status: "authenticated", type: "oauth", label: "Factory account" };
    }
  }
  return { status: "unknown" };
});

/**
 * One droid process answers every inventory question. Startup is the expensive part,
 * and `list_models`/`list_commands`/`list_skills` are all session-less handlers
 * (factory-mono streamingJsonRpcExecRunner.ts) resolved against this process's cwd,
 * so environment-scoped commands and skills come back without initializing a session.
 */
const discoverDroidInventory = (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd: string = process.cwd(),
) =>
  Effect.gen(function* () {
    const rpc = yield* makeDroidExecRpcClient({
      binaryPath: droidSettings.binaryPath,
      cwd,
      env: environment,
    });

    const [modelResult, commandResult, skillResult] = yield* Effect.all(
      [
        rpc.request("droid.list_models", {}).pipe(Effect.flatMap(decodeListModelsResult)),
        rpc.request("droid.list_commands", {}).pipe(Effect.flatMap(decodeListCommandsResult)),
        rpc.request("droid.list_skills", {}).pipe(Effect.flatMap(decodeListSkillsResult)),
      ],
      { concurrency: "unbounded" },
    );

    return {
      models: buildDroidDiscoveredModels(modelResult.models),
      slashCommands: buildDroidSlashCommands(commandResult.commands),
      skills: buildDroidSkills(skillResult.skills),
    };
  }).pipe(Effect.scoped);

const droidCliCommandMissingMessage = (droidSettings: DroidSettings) => {
  const command = droidSettings.binaryPath || "droid";
  return [
    `Droid CLI command \`${command}\` was not found.`,
    `Install the Droid CLI, make sure \`${command}\` is on PATH, then restart T3 Code.`,
    "See https://docs.factory.ai/cli/getting-started/quickstart.",
  ].join(" ");
};

const runDroidVersionCommand = (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = droidSettings.binaryPath || "droid";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export function buildInitialDroidProviderSnapshot(
  droidSettings: DroidSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = droidModelsFromSettings(droidSettings.customModels);

    if (!droidSettings.enabled) {
      return buildServerProvider({
        presentation: DROID_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Droid is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Droid CLI availability...",
      },
    });
  });
}

export const checkDroidProviderStatus = Effect.fn("checkDroidProviderStatus")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd: string = process.cwd(),
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = droidModelsFromSettings(droidSettings.customModels);

  if (!droidSettings.enabled) {
    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Droid is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runDroidVersionCommand(droidSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Droid CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: droidSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? droidCliCommandMissingMessage(droidSettings)
          : "Failed to execute Droid CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: droidSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Droid CLI is installed but timed out while running `droid --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Droid CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: DROID_PRESENTATION,
      enabled: droidSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Droid CLI is installed but failed to run.",
      },
    });
  }

  const auth = yield* detectDroidAuth(environment);
  const discoveryExit = yield* discoverDroidInventory(droidSettings, environment, cwd).pipe(
    Effect.timeoutOption(INVENTORY_DISCOVERY_TIMEOUT_MS),
    Effect.exit,
  );
  const inventory =
    Exit.isSuccess(discoveryExit) && Option.isSome(discoveryExit.value)
      ? discoveryExit.value.value
      : undefined;
  let inventoryWarning: string | undefined;
  if (inventory === undefined) {
    if (Exit.isFailure(discoveryExit)) {
      yield* Effect.logWarning("Droid inventory discovery failed.", {
        errorTag: causeErrorTag(discoveryExit.cause),
      });
      inventoryWarning =
        "Droid inventory discovery failed. Using fallback models; slash commands and skills are unavailable.";
    } else {
      yield* Effect.logWarning(
        `Droid inventory discovery timed out after ${INVENTORY_DISCOVERY_TIMEOUT_MS}ms.`,
      );
      inventoryWarning = `Droid inventory discovery timed out after ${INVENTORY_DISCOVERY_TIMEOUT_MS}ms. Using fallback models; slash commands and skills are unavailable.`;
    }
  }
  const models =
    inventory !== undefined && inventory.models.length > 0
      ? droidModelsFromSettings(droidSettings.customModels, inventory.models)
      : fallbackModels;
  let message = inventoryWarning;
  if (auth.status === "unknown") {
    message = message ? `${message} ${DROID_LOGIN_MESSAGE}` : DROID_LOGIN_MESSAGE;
  }

  return buildServerProvider({
    presentation: DROID_PRESENTATION,
    enabled: droidSettings.enabled,
    checkedAt,
    models,
    ...(inventory ? { slashCommands: inventory.slashCommands, skills: inventory.skills } : {}),
    probe: {
      installed: true,
      version,
      status: inventoryWarning ? "warning" : "ready",
      auth,
      ...(message ? { message } : {}),
    },
  });
});

const decodeListModelsResult = Schema.decodeUnknownEffect(DroidListModelsResult);
const decodeListCommandsResult = Schema.decodeUnknownEffect(DroidListCommandsResult);
const decodeListSkillsResult = Schema.decodeUnknownEffect(DroidListSkillsResult);

export const enrichDroidSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Droid version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};

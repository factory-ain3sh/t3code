import {
  type DroidSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
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

import { DroidModelInfo } from "../droid/DroidProtocol.ts";
import { makeDroidRpcClient } from "../droid/DroidRpcClient.ts";
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
const MODEL_DISCOVERY_TIMEOUT_MS = 15_000;

export const DROID_LOGIN_MESSAGE = "Run `droid` in a terminal to sign in to Factory.";

const DROID_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "claude-opus-5",
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
  const efforts = model.supportedReasoningEfforts ?? [];
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
        name: model.displayName?.trim() || slug,
        ...(model.shortDisplayName?.trim() ? { shortName: model.shortDisplayName.trim() } : {}),
        isCustom: model.isCustom === true,
        capabilities: reasoningEffortCapabilities(model),
      };
    })
    .filter((model): model is ServerProviderModel => model !== undefined);
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
  for (const candidate of ["auth.v2.keyring", "auth.v2.file"]) {
    const exists = yield* fileSystem
      .exists(path.join(factoryHome, candidate))
      .pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      return { status: "authenticated", type: "oauth", label: "Factory account" };
    }
  }
  return { status: "unknown" };
});

const discoverDroidModels = (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const rpc = yield* makeDroidRpcClient({
      command: droidSettings.binaryPath,
      args: ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"],
      cwd: process.cwd(),
      env: environment,
    });
    const result = yield* rpc.request("droid.list_models", {});
    const models = yield* decodeListModels(result);
    return buildDroidDiscoveredModels(models);
  }).pipe(Effect.scoped);

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
          ? "Droid CLI (`droid`) is not installed or not on PATH. Install with `npm install -g @factory/cli`."
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
  const discoveryExit = yield* discoverDroidModels(droidSettings, environment).pipe(
    Effect.timeoutOption(MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.exit,
  );
  const discoveredModels =
    Exit.isSuccess(discoveryExit) && Option.isSome(discoveryExit.value)
      ? discoveryExit.value.value
      : undefined;
  if (discoveredModels === undefined) {
    if (Exit.isFailure(discoveryExit)) {
      yield* Effect.logWarning("Droid model discovery failed.", {
        errorTag: causeErrorTag(discoveryExit.cause),
      });
    } else {
      yield* Effect.logWarning(
        `Droid model discovery timed out after ${MODEL_DISCOVERY_TIMEOUT_MS}ms.`,
      );
    }
  }
  const models =
    discoveredModels !== undefined && discoveredModels.length > 0
      ? droidModelsFromSettings(droidSettings.customModels, discoveredModels)
      : fallbackModels;

  return buildServerProvider({
    presentation: DROID_PRESENTATION,
    enabled: droidSettings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth,
      ...(auth.status === "unknown" ? { message: DROID_LOGIN_MESSAGE } : {}),
    },
  });
});

const decodeModelInfo = Schema.decodeUnknownEffect(DroidModelInfo);

/** Undecodable entries are skipped, not fatal: model metadata drifts. */
const decodeListModels = (result: unknown) =>
  Effect.gen(function* () {
    const models = (result as { readonly models?: ReadonlyArray<unknown> } | undefined)?.models;
    if (!Array.isArray(models)) return [];
    const decoded = yield* Effect.forEach(models, (model) =>
      decodeModelInfo(model).pipe(Effect.option),
    );
    return decoded.filter(Option.isSome).map((model) => model.value);
  });

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

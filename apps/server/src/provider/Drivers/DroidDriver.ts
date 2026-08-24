import { DroidSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeDroidTextGeneration } from "../../textGeneration/DroidTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeDroidAdapter } from "../Layers/DroidAdapter.ts";
import {
  buildInitialDroidProviderSnapshot,
  checkDroidProviderStatus,
  enrichDroidSnapshot,
} from "../Layers/DroidProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
  withProviderInstanceIdentity,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makePackageManagedProviderMaintenanceResolver,
  normalizeCommandPath,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeDroidSettings = Schema.decodeSync(DroidSettings);

const DRIVER_KIND = ProviderDriverKind.make("droid");

/**
 * The curl installer puts droid in ~/.local/bin; the Windows PowerShell installer puts
 * droid.exe in %USERPROFILE%\bin. Both are self-updating single-executable installs, so
 * they update through `droid update`. Anything else (npm, bun, pnpm, Homebrew) keeps its
 * package-manager update path.
 */
function isDroidNativeCommandPath(commandPath: string): boolean {
  const normalized = normalizeCommandPath(commandPath);
  return (
    normalized.endsWith("/.local/bin/droid") || /\/users\/[^/]+\/bin\/droid\.exe$/.test(normalized)
  );
}

export const DroidProviderMaintenanceResolver = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER_KIND,
  npmPackageName: "@factory/cli",
  homebrewFormula: null,
  nativeUpdate: {
    executable: "droid",
    args: ["update"],
    lockKey: "droid-native",
    isCommandPath: isDroidNativeCommandPath,
  },
});

export type DroidDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

export const DroidDriver: ProviderDriver<DroidSettings, DroidDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Droid",
    supportsMultipleInstances: true,
  },
  configSchema: DroidSettings,
  defaultConfig: (): DroidSettings => decodeDroidSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const httpClient = yield* HttpClient.HttpClient;
      const serverConfig = yield* ServerConfig;
      const serverSettings = yield* ServerSettingsService;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withProviderInstanceIdentity({
        instanceId,
        continuationIdentity,
        displayName,
        accentColor,
      });
      const effectiveConfig = { ...config, enabled } satisfies DroidSettings;
      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(
        DroidProviderMaintenanceResolver,
        {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnv,
        },
      );

      const adapter = yield* makeDroidAdapter(effectiveConfig, {
        environment: processEnv,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        instanceId,
      });
      const textGeneration = yield* makeDroidTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkDroidProviderStatus(
        effectiveConfig,
        processEnv,
        serverConfig.cwd,
      ).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, path),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DroidSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialDroidProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          enrichDroidSnapshot({
            snapshot: currentSnapshot,
            maintenanceCapabilities,
            enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
            publishSnapshot,
            httpClient,
          }),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Droid snapshot.",
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};

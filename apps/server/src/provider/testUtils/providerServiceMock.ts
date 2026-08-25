import { ProviderDriverKind, type ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { defaultProviderContinuationIdentity } from "../ProviderDriver.ts";
import type { ProviderServiceShape } from "../Services/ProviderService.ts";

export const unsupportedProviderTestCall = <A>() =>
  Effect.die(new Error("Unsupported provider call in test")) as Effect.Effect<A, never>;

export function makeProviderServiceMock(
  driverKind: ProviderDriverKind,
  overrides: Partial<ProviderServiceShape> = {},
): ProviderServiceShape {
  return {
    startSession: () => unsupportedProviderTestCall(),
    sendTurn: () => unsupportedProviderTestCall(),
    interruptTurn: () => unsupportedProviderTestCall(),
    respondToRequest: () => unsupportedProviderTestCall(),
    respondToUserInput: () => unsupportedProviderTestCall(),
    stopSession: () => Effect.succeed("stopped"),
    listSessions: () => Effect.succeed([]),
    recoverSession: () => unsupportedProviderTestCall(),
    withSessionLifecycleLock: (_threadId, effect) => effect,
    getCapabilities: () =>
      Effect.succeed({
        sessionModelSwitch: "in-session",
        conversationRollback: "unsupported",
      }),
    getInstanceInfo: (instanceId: ProviderInstanceId) =>
      Effect.succeed({
        instanceId,
        driverKind,
        displayName: undefined,
        enabled: true,
        continuationIdentity: defaultProviderContinuationIdentity({ driverKind, instanceId }),
      }),
    rollbackConversation: () => unsupportedProviderTestCall(),
    uploadFeedback: () => unsupportedProviderTestCall(),
    streamEvents: Stream.empty,
    ...overrides,
  };
}

import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { makeProviderServiceMock } from "./providerServiceMock.ts";

it.effect("keeps the configured driver kind for custom instance ids", () =>
  Effect.gen(function* () {
    const driverKind = ProviderDriverKind.make("codex");
    const instanceId = ProviderInstanceId.make("codex_work");
    const service = makeProviderServiceMock(driverKind);

    const info = yield* service.getInstanceInfo(instanceId);

    assert.equal(info.driverKind, driverKind);
    assert.equal(info.continuationIdentity.driverKind, driverKind);
    assert.equal(info.continuationIdentity.continuationKey, "codex:instance:codex_work");
  }),
);

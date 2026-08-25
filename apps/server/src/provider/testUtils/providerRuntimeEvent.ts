import {
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
  type EventId,
  type ProviderDriverKind,
  type ProviderSessionLease,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export interface ProviderRuntimeEventFixture {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId;
  readonly sessionLease?: ProviderSessionLease;
  readonly createdAt: string;
  readonly threadId: string;
  readonly [key: string]: unknown;
}

const decodeProviderRuntimeEvent = Schema.decodeUnknownSync(ProviderRuntimeEvent);

export function makeProviderRuntimeEvent(
  fixture: ProviderRuntimeEventFixture,
  fallbackLease: ProviderSessionLease,
): ProviderRuntimeEvent {
  return decodeProviderRuntimeEvent({
    ...fixture,
    payload: fixture.payload ?? {},
    providerInstanceId:
      fixture.providerInstanceId ?? ProviderInstanceId.make(String(fixture.provider)),
    sessionLease: fixture.sessionLease ?? fallbackLease,
    threadId: ThreadId.make(fixture.threadId),
  });
}

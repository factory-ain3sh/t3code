import * as Schema from "effect/Schema";

import {
  ApprovalRequestId,
  type OrchestrationThreadActivity,
  ProviderApprovalOption,
  ProviderRequestKind,
  providerRequestKindFromRequestType,
} from "@t3tools/contracts";

const isProviderRequestKind = Schema.is(ProviderRequestKind);
const isProviderApprovalOption = Schema.is(ProviderApprovalOption);

/**
 * Reads the client-facing approval kind from an orchestration activity
 * payload: the server-stamped `requestKind` when present, otherwise derived
 * from the raw provider request type. Dynamic tool calls use the generic
 * executable-action bucket so they remain actionable on clients that do not
 * render provider-specific tool kinds.
 */
export function approvalRequestKindFromPayload(
  payload: Readonly<Record<string, unknown>> | null | undefined,
): ProviderRequestKind | null {
  const requestKind = payload?.requestKind;
  if (isProviderRequestKind(requestKind)) {
    return requestKind;
  }

  return typeof payload?.requestType === "string"
    ? (providerRequestKindFromRequestType(payload.requestType) ?? null)
    : null;
}

/**
 * Reads the provider-declared approval options from an activity payload.
 * Returns undefined when the payload carries none the client can act on
 * (absent field, legacy persisted approvals, or every decision unknown to
 * this client version), so callers fall back to their default option set.
 */
export function approvalOptionsFromPayload(
  payload: Readonly<Record<string, unknown>> | null | undefined,
): ReadonlyArray<ProviderApprovalOption> | undefined {
  const options = payload?.options;
  if (!Array.isArray(options)) {
    return undefined;
  }
  const valid = options.filter(isProviderApprovalOption);
  return valid.length > 0 ? valid : undefined;
}

export interface PendingProviderApproval {
  readonly requestId: ApprovalRequestId;
  readonly requestKind: ProviderRequestKind;
  readonly createdAt: string;
  readonly detail?: string;
  readonly appName?: string;
  readonly options?: ReadonlyArray<ProviderApprovalOption>;
}

function approvalPayload(
  activity: OrchestrationThreadActivity,
): Readonly<Record<string, unknown>> | null {
  return activity.payload && typeof activity.payload === "object"
    ? (activity.payload as Record<string, unknown>)
    : null;
}

export function isStalePendingRequestFailureDetail(detail: string | undefined): boolean {
  const normalized = detail?.toLowerCase();
  return (
    normalized?.includes("stale pending approval request") === true ||
    normalized?.includes("stale pending user-input request") === true ||
    normalized?.includes("unknown pending approval request") === true ||
    normalized?.includes("unknown pending permission request") === true ||
    normalized?.includes("unknown pending user-input request") === true ||
    normalized?.includes("unknown pending user input request") === true ||
    normalized?.includes("unknown pending codex user input request") === true
  );
}

/**
 * Reduces approval lifecycle activities in caller-provided lifecycle order.
 * Callers own ordering because web and mobile already share their ordered
 * activity lists with other presentation reducers.
 */
export function reducePendingApprovals(
  orderedActivities: ReadonlyArray<OrchestrationThreadActivity>,
): PendingProviderApproval[] {
  const openByRequestId = new Map<ApprovalRequestId, PendingProviderApproval>();

  for (const activity of orderedActivities) {
    const payload = approvalPayload(activity);
    const requestId =
      typeof payload?.requestId === "string" ? ApprovalRequestId.make(payload.requestId) : null;

    if (activity.kind === "approval.requested" && requestId) {
      const requestKind = approvalRequestKindFromPayload(payload);
      if (!requestKind) {
        continue;
      }
      const detail = typeof payload?.detail === "string" ? payload.detail : undefined;
      const appName = typeof payload?.appName === "string" ? payload.appName : undefined;
      const options = approvalOptionsFromPayload(payload);
      openByRequestId.set(requestId, {
        requestId,
        requestKind,
        createdAt: activity.createdAt,
        ...(detail ? { detail } : {}),
        ...(appName ? { appName } : {}),
        ...(options ? { options } : {}),
      });
      continue;
    }

    if (activity.kind === "approval.resolved" && requestId) {
      openByRequestId.delete(requestId);
      continue;
    }

    if (
      activity.kind === "provider.approval.respond.failed" &&
      requestId &&
      isStalePendingRequestFailureDetail(
        typeof payload?.detail === "string" ? payload.detail : undefined,
      )
    ) {
      openByRequestId.delete(requestId);
    }
  }

  return [...openByRequestId.values()].toSorted((left, right) =>
    left.createdAt.localeCompare(right.createdAt),
  );
}

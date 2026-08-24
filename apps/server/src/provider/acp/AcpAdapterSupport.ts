import {
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  type ProviderDriverKind,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  type ProviderAdapterError,
} from "../Errors.ts";
const isAcpProcessExitedError = Schema.is(EffectAcpErrors.AcpProcessExitedError);
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);

export function mapAcpToAdapterError(
  provider: ProviderDriverKind,
  threadId: ThreadId,
  method: string,
  error: EffectAcpErrors.AcpError,
): ProviderAdapterError {
  if (isAcpProcessExitedError(error)) {
    return new ProviderAdapterSessionClosedError({
      provider,
      threadId,
      cause: error,
    });
  }
  if (isAcpRequestError(error)) {
    return new ProviderAdapterRequestError({
      provider,
      method,
      detail: error.message,
      cause: error,
    });
  }
  return new ProviderAdapterRequestError({
    provider,
    method,
    detail: error.message,
    cause: error,
  });
}

function acpOptionForDecision(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): EffectAcpSchema.RequestPermissionRequest["options"][number] | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : decision === "decline"
          ? "reject_once"
          : undefined;
  if (kind === undefined) {
    return undefined;
  }
  return request.options.find((entry) => entry.kind === kind);
}

export function selectAcpPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  // The option id is an opaque agent token: trimming only validates
  // non-emptiness, and the reply carries the id back byte-for-byte.
  const optionId = acpOptionForDecision(request, decision)?.optionId;
  return optionId?.trim() ? optionId : undefined;
}

const ACP_DECISION_FALLBACK_LABELS = {
  accept: "Approve",
  acceptForSession: "Always allow this session",
  decline: "Decline",
} as const;

/**
 * Approval options this ACP request supports, labeled with the agent's own
 * option names. Cancel is always offered: it resolves the request client-side
 * without selecting an ACP option.
 */
export function acpApprovalOptions(
  request: EffectAcpSchema.RequestPermissionRequest,
): ReadonlyArray<ProviderApprovalOption> {
  const options: ProviderApprovalOption[] = [];
  for (const decision of ["accept", "acceptForSession", "decline"] as const) {
    const option = acpOptionForDecision(request, decision);
    if (option?.optionId.trim()) {
      options.push({
        decision,
        label: option.name.trim() || ACP_DECISION_FALLBACK_LABELS[decision],
      });
    }
  }
  options.push({ decision: "cancel", label: "Cancel" });
  return options;
}

export function selectAutoApprovedAcpPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectAcpPermissionOptionId(request, "acceptForSession") ??
    selectAcpPermissionOptionId(request, "accept")
  );
}

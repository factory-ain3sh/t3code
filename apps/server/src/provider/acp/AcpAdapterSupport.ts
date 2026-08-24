import {
  type ProviderApprovalDecision,
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

export function selectAcpPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  const option = request.options.find((entry) => entry.kind === kind);
  return option?.optionId.trim() || undefined;
}

export function supportedAcpApprovalDecisions(
  request: EffectAcpSchema.RequestPermissionRequest,
): ReadonlyArray<ProviderApprovalDecision> {
  const decisions: ProviderApprovalDecision[] = [];
  if (selectAcpPermissionOptionId(request, "accept") !== undefined) {
    decisions.push("accept");
  }
  if (selectAcpPermissionOptionId(request, "acceptForSession") !== undefined) {
    decisions.push("acceptForSession");
  }
  if (selectAcpPermissionOptionId(request, "decline") !== undefined) {
    decisions.push("decline");
  }
  decisions.push("cancel");
  return decisions;
}

export function selectAutoApprovedAcpPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectAcpPermissionOptionId(request, "acceptForSession") ??
    selectAcpPermissionOptionId(request, "accept")
  );
}

import { PROVIDER_APPROVAL_DECISIONS, type ProviderApprovalDecision } from "@t3tools/contracts";

export type ApprovalRequestKind = "command" | "file-read" | "file-change" | "plan";

/**
 * Reads the client-facing approval kind from an orchestration activity payload.
 * Dynamic tool calls use the generic executable-action bucket so they remain
 * actionable on clients that do not render provider-specific tool kinds.
 */
export function approvalRequestKindFromPayload(
  payload: Readonly<Record<string, unknown>> | null | undefined,
): ApprovalRequestKind | null {
  const requestKind = payload?.requestKind;
  if (
    requestKind === "command" ||
    requestKind === "file-read" ||
    requestKind === "file-change" ||
    requestKind === "plan"
  ) {
    return requestKind;
  }

  switch (payload?.requestType) {
    case "command_execution_approval":
    case "exec_command_approval":
    case "dynamic_tool_call":
      return "command";
    case "file_read_approval":
      return "file-read";
    case "file_change_approval":
    case "apply_patch_approval":
      return "file-change";
    case "plan_approval":
      return "plan";
    default:
      return null;
  }
}

/**
 * Reads the decisions declared by the provider request. The default exists
 * only for approvals already persisted before this capability became part of
 * the activity payload.
 */
export function supportedApprovalDecisionsFromPayload(
  payload: Readonly<Record<string, unknown>> | null | undefined,
): ReadonlyArray<ProviderApprovalDecision> {
  const decisions = payload?.supportedDecisions;
  if (!Array.isArray(decisions)) {
    return PROVIDER_APPROVAL_DECISIONS;
  }
  return decisions.filter(
    (decision): decision is ProviderApprovalDecision =>
      typeof decision === "string" &&
      PROVIDER_APPROVAL_DECISIONS.includes(decision as ProviderApprovalDecision),
  );
}

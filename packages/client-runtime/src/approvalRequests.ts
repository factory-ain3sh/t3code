import * as Schema from "effect/Schema";

import { ProviderApprovalOption, ProviderRequestKind } from "@t3tools/contracts";

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
    case "mcp_elicitation_approval":
      return "mcp-elicitation";
    default:
      return null;
  }
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

import type { ProviderRequestKind } from "@t3tools/contracts";
import { memo } from "react";

import { type PendingApproval } from "../../session-logic";
import { cn } from "~/lib/utils";

interface ComposerPendingApprovalPanelProps {
  approval: PendingApproval;
  pendingCount: number;
  className?: string;
}

const REQUEST_KIND_LABELS = {
  command: {
    fallback: "Command approval",
    detail: "Command",
  },
  "file-read": {
    fallback: "File read approval",
    detail: "File to read",
  },
  "file-change": {
    fallback: "File change approval",
    detail: "File change",
  },
  plan: {
    fallback: "Plan approval",
    detail: "Plan",
  },
  "mcp-elicitation": {
    fallback: "App access approval",
    detail: "App access request",
  },
} satisfies Record<ProviderRequestKind, { fallback: string; detail: string }>;

export const ComposerPendingApprovalPanel = memo(function ComposerPendingApprovalPanel({
  approval,
  pendingCount,
  className,
}: ComposerPendingApprovalPanelProps) {
  const labels = REQUEST_KIND_LABELS[approval.requestKind];

  return (
    <div
      aria-label={labels.fallback}
      className={cn("flex min-w-0 flex-1 items-center gap-2", className)}
      role="group"
    >
      {approval.appName ? (
        <span className="max-w-32 shrink truncate text-[11px] font-medium text-foreground">
          {approval.appName}
        </span>
      ) : null}
      <code
        aria-label={labels.detail}
        className="block max-h-20 min-w-0 flex-1 overflow-auto whitespace-pre font-mono text-[11px] text-foreground/85 [scrollbar-width:thin] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70 [&::-webkit-scrollbar]:h-1.5"
        data-approval-detail="complete"
        tabIndex={0}
      >
        {approval.detail || labels.fallback}
      </code>
      {pendingCount > 1 ? (
        <span className="shrink-0 text-[10px] font-medium text-muted-foreground tabular-nums">
          1/{pendingCount}
        </span>
      ) : null}
    </div>
  );
});

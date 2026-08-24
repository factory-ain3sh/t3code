import { type ApprovalRequestId, type ProviderApprovalDecision } from "@t3tools/contracts";
import { memo } from "react";
import { Button } from "../ui/button";

interface ComposerPendingApprovalActionsProps {
  requestId: ApprovalRequestId;
  supportedDecisions: ReadonlyArray<ProviderApprovalDecision>;
  isResponding: boolean;
  onRespondToApproval: (
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ) => Promise<unknown>;
}

const APPROVAL_ACTION_CLASS_NAME = "font-normal";

export const ComposerPendingApprovalActions = memo(function ComposerPendingApprovalActions({
  requestId,
  supportedDecisions,
  isResponding,
  onRespondToApproval,
}: ComposerPendingApprovalActionsProps) {
  return (
    <>
      {supportedDecisions.includes("cancel") ? (
        <Button
          size="micro"
          variant="ghost-muted"
          className={APPROVAL_ACTION_CLASS_NAME}
          disabled={isResponding}
          onClick={() => void onRespondToApproval(requestId, "cancel")}
        >
          Cancel
        </Button>
      ) : null}
      {supportedDecisions.includes("decline") ? (
        <Button
          size="micro"
          variant="ghost-muted"
          className={`${APPROVAL_ACTION_CLASS_NAME} text-destructive-foreground [:hover,[data-pressed]]:text-destructive-foreground`}
          disabled={isResponding}
          onClick={() => void onRespondToApproval(requestId, "decline")}
        >
          Decline
        </Button>
      ) : null}
      {supportedDecisions.includes("acceptForSession") ? (
        <Button
          size="micro"
          variant="ghost-muted"
          className={APPROVAL_ACTION_CLASS_NAME}
          disabled={isResponding}
          onClick={() => void onRespondToApproval(requestId, "acceptForSession")}
        >
          Always allow this session
        </Button>
      ) : null}
      {supportedDecisions.includes("accept") ? (
        <Button
          size="micro"
          variant="ghost-muted"
          className={`${APPROVAL_ACTION_CLASS_NAME} text-foreground`}
          disabled={isResponding}
          onClick={() => void onRespondToApproval(requestId, "accept")}
        >
          Approve
        </Button>
      ) : null}
    </>
  );
});

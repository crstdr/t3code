import type { MessageId } from "@t3tools/contracts";
import * as Context from "effect/Context";

// Only the private synchronization entry point supplies this planning permit.
// Persisted transport ownership is independently checked by the SQL commit hook.
export class OverseerNoticeGrant extends Context.Reference<
  | { readonly messageId: MessageId; readonly queued: boolean; readonly retiring?: boolean }
  | undefined
>("t3/overseer/OverseerNoticeGrant", {
  defaultValue: ():
    | { readonly messageId: MessageId; readonly queued: boolean; readonly retiring?: boolean }
    | undefined => undefined,
}) {}

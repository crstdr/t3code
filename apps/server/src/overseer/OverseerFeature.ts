import * as Context from "effect/Context";
import { WS_METHODS } from "@t3tools/contracts";

export const rpcTags = [
  WS_METHODS.oversightWatch,
  WS_METHODS.oversightUnlink,
  WS_METHODS.oversightConfigure,
] as const;

// No runtime setting or environment variable enables unapproved agent-facing text.
// Slice 6 can provide this reference only after the release/text gates are met.
export class Enabled extends Context.Reference<boolean>("t3/overseer/Enabled", {
  defaultValue: () => false,
}) {}

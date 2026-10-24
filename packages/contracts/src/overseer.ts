import * as Schema from "effect/Schema";
import { NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ThreadId, ProjectId } from "./baseSchemas.ts";
import { OrchestrationV2ShellThreadStatus } from "./orchestrationV2.ts";

export const OversightLink = Schema.Struct({
  linkId: TrimmedNonEmptyString,
  observerThreadId: ThreadId,
  targetThreadId: ThreadId,
  projectId: ProjectId,
  title: Schema.String,
  titleTruncated: Schema.optionalKey(Schema.Literal(true)),
  workerStatus: Schema.Union([
    OrchestrationV2ShellThreadStatus,
    Schema.Literals(["reserved", "archived", "deleted"]),
  ]),
  pendingRequestCount: NonNegativeInt,
  deliveryState: Schema.Literals(["pending", "queued", "accepted", "held", "error", "closed"]),
});
export type OversightLink = typeof OversightLink.Type;
export const OversightWatchInput = Schema.Struct({
  threadId: ThreadId,
  cursor: Schema.optionalKey(TrimmedNonEmptyString),
});
export type OversightWatchInput = typeof OversightWatchInput.Type;
export const OversightPage = Schema.Struct({
  links: Schema.Array(OversightLink),
  nextCursor: Schema.optionalKey(TrimmedNonEmptyString),
});
export type OversightPage = typeof OversightPage.Type;
export const OversightWatchFrame = Schema.Struct({
  ...OversightPage.fields,
  revision: NonNegativeInt,
});
export type OversightWatchFrame = typeof OversightWatchFrame.Type;
export const OversightUnlinkInput = Schema.Struct({ linkId: TrimmedNonEmptyString });
export type OversightUnlinkInput = typeof OversightUnlinkInput.Type;
export const OversightUnlinkResult = Schema.Struct({
  linkId: TrimmedNonEmptyString,
  closed: Schema.Literal(true),
});
export const OversightLinkInput = Schema.Struct({
  op: Schema.Literals(["list", "unlink"]),
  cursor: Schema.optionalKey(TrimmedNonEmptyString),
  linkId: Schema.optionalKey(TrimmedNonEmptyString),
});
export class OversightError extends Schema.TaggedError<OversightError>()("OversightError", {
  reason: Schema.Literals(["unsupported", "not_found", "invalid_cursor", "unavailable"]),
}) {}

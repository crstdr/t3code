import {
  ModelSelection,
  TrimmedNonEmptyString,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  RuntimeMode,
  ProviderInteractionMode,
  ProjectId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { McpAttachmentInput } from "../mcp/toolkits/attachment/input.ts";
import { launchText } from "./OverseerText.ts";

export const LaunchParameters = Schema.Struct({
  projectId: Schema.optional(ProjectId),
  scratch: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "Launch without a project, in its own folder under the environment's Scratch project. Not with projectId or workspaceStrategy.",
    }),
  ),
  title: TrimmedNonEmptyString,
  modelSelection: Schema.optional(ModelSelection),
  runtimeMode: Schema.optional(RuntimeMode),
  interactionMode: Schema.optional(ProviderInteractionMode),
  workspaceStrategy: Schema.optional(
    OrchestrationV2ThreadLaunchWorkspaceStrategy.annotate({
      description:
        "Choose where this thread runs before starting its agent: worktree creates and binds a new checkout from baseRef; existing_worktree binds worktreePath, which must be one of the project's git worktrees; root uses the project checkout. Omitted means root, not the caller's worktree. For a PR stack use the parent branch as baseRef and startFromOrigin:false. Uncommitted changes are not copied.",
    }),
  ),
  message: Schema.optional(
    Schema.String.check(Schema.isMaxLength(120000)).annotate({
      description:
        "First task prompt, delivered after workspace preparation. Omit message and attachments to create an idle thread.",
    }),
  ),
  attachments: Schema.optional(Schema.Array(McpAttachmentInput).check(Schema.isMaxLength(8))),
});

export const OverseerLaunchParameters = Schema.Struct({
  ...LaunchParameters.fields,
  oversee: Schema.optional(Schema.Boolean.annotate({ description: launchText.T4 })),
  clientRequestId: Schema.optional(
    Schema.String.check(Schema.isMaxLength(256))
      .annotate({ description: launchText.T5 })
      .pipe(Schema.decodeTo(TrimmedNonEmptyString)),
  ),
});

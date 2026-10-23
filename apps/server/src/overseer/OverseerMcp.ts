import {
  OversightPage,
  OversightLinkInput,
  OversightUnlinkResult,
  OrchestratorMcpFailure,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { extendLaunchParameters, type Request } from "./OverseerLaunchInput.ts";
import * as Launch from "./OverseerLaunchService.ts";
import * as Service from "./OverseerService.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import { Tool, Toolkit } from "effect/ai";
import { linkText } from "./OverseerText.ts";
import { oversightLaunchDescription } from "./OverseerText.ts";

export function extendThreadLaunch(baseline: typeof ProjectToolkit.tools.t3_thread_launch) {
  const tool = baseline
    .setParameters(extendLaunchParameters(baseline.parametersSchema))
    .setSuccess(
      Schema.Struct({ ...baseline.successSchema.fields, linkId: Schema.optional(Schema.String) }),
    )
    .setFailure(
      Schema.Struct({
        ...OrchestratorMcpFailure.fields,
        linkId: Schema.optional(Schema.String),
        threadId: Schema.optional(ThreadId),
        projectId: Schema.optional(ProjectId),
      }),
    )
    .addDependency(Service.OverseerService);
  // Fluent operations clone the tool; the ordinary catalog must retain its exact description.
  return Object.assign(tool, {
    description: oversightLaunchDescription(baseline.description ?? ""),
  });
}

export const LinkToolkit = Toolkit.make(
  Tool.make("t3_thread_link", {
    description: linkText.description,
    parameters: Schema.Struct({
      op: OversightLinkInput.fields.op.annotate({ description: linkText.op }),
      cursor: Schema.optionalKey(Schema.String.annotate({ description: linkText.cursor })),
      linkId: Schema.optionalKey(Schema.String.annotate({ description: linkText.linkId })),
    }),
    success: Schema.Struct({
      links: Schema.optionalKey(OversightPage.fields.links),
      nextCursor: OversightPage.fields.nextCursor,
      linkId: Schema.optionalKey(OversightUnlinkResult.fields.linkId),
      closed: Schema.optionalKey(OversightUnlinkResult.fields.closed),
    }),
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }).addDependency(Service.OverseerService),
);

export const layerLinks = McpToolAccess.toLayer(LinkToolkit, {
  t3_thread_link: McpToolAccess.readsAsCaller((input) =>
    Service.OverseerService.use((service) => service.link(input)).pipe(
      Effect.catchTags({
        OversightError: (error) =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: error.reason === "unavailable" ? "orchestration_error" : "invalid_request",
              message: linkText[error.reason],
            }),
          ),
      }),
    ),
  ),
});

export const launch = Effect.fn("OverseerMcp.launch")(function* (input: Request) {
  if (input.oversee !== true)
    return yield* Launch.toMcpFailure(new Launch.OverseerLaunchError({ reason: "E3" }));
  const service = yield* Effect.serviceOption(Service.OverseerService);
  if (Option.isNone(service))
    return yield* Launch.toMcpFailure(new Launch.OverseerLaunchError({ reason: "E8" }));
  const result = yield* service.value
    .launch(input)
    .pipe(
      Effect.catchTags({ OverseerLaunchError: (error) => Effect.fail(Launch.toMcpFailure(error)) }),
    );
  const thread = result.projection.thread;
  const run = result.projection.runs.find(
    (entry) => entry.userMessageId === result.initialMessageId,
  );
  return {
    threadId: thread.id,
    projectId: thread.projectId,
    modelSelection: thread.modelSelection,
    runId: run?.id ?? null,
    status: run?.status ?? null,
    ...(result.linkId === undefined ? {} : { linkId: result.linkId }),
  };
});

import { OrchestratorMcpFailure, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { extendLaunchParameters, type Request } from "./OverseerLaunchInput.ts";
import * as Launch from "./OverseerLaunchService.ts";
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
    .addDependency(Launch.OverseerLaunchService);
  // Fluent operations clone the tool; the ordinary catalog must retain its exact description.
  return Object.assign(tool, {
    description: oversightLaunchDescription(baseline.description ?? ""),
  });
}

export const launch = Effect.fn("OverseerMcp.launch")(function* (input: Request) {
  if (input.oversee !== true)
    return yield* Launch.toMcpFailure(new Launch.OverseerLaunchError({ reason: "E3" }));
  const service = yield* Effect.serviceOption(Launch.OverseerLaunchService);
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

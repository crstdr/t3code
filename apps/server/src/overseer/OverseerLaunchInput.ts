import { TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { launchText } from "./OverseerText.ts";

const options = Schema.Struct({
  oversee: Schema.optional(Schema.Boolean.annotate({ description: launchText.T4 })),
  clientRequestId: Schema.optional(
    Schema.String.check(Schema.isMaxLength(256))
      .annotate({ description: launchText.T5 })
      .pipe(Schema.decodeTo(TrimmedNonEmptyString)),
  ),
});

type Baseline = typeof ProjectToolkit.tools.t3_thread_launch.parametersSchema;
export type Request = Baseline["Type"] & typeof options.Type;

export function extendLaunchParameters(baseline: Baseline) {
  return Schema.Struct({ ...baseline.fields, ...options.fields });
}

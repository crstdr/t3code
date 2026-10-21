import { assert, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import { Tool } from "effect/ai";
import { ProjectToolkit, ProjectToolkitOverseer } from "../mcp/toolkits/project/tools.ts";
import * as Feature from "./OverseerFeature.ts";
import { launchText } from "./OverseerText.ts";
import { OverseerLaunchError, toMcpFailure } from "./OverseerLaunchService.ts";

it.effect(
  "keeps the unapproved catalog hidden by default and snapshots the exact opt-in catalog and errors",
  () =>
    Effect.gen(function* () {
      assert.isFalse(yield* Feature.Enabled);
      const ordinary = ProjectToolkit.tools.t3_thread_launch;
      const overseen = ProjectToolkitOverseer.tools.t3_thread_launch;
      assert.isFalse(Tool.getDescription(ordinary)?.includes("oversee:true") ?? false);
      assert.notProperty(Tool.getJsonSchema(ordinary).properties, "oversee");
      assert.isTrue(Tool.getStrictMode(ordinary));
      assert.isTrue(Tool.getStrictMode(overseen));
      assert.deepEqual(
        Object.keys(ProjectToolkit.tools),
        Object.keys(ProjectToolkitOverseer.tools),
      );
      const catalog = [ordinary, overseen].map((tool) => ({
        name: tool.name,
        description: Tool.getDescription(tool),
        inputSchema: Tool.getJsonSchema(tool),
        annotations: {
          readOnlyHint: Context.get(tool.annotations, Tool.Readonly),
          destructiveHint: Context.get(tool.annotations, Tool.Destructive),
          idempotentHint: Context.get(tool.annotations, Tool.Idempotent),
          openWorldHint: Context.get(tool.annotations, Tool.OpenWorld),
        },
      }));
      for (const key of ["T1", "T2", "T3", "T4", "T5"] as const)
        assert.include(JSON.stringify(catalog[1]), launchText[key]);
      assert.include(JSON.stringify(catalog[1]), '"maxLength":256');
      expect({
        catalog,
        strings: launchText,
        errors: (["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"] as const).map(
          (reason) => toMcpFailure(new OverseerLaunchError({ reason })).message,
        ),
      }).toMatchSnapshot();
    }),
);

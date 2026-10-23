import { assert, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Layer from "effect/Layer";
import { McpServer } from "effect/ai";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { ChatAttachmentId } from "@t3tools/contracts";
import { toolkitRegistration } from "../mcp/McpHttpServer.ts";
import * as Handlers from "../mcp/toolkits/project/handlers.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import { createPendingAttachmentId } from "../attachmentStore.ts";
import * as Config from "../config.ts";
import * as Claims from "../orchestration-v2/AttachmentClaims.ts";
import { harness } from "./OverseerLaunch.testkit.ts";
import pinnedCatalog from "./fixtures/upstream-11adcb0e25-project-catalog.json" with { type: "json" };
import pinnedAttachmentFailures from "./fixtures/upstream-96f09c6558-attachment-failures.json" with { type: "json" };
import { Tool } from "effect/ai";
import { ProjectToolkit, ProjectToolkitOverseer } from "../mcp/toolkits/project/tools.ts";
import * as Feature from "./OverseerFeature.ts";
import { launchText } from "./OverseerText.ts";
import { OverseerLaunchError, toMcpFailure } from "./OverseerLaunchService.ts";

// Register against Effect's in-memory MCP registry, without a transport or listener.
const renderCatalog = Effect.fn("test.overseer.catalog")(function* (overseen = false) {
  const registration = overseen
    ? toolkitRegistration(ProjectToolkitOverseer, Handlers.layerOverseer)
    : toolkitRegistration(ProjectToolkit, Handlers.layer);
  return yield* Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return server.tools.map(({ tool }) => tool);
  }).pipe(
    Effect.provide(
      registration.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(
          Layer.mergeAll(
            harness(),
            NodeCrypto.layer,
            Layer.mock(Repositories.SourceControlRepositoryService)({}),
          ),
        ),
      ),
    ),
  );
});

// Captured by toolkitRegistration from git-show 11adcb0e25:.../project/tools.ts
// (blob 2114565d8201ea11c085f43a7c1c3e17b0c7d7ea), not Tool.getJsonSchema.
it.effect("the disabled project's actual MCP catalog is byte-identical to pinned upstream", () =>
  Effect.gen(function* () {
    assert.equal(JSON.stringify(yield* renderCatalog()), JSON.stringify(pinnedCatalog));
  }),
);

it.effect(
  "ordinary attachment error priority matches pinned upstream for overlapping invalid inputs",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* Config.ServerConfig;
      yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
      const id = ChatAttachmentId.make(createPendingAttachmentId());
      const source = path.join(config.attachmentsDir, `${id}.png`);
      yield* fs.writeFile(source, new Uint8Array([1, 2, 3, 4]));
      const wrongType = {
        type: "image" as const,
        id,
        name: "pending.png",
        mimeType: "image/jpeg",
        sizeBytes: 4,
      };
      const messages: string[] = [];
      for (const sizeBytes of [999, 4]) {
        const failure = yield* Claims.claimPendingAttachments({
          threadId: "ordinary",
          attachments: [{ ...wrongType, sizeBytes }],
        }).pipe(Effect.flip);
        messages.push(failure.message);
      }
      yield* fs.remove(source);
      const missing = yield* Claims.claimPendingAttachments({
        threadId: "ordinary",
        attachments: [{ ...wrongType, sizeBytes: 999 }],
      }).pipe(Effect.flip);
      messages.push(missing.message);
      // Captured from upstream AttachmentClaims blob 40955e74a12f8a5a6b48da2ab3e92e3123ba283a.
      assert.deepEqual(messages, pinnedAttachmentFailures);
      assert.deepEqual(yield* fs.readDirectory(config.attachmentsDir), []);
    }).pipe(
      Effect.provide(
        Config.layerTest(process.cwd(), { prefix: "overseer-compatibility-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
);

it.effect(
  "keeps the unapproved catalog hidden by default and snapshots the exact opt-in catalog and errors",
  () =>
    Effect.gen(function* () {
      assert.isFalse(yield* Feature.Enabled);
      const ordinary = ProjectToolkit.tools.t3_thread_launch;
      const overseen = ProjectToolkitOverseer.tools.t3_thread_launch;
      assert.isFalse(Tool.getDescription(ordinary)?.includes("oversee:true") ?? false);
      assert.notProperty(Tool.getJsonSchema(ordinary).properties, "oversee");
      assert.notEqual(Tool.getStrictMode(ordinary), true);
      assert.notEqual(Tool.getStrictMode(overseen), true);
      assert.deepEqual(
        Object.keys(ProjectToolkit.tools),
        Object.keys(ProjectToolkitOverseer.tools),
      );
      const catalog = [
        (yield* renderCatalog()).find((tool) => tool.name === "t3_thread_launch"),
        (yield* renderCatalog(true)).find((tool) => tool.name === "t3_thread_launch"),
      ];
      const upstream = pinnedCatalog.find((tool) => tool.name === "t3_thread_launch")!;
      const { oversee, clientRequestId, ...properties } = catalog[1]!.inputSchema.properties ?? {};
      assert.isDefined(oversee);
      assert.isDefined(clientRequestId);
      assert.equal(
        JSON.stringify({ ...catalog[1]!.inputSchema, properties }),
        JSON.stringify(upstream.inputSchema),
      );
      for (const key of ["T1", "T2", "T3", "T4", "T5"] as const)
        assert.include(JSON.stringify(catalog[1]), launchText[key]);
      assert.include(JSON.stringify(catalog[1]), '"maxLength":256');
      expect({
        catalog,
        entryBytes: {
          ordinary: new TextEncoder().encode(JSON.stringify(catalog[0])).byteLength,
          enabled: new TextEncoder().encode(JSON.stringify(catalog[1])).byteLength,
        },
        strings: launchText,
        errors: (["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"] as const).map(
          (reason) => toMcpFailure(new OverseerLaunchError({ reason })).message,
        ),
      }).toMatchSnapshot();
    }),
);

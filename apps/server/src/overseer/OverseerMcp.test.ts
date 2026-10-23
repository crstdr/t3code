import { assert, it } from "@effect/vitest";
import { OversightPage, OversightUnlinkResult, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Mcp from "./OverseerMcp.ts";
import * as Service from "./OverseerService.ts";
import * as Store from "./OverseerStore.ts";
import * as ToolAccess from "../mcp/McpToolAccess.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import {
  setup,
  request,
  harness,
  enabled,
  invocation,
  observer,
} from "./OverseerLaunch.testkit.ts";

const decodePage = Schema.decodeUnknownEffect(OversightPage);
const decodeUnlink = Schema.decodeUnknownEffect(OversightUnlinkResult);
const decodeFailure = Schema.decodeUnknownEffect(OrchestratorMcpFailure);

it.effect(
  "actual link-tool calls isolate endpoints, reject read-only unlink and leave worker runs untouched",
  () =>
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Service.OverseerService;
      const first = yield* service.launch(request);
      const second = yield* service.launch({ ...request, clientRequestId: "second" });
      const privateRow = yield* (yield* Store.OverseerStore).reservationFor(
        observer,
        request.clientRequestId,
      );
      assert.notEqual(first.linkId, privateRow!.link_id);
      const tools = yield* Mcp.LinkToolkit.pipe(
        Effect.provide(ToolAccess.HandlersLayer.layer(Mcp.layerLinks)),
      );
      const otherEndpoint = {
        ...invocation,
        thread: { ...invocation.thread!, threadId: second.projection.thread.id },
      };
      const spoofed = { op: "list", threadId: observer } as const;
      const listed = Option.getOrThrow(
        yield* tools
          .handle("t3_thread_link", spoofed, undefined)
          .pipe(
            Stream.unwrap,
            Stream.runHead,
            Effect.provideService(Invocation.McpInvocationContext, otherEndpoint),
          ),
      );
      assert.isFalse(listed.isFailure);
      const page = yield* decodePage(listed.encodedResult);
      assert.deepEqual(
        page.links.map((link) => link.linkId),
        [second.linkId],
      );
      const refused = Option.getOrThrow(
        yield* tools
          .handle("t3_thread_link", { op: "unlink", linkId: first.linkId! }, undefined)
          .pipe(
            Stream.unwrap,
            Stream.runHead,
            Effect.provideService(Invocation.McpInvocationContext, otherEndpoint),
          ),
      );
      assert.isTrue(refused.isFailure);
      assert.equal((yield* decodeFailure(refused.encodedResult)).code, "invalid_request");
      const readonly = {
        ...invocation,
        client: { sessionId: "readonly", label: "readonly", access: "read-only" as const },
      };
      const denied = Option.getOrThrow(
        yield* tools
          .handle("t3_thread_link", { op: "unlink", linkId: first.linkId! }, undefined)
          .pipe(
            Stream.unwrap,
            Stream.runHead,
            Effect.provideService(Invocation.McpInvocationContext, readonly),
          ),
      );
      assert.isTrue(denied.isFailure);
      assert.equal((yield* decodeFailure(denied.encodedResult)).code, "capability_denied");
      const sql = yield* SqlClient.SqlClient;
      const before =
        yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${first.projection.thread.id}`;
      const owner = {
        ...invocation,
        thread: { ...invocation.thread!, threadId: first.projection.thread.id },
      };
      for (const caller of [owner, invocation]) {
        const result = Option.getOrThrow(
          yield* tools
            .handle("t3_thread_link", { op: "unlink", linkId: first.linkId! }, undefined)
            .pipe(
              Stream.unwrap,
              Stream.runHead,
              Effect.provideService(Invocation.McpInvocationContext, caller),
            ),
        );
        assert.isFalse(result.isFailure);
        assert.deepEqual(yield* decodeUnlink(result.encodedResult), {
          linkId: first.linkId,
          closed: true,
        });
      }
      assert.deepEqual(
        yield* sql`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${first.projection.thread.id}`,
        before,
      );
    }).pipe(enabled, Effect.provide(harness())),
);

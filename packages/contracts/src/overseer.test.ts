import { it, expect } from "@effect/vitest";
import * as Schema from "effect/Schema";
import * as Current from "./orchestrationV2.ts";
import * as Official from "./fixtures/official-v0.1.2/orchestrationV2.ts";
import {
  OversightWatchFrame,
  OversightWatchInput,
  OversightUnlinkInput,
  OversightUnlinkResult,
} from "./overseer.ts";

const now = "2026-10-23T00:00:00.000Z";
const thread = {
  id: "worker",
  projectId: "project:b",
  title: "Ordinary worker",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "worker" },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "mcp",
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledOverride: null,
  settledAt: null,
};
const message = {
  id: "wake",
  threadId: "observer",
  runId: null,
  nodeId: null,
  role: "user",
  text: "Oversight updates",
  attachments: [],
  streaming: false,
  createdBy: "agent",
  creationSource: "server",
  createdAt: now,
  updatedAt: now,
  notification: {
    source: { kind: "background_task" },
    outcome: "updated",
    summary: "Oversight updates",
  },
};
const shell = {
  ...thread,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  itemCount: 0,
  visibleItemCount: 0,
};

it("round-trips the isolated public contracts without requiring capability on old descriptors", () => {
  const frame = {
    revision: 12,
    links: [
      {
        linkId: "opaque",
        observerThreadId: "observer",
        targetThreadId: "worker",
        projectId: "project:b",
        title: "Worker",
        workerStatus: "preparing",
        pendingRequestCount: 2,
        deliveryState: "held",
      },
    ],
    nextCursor: "opaque-cursor",
  };
  for (const [schema, input] of [
    [OversightWatchFrame, frame],
    [OversightWatchInput, { threadId: "observer" }],
    [OversightUnlinkInput, { linkId: "opaque" }],
    [OversightUnlinkResult, { linkId: "opaque", closed: true }],
  ] as const) {
    const codec = Schema.toCodecJson(schema);
    expect(Schema.encodeSync(codec)(Schema.decodeUnknownSync(codec)(input))).toEqual(input);
  }
});

it("actual official release decoders accept new encoded ordinary worker, wake, event and shell fixtures", () => {
  const currentMessage = Schema.toCodecJson(Current.OrchestrationV2ConversationMessage);
  const officialMessage = Schema.toCodecJson(Official.OrchestrationV2ConversationMessage);
  const encodedMessage = Schema.encodeSync(currentMessage)(
    Schema.decodeUnknownSync(currentMessage)(message),
  );
  expect(
    Schema.encodeSync(officialMessage)(Schema.decodeUnknownSync(officialMessage)(encodedMessage)),
  ).toEqual(encodedMessage);
  const currentEvent = Schema.toCodecJson(Current.OrchestrationV2DomainEvent);
  const officialEvent = Schema.toCodecJson(Official.OrchestrationV2DomainEvent);
  for (const event of [
    { id: "created", threadId: "worker", occurredAt: now, type: "thread.created", payload: thread },
    {
      id: "notice",
      threadId: "observer",
      occurredAt: now,
      type: "message.updated",
      payload: message,
    },
  ]) {
    const encoded = Schema.encodeSync(currentEvent)(Schema.decodeUnknownSync(currentEvent)(event));
    const decodedOld = Schema.decodeUnknownSync(officialEvent)(encoded);
    expect(Schema.encodeSync(officialEvent)(decodedOld)).toEqual(encoded);
    expect(
      Schema.encodeSync(currentEvent)(
        Schema.decodeUnknownSync(currentEvent)(Schema.encodeSync(officialEvent)(decodedOld)),
      ),
    ).toEqual(encoded);
  }
  const currentShell = Schema.toCodecJson(Current.OrchestrationV2RpcSchemas.subscribeShell.output);
  const officialShell = Schema.toCodecJson(
    Official.OrchestrationV2RpcSchemas.subscribeShell.output,
  );
  const encodedShell = Schema.encodeSync(currentShell)(
    Schema.decodeUnknownSync(currentShell)({
      kind: "thread.updated",
      sequence: 1,
      location: "active",
      thread: shell,
    }),
  );
  expect(
    Schema.encodeSync(officialShell)(Schema.decodeUnknownSync(officialShell)(encodedShell)),
  ).toEqual(encodedShell);
});

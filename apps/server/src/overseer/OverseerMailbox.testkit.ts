import { assert } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Events from "../orchestration-v2/EventStore.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Positions from "../orchestration-v2/TurnItemPositionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Feature from "./OverseerFeature.ts";
import * as Store from "./OverseerStore.ts";
import * as Transport from "./OverseerTransport.ts";
import * as Mailbox from "./OverseerMailbox.ts";
import * as Gate from "./OverseerGate.ts";

export const now = DateTime.makeUnsafe("2026-10-21T00:00:00.000Z");
export const observer = ThreadId.make("observer");
export const target = ThreadId.make("worker");
const project = ProjectId.make("project");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
export const enabled = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Feature.Enabled, true));
const stores = Layer.mergeAll(
  Gate.layer,
  Store.layer,
  Transport.layer,
  Mailbox.layer,
  Events.layer,
  Receipts.layer,
  Projections.layer,
  Projects.layer,
  Positions.layer,
  Outbox.layer,
);
export const layer = Sink.layerFromStores.pipe(
  Layer.provideMerge(stores),
  Layer.provideMerge(Sqlite.layerMemory),
);

const thread = (id: ThreadId): OrchestrationV2AppThread => ({
  id,
  projectId: project,
  title: id,
  providerInstanceId: selection.instanceId,
  modelSelection: selection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "web",
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
});
const run = (ordinal: number, overrides: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run => ({
  id: RunId.make(`run:${ordinal}`),
  threadId: target,
  ordinal,
  providerInstanceId: selection.instanceId,
  modelSelection: selection,
  providerThreadId: null,
  userMessageId: MessageId.make(`message:${ordinal}`),
  rootNodeId: null,
  activeAttemptId: RunAttemptId.make(`attempt:${ordinal}`),
  status: "running",
  requestedAt: now,
  startedAt: now,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
  ...overrides,
});
const message = (
  work: OrchestrationV2Run,
  overrides: Partial<OrchestrationV2ConversationMessage> = {},
): OrchestrationV2ConversationMessage => ({
  id: work.userMessageId,
  threadId: work.threadId,
  runId: work.id,
  nodeId: null,
  role: "user",
  text: "Work",
  attachments: [],
  streaming: false,
  createdBy: "user",
  creationSource: "web",
  createdAt: now,
  updatedAt: now,
  ...overrides,
});
type NewEvent = OrchestrationV2DomainEvent extends infer E
  ? E extends OrchestrationV2DomainEvent
    ? Omit<E, "id" | "occurredAt">
    : never
  : never;

export const setup = Effect.fn("test.overseerMailbox.setup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.OverseerStore;
  const sink = yield* Sink.EventSinkV2;
  const transport = yield* Transport.OverseerTransport;
  const mailbox = yield* Mailbox.OverseerMailbox;
  const outbox = yield* Outbox.EffectOutboxV2;
  const gate = yield* Gate.OverseerGate;
  let serial = 0;
  const write = Effect.fn("test.overseerMailbox.write")(function* (
    events: ReadonlyArray<NewEvent>,
    commandType?: string,
    threadId = observer,
  ) {
    const decoded = events.map((event) => ({
      ...event,
      id: EventId.make(`event:${++serial}`),
      occurredAt: now,
    }));
    if (commandType)
      return (yield* sink.commitCommand({
        commandId: CommandId.make(`command:${++serial}`),
        threadId,
        commandType,
        acceptedAt: now,
        events: decoded,
        effects: [],
      })).storedEvents;
    return yield* sink.write({ events: decoded });
  });
  yield* sql`INSERT INTO projection_projects(project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${project}, 'test', '/isolated', '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)})`;
  yield* write([{ type: "thread.created", threadId: observer, payload: thread(observer) }]);
  yield* store.reserve({
    linkId: "link",
    observerThreadId: observer,
    observerProjectId: project,
    targetThreadId: target,
    targetProjectId: project,
    clientRequestId: "retry",
    fingerprint: "input",
    launchCommandId: CommandId.make("launch"),
    launchMessageId: MessageId.make("initial"),
    launchInputJson: "{}",
  });
  yield* write([{ type: "thread.created", threadId: target, payload: thread(target) }]);
  const start = Effect.fn("test.overseerMailbox.start")(function* (ordinal: number) {
    const work = run(ordinal);
    yield* write([
      { type: "message.updated", threadId: target, payload: message(work) },
      { type: "run.created", threadId: target, payload: work },
    ]);
    return work;
  });
  const question = Effect.fn("test.overseerMailbox.question")(function* (
    work: OrchestrationV2Run,
    id: string,
  ) {
    const request = {
      id: RuntimeRequestId.make(id),
      nodeId: NodeId.make(`node:${id}`),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: now,
      resolvedAt: null,
    };
    yield* write([
      { type: "runtime-request.updated", threadId: target, runId: work.id, payload: request },
    ]);
    return request;
  });
  const prepare = Effect.fn("test.overseerMailbox.prepare")(function* (text = "transport fixture") {
    const box = (yield* store.mailbox(observer))!;
    const fence = { observerThreadId: observer, epoch: box.epoch, revision: box.revision };
    const staged = yield* store.stageBatch(fence);
    if (staged.state !== "ready") return assert.fail(`not ready: ${staged.state}`);
    return yield* transport.prepare({ ...fence, batchNumber: staged.batch.batch_number, text });
  });
  const freeze = Effect.fn("test.overseerMailbox.freeze")(function* (
    row: Transport.TransportRow,
    ordinal = 100,
    status: "starting" | "queued" | "running" = "running",
  ) {
    const wake = run(ordinal, {
      threadId: observer,
      id: RunId.make(`wake:${ordinal}`),
      userMessageId: MessageId.make(row.message_id),
      providerThreadId: ProviderThreadId.make("provider:observer"),
      status,
    });
    yield* write([
      {
        type: "provider-thread.updated",
        threadId: observer,
        payload: {
          id: wake.providerThreadId!,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: selection.instanceId,
          appThreadId: observer,
          ownerNodeId: null,
          providerSessionId: ProviderSessionId.make("session"),
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "active",
          firstRunOrdinal: ordinal,
          lastRunOrdinal: ordinal,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      },
      {
        type: "message.updated",
        threadId: observer,
        payload: message(wake, {
          text: row.text,
          createdBy: "agent",
          creationSource: "server",
          notification: {
            source: { kind: "background_task" },
            outcome: "updated",
            summary: "Oversight updates",
          },
        }),
      },
      { type: "run.created", threadId: observer, payload: wake },
    ]);
    return {
      messageId: MessageId.make(row.message_id),
      runId: wake.id,
      attemptId: wake.activeAttemptId!,
      providerSessionId: "session",
    };
  });
  const sync = Effect.fn("test.overseerMailbox.sync")(function* <E, R>(
    dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E, R>,
  ) {
    const box = (yield* store.mailbox(observer))!;
    assert.isDefined(box.flush_id);
    yield* mailbox.sync(
      {
        observerThreadId: observer,
        generation: box.flush_id!.slice("effect:overseer-flush:".length),
      },
      dispatch,
    );
  });
  return {
    sql,
    store,
    sink,
    outbox,
    transport,
    mailbox,
    gate,
    write,
    start,
    question,
    prepare,
    freeze,
    sync,
  };
});

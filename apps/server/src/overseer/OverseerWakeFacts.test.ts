import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderDriverKind,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Worker from "../orchestration-v2/EffectWorker.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Events from "../orchestration-v2/EventStore.ts";
import * as Maintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../orchestration-v2/ProjectStore.ts";
import * as Positions from "../orchestration-v2/TurnItemPositionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Store from "./OverseerStore.ts";
import * as Facts from "./OverseerWakeFacts.ts";

const now = DateTime.makeUnsafe("2026-10-21T00:00:00.000Z");
const projectId = ProjectId.make("project:worker");
const observerProject = ProjectId.make("project:observer");
const observer = ThreadId.make("observer");
const target = ThreadId.make("worker");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
const providerId = ProviderThreadId.make("provider:worker");
const makeThread = (id: ThreadId, project = projectId): OrchestrationV2AppThread => ({
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
const makeRun = (
  ordinal: number,
  overrides: Partial<OrchestrationV2Run> = {},
): OrchestrationV2Run => ({
  id: RunId.make(`run:${ordinal}`),
  threadId: target,
  ordinal,
  providerInstanceId: selection.instanceId,
  modelSelection: selection,
  providerThreadId: providerId,
  userMessageId: MessageId.make(`message:${ordinal}`),
  rootNodeId: null,
  activeAttemptId: null,
  status: "running",
  requestedAt: now,
  startedAt: now,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
  ...overrides,
});
const makeMessage = (
  run: OrchestrationV2Run,
  overrides: Partial<OrchestrationV2ConversationMessage> = {},
): OrchestrationV2ConversationMessage => ({
  id: run.userMessageId,
  threadId: run.threadId,
  runId: run.id,
  role: "user",
  text: "Work",
  attachments: [],
  nodeId: null,
  streaming: false,
  createdBy: "user",
  creationSource: "web",
  createdAt: now,
  updatedAt: now,
  ...overrides,
});
const makeProvider = (
  ordinal: number,
  tasks: OrchestrationV2ProviderThread["pendingBackgroundTasks"] = [],
): OrchestrationV2ProviderThread => ({
  id: providerId,
  driver: ProviderDriverKind.make("codex"),
  providerInstanceId: selection.instanceId,
  appThreadId: target,
  ownerNodeId: null,
  providerSessionId: null,
  nativeThreadRef: null,
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: 1,
  lastRunOrdinal: ordinal,
  handoffIds: [],
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  pendingBackgroundTasks: tasks,
});
const layers = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) => {
  const stores = Layer.mergeAll(
    Store.layer,
    Facts.layer,
    Events.layer,
    Projections.layer,
    Projects.layer,
    Outbox.layer,
    Receipts.layer,
    Positions.layer,
  );
  const core = stores.pipe(Layer.provideMerge(database));
  return Layer.mergeAll(Sink.layerFromStores, Maintenance.layer).pipe(Layer.provideMerge(core));
};
const layerTest = layers(Sqlite.layerMemory);
const enabled = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provideService(Feature.Enabled, true));
const occurrences = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly source_id: string;
    readonly kind: string;
    readonly ready: number;
    readonly disposition: string;
    readonly source_sequence: number;
  }>`
    SELECT source_id, kind, ready, disposition, source_sequence FROM overseer_occurrences ORDER BY source_sequence, kind, source_id`;
});
type NewEvent = OrchestrationV2DomainEvent extends infer Event
  ? Event extends OrchestrationV2DomainEvent
    ? Omit<Event, "id" | "occurredAt">
    : never
  : never;
const setup = Effect.fn("test.wakeFacts.setup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* Sink.EventSinkV2;
  const store = yield* Store.OverseerStore;
  const facts = yield* Facts.OverseerWakeFacts;
  let nextEvent = 0;
  const write = Effect.fn("test.wakeFacts.write")(function* (
    events: ReadonlyArray<NewEvent>,
    effects: ReadonlyArray<Outbox.PendingOrchestrationEffectV2> = [],
    commandType?: string,
  ) {
    const decoded = events.map((event) => ({
      ...event,
      id: EventId.make(`event:${++nextEvent}`),
      occurredAt: now,
    }));
    if (commandType !== undefined)
      return (yield* sink.commitCommand({
        commandId: CommandId.make(`command:${nextEvent}`),
        threadId: decoded[0]!.threadId,
        commandType,
        acceptedAt: now,
        events: decoded,
        effects,
      })).storedEvents;
    return yield* sink.writeWithEffects({ events: decoded, effects });
  });
  for (const id of [projectId, observerProject])
    yield* sql`INSERT INTO projection_projects(project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${id}, ${id}, '/isolated', '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)})`;
  yield* write([
    { type: "thread.created", threadId: observer, payload: makeThread(observer, observerProject) },
  ]);
  const reserve = Effect.fn("test.wakeFacts.reserve")(function* (
    id = target,
    suffix = "1",
    destination = projectId,
  ) {
    return yield* store.reserve({
      linkId: `link:${suffix}`,
      observerThreadId: observer,
      observerProjectId: observerProject,
      targetThreadId: id,
      targetProjectId: destination,
      clientRequestId: `retry:${suffix}`,
      fingerprint: "input",
      launchCommandId: CommandId.make(`launch:${suffix}`),
      launchMessageId: MessageId.make(`initial:${suffix}`),
      launchInputJson: "{}",
    });
  });
  yield* reserve();
  yield* write([{ type: "thread.created", threadId: target, payload: makeThread(target) }]);
  const start = Effect.fn("test.wakeFacts.start")(function* (
    run: OrchestrationV2Run,
    messageOverrides: Partial<OrchestrationV2ConversationMessage> = {},
    effects: ReadonlyArray<Outbox.PendingOrchestrationEffectV2> = [],
  ) {
    return yield* write(
      [
        {
          type: "message.updated",
          threadId: run.threadId,
          payload: makeMessage(run, messageOverrides),
        },
        { type: "run.created", threadId: run.threadId, payload: run },
      ],
      effects,
      "message.dispatch",
    );
  });
  const finish = (
    run: OrchestrationV2Run,
    status: OrchestrationV2Run["status"] = "completed",
    effects: ReadonlyArray<Outbox.PendingOrchestrationEffectV2> = [],
  ) =>
    write(
      [
        {
          type: "run.updated",
          threadId: run.threadId,
          payload: { ...run, status, completedAt: now },
        },
      ],
      effects,
    );
  const question = Effect.fn("test.wakeFacts.question")(function* (
    run: OrchestrationV2Run,
    id: string,
    response: "live" | "message" = "live",
    kind: "user_input" | "permission" = "user_input",
  ) {
    const nodeId = NodeId.make(`node:${id}`);
    const request: OrchestrationV2RuntimeRequest = {
      id: RuntimeRequestId.make(id),
      nodeId,
      providerTurnId: null,
      nativeRequestRef: null,
      kind,
      status: "pending",
      responseCapability:
        response === "live"
          ? { type: "live", providerSessionId: ProviderSessionId.make("session") }
          : { type: "message" },
      createdAt: now,
      resolvedAt: null,
    };
    yield* write([
      {
        type: "node.updated",
        threadId: run.threadId,
        payload: {
          id: nodeId,
          threadId: run.threadId,
          runId: run.id,
          parentNodeId: null,
          rootNodeId: nodeId,
          kind: "user_input_request",
          status: "waiting",
          countsForRun: true,
          providerThreadId: providerId,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: request.id,
          checkpointScopeId: null,
          startedAt: now,
          completedAt: null,
        },
      },
      { type: "runtime-request.updated", threadId: run.threadId, payload: request },
    ]);
    return request;
  });
  return { sql, sink, store, facts, write, reserve, start, finish, question };
});

it.effect("records every explicit work identity in source order before launch acceptance", () =>
  Effect.gen(function* () {
    const { start, finish, facts, store } = yield* setup();
    const a = makeRun(1, { status: "preparing", startedAt: null, providerThreadId: null });
    const first = yield* start(a, { createdBy: "agent", creationSource: "mcp" });
    yield* finish(a, "failed");
    const b = makeRun(2);
    yield* start(b);
    yield* finish(b);
    const rows = yield* occurrences;
    assert.deepEqual(
      rows.map((row) => [row.source_id, row.ready, row.disposition]),
      [
        [a.id, 1, "pending"],
        [b.id, 1, "pending"],
      ],
    );
    assert.equal(rows[0]!.source_sequence, first[1]!.sequence);
    assert.equal((yield* store.reservationFor(observer, "retry:1"))!.launch_accepted, 0);
    assert.equal(yield* facts.completionOutcome("link:1", a.id), "failed");
    assert.equal(yield* facts.completionOutcome("link:1", b.id), "completed");
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("collapses restart chains and waits for an eventless declined continuation", () =>
  Effect.gen(function* () {
    const { start, finish, facts } = yield* setup();
    const outbox = yield* Outbox.EffectOutboxV2;
    const a = makeRun(1);
    yield* start(a);
    const continuation = (run: OrchestrationV2Run): Outbox.PendingOrchestrationEffectV2 => ({
      id: `effect:restart-continuation:${run.id}`,
      commandId: CommandId.make(`continue:${run.id}`),
      threadId: target,
      request: { type: "provider-runtime.continue", sourceRunId: run.id },
    });
    yield* finish(a, "cancelled", [continuation(a)]);
    assert.equal((yield* occurrences)[0]!.ready, 0);
    const b = makeRun(2, { restartContinuationOfRunId: a.id });
    yield* start(b, {
      createdBy: "agent",
      creationSource: "server",
      notification: { source: { kind: "system" }, outcome: "updated", summary: "Continuation" },
    });
    const claimed = yield* outbox.claimNext({ workerId: "test", leaseDurationMs: 1000 });
    assert.isTrue(Option.isSome(claimed));
    yield* outbox.succeed({ effectId: continuation(a).id, workerId: "test" });
    yield* finish(b, "cancelled", [continuation(b)]);
    assert.equal((yield* occurrences).length, 1);
    const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
      execute: () => Effect.void,
    });
    const worker = yield* Worker.OrchestrationEffectWorkerV2.pipe(
      Effect.provide(Worker.layer.pipe(Layer.provide(executor))),
    );
    assert.equal(yield* worker.drain(), 1);
    assert.equal((yield* occurrences)[0]!.ready, 1);
    assert.equal(yield* facts.completionOutcome("link:1", a.id), "cancelled");
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("settles after a continuation fails its last execution, without another run event", () =>
  Effect.gen(function* () {
    const { start, finish } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    yield* finish(run, "cancelled", [
      {
        id: `effect:restart-continuation:${run.id}`,
        commandId: CommandId.make("continue"),
        threadId: target,
        request: { type: "provider-runtime.continue", sourceRunId: run.id },
      },
    ]);
    const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
      execute: () =>
        Effect.fail(
          new Worker.OrchestrationEffectExecutionError({
            effectId: "continue",
            effectType: "provider-runtime.continue",
            cause: "fixture",
          }),
        ),
    });
    const worker = yield* Worker.OrchestrationEffectWorkerV2.pipe(
      Effect.provide(Worker.layerWithOptions({ maxAttempts: 1 }).pipe(Layer.provide(executor))),
    );
    yield* worker.drain();
    assert.equal((yield* occurrences)[0]!.ready, 1);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("automatic wake, maintenance and their restart descendants never become sources", () =>
  Effect.gen(function* () {
    const { start, finish, question } = yield* setup();
    const automatic = makeRun(1);
    yield* start(automatic, {
      createdBy: "agent",
      creationSource: "server",
      notification: { source: { kind: "background_task" }, outcome: "updated", summary: "Notice" },
    });
    yield* question(automatic, "automatic-question");
    yield* finish(automatic, "cancelled");
    const descendant = makeRun(2, { restartContinuationOfRunId: automatic.id });
    yield* start(descendant, { createdBy: "agent", creationSource: "server" });
    yield* question(descendant, "descendant-question", "message");
    yield* finish(descendant);
    const maintenance = makeRun(3);
    yield* start(maintenance, { text: "/compact" });
    yield* finish(maintenance);
    assert.deepEqual(yield* occurrences, []);
    const work = makeRun(4);
    yield* start(work);
    yield* finish(work);
    assert.deepEqual(
      (yield* occurrences).map((row) => row.source_id),
      [work.id],
    );
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "keeps live and message questions independently, excluding approvals and withdrawing dead requests",
  () =>
    Effect.gen(function* () {
      const { start, question, write, store } = yield* setup();
      const a = makeRun(1);
      yield* start(a);
      const live = yield* question(a, "live");
      const message = yield* question(a, "message", "message");
      yield* question(a, "approval", "live", "permission");
      assert.deepEqual(
        (yield* occurrences).filter((row) => row.kind === "question").map((row) => row.source_id),
        ["live", "message"],
      );
      const box = (yield* store.mailbox(observer))!;
      const batch = yield* store.stageBatch({
        observerThreadId: observer,
        epoch: box.epoch,
        revision: box.revision,
      });
      assert.equal(batch.state, "ready");
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          payload: { ...live, status: "resolved", resolvedAt: now },
        },
        {
          type: "runtime-request.updated",
          threadId: target,
          payload: {
            ...message,
            status: "expired",
            responseCapability: { type: "not_resumable", reason: "Fixture restart" },
            resolvedAt: now,
          },
        },
      ]);
      assert.deepEqual(
        (yield* occurrences).filter((row) => row.kind === "question").map((row) => row.disposition),
        ["withdrawn", "withdrawn"],
      );
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("uses the question node's run, never the latest explicit work", () =>
  Effect.gen(function* () {
    const { start, question } = yield* setup();
    const automatic = makeRun(1);
    yield* start(automatic, {
      createdBy: "agent",
      creationSource: "server",
      notification: { source: { kind: "system" }, outcome: "updated", summary: "Notice" },
    });
    yield* start(makeRun(2));
    yield* question(automatic, "late-automatic");
    assert.equal((yield* occurrences).filter((row) => row.kind === "question").length, 0);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "holds completion for owned roster work without assigning it to a later explicit turn",
  () =>
    Effect.gen(function* () {
      const { start, finish, write } = yield* setup();
      const a = makeRun(1);
      yield* start(a);
      yield* write([
        {
          type: "provider-thread.updated",
          threadId: target,
          payload: makeProvider(1, [{ taskId: "monitor", kind: "monitor" }]),
        },
      ]);
      yield* finish(a);
      assert.equal((yield* occurrences)[0]!.ready, 0);
      const b = makeRun(2);
      yield* start(b);
      yield* write([
        {
          type: "provider-thread.updated",
          threadId: target,
          payload: makeProvider(2, [{ taskId: "monitor", kind: "monitor" }]),
        },
      ]);
      yield* finish(b);
      assert.deepEqual(
        (yield* occurrences).map((row) => row.ready),
        [0, 1],
      );
      yield* write([
        { type: "provider-thread.updated", threadId: target, payload: makeProvider(2) },
      ]);
      assert.deepEqual(
        (yield* occurrences).map((row) => row.ready),
        [1, 1],
      );
      const reaction = makeRun(3);
      yield* start(reaction, {
        createdBy: "agent",
        creationSource: "provider",
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Background reaction",
        },
      });
      assert.deepEqual(
        (yield* occurrences).map((row) => row.ready),
        [1, 1],
      );
      yield* finish(reaction);
      const c = makeRun(4, { providerThreadId: null });
      yield* start(c);
      yield* write([
        {
          type: "provider-thread.updated",
          threadId: target,
          payload: makeProvider(4, [{ taskId: "monitor", kind: "monitor" }]),
        },
      ]);
      yield* finish(c);
      assert.deepEqual(
        (yield* occurrences).map((row) => row.ready),
        [1, 1, 0],
      );
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("waiting is not completion; owned background tool work settles only when ended", () =>
  Effect.gen(function* () {
    const { start, finish, write } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    yield* finish(run, "waiting");
    assert.equal((yield* occurrences)[0]!.ready, 0);
    const item = {
      id: TurnItemId.make("background"),
      threadId: target,
      runId: run.id,
      nodeId: null,
      providerThreadId: providerId,
      providerTurnId: null,
      parentItemId: null,
      nativeItemRef: null,
      ordinal: 1,
      type: "dynamic_tool" as const,
      status: "running" as const,
      title: null,
      toolName: "background",
      input: {},
      output: null,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
    };
    yield* write([{ type: "turn-item.updated", threadId: target, payload: item }]);
    yield* finish(run);
    assert.equal((yield* occurrences)[0]!.ready, 0);
    yield* write([
      {
        type: "turn-item.updated",
        threadId: target,
        payload: { ...item, status: "completed", completedAt: now },
      },
    ]);
    assert.equal((yield* occurrences)[0]!.ready, 1);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "archive retains sources, unarchive catches up, and endpoint deletion closes without stopping its peer",
  () =>
    Effect.gen(function* () {
      const { start, finish, write, store, sql } = yield* setup();
      const run = makeRun(1);
      yield* start(run);
      yield* write([
        {
          type: "thread.archived",
          threadId: target,
          payload: { ...makeThread(target), archivedAt: now },
        },
      ]);
      yield* finish(run);
      assert.equal((yield* occurrences)[0]!.disposition, "pending");
      const box = (yield* store.mailbox(observer))!;
      assert.equal(
        (yield* store.stageBatch({
          observerThreadId: observer,
          epoch: box.epoch,
          revision: box.revision,
        })).state,
        "blocked",
      );
      yield* write([{ type: "thread.unarchived", threadId: target, payload: makeThread(target) }]);
      const resumed = (yield* store.mailbox(observer))!;
      assert.isAbove(resumed.revision, box.revision);
      assert.equal(
        (yield* store.stageBatch({
          observerThreadId: observer,
          epoch: resumed.epoch,
          revision: resumed.revision,
        })).state,
        "ready",
      );
      yield* write([
        {
          type: "thread.deleted",
          threadId: observer,
          payload: { ...makeThread(observer, observerProject), deletedAt: now },
        },
      ]);
      assert.equal((yield* store.reservationFor(observer, "retry:1"))!.state, "closed");
      assert.equal((yield* sql`SELECT run_id FROM overseer_work_runs`).length, 0);
      assert.equal(
        (yield* (yield* Projections.ProjectionStoreV2).getThread(target)).deletedAt,
        null,
      );
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "project-only deletion closes reservation-only targets in the same commit and receipt retry is inert",
  () =>
    Effect.gen(function* () {
      const { reserve, sink, store } = yield* setup();
      yield* reserve(ThreadId.make("not-created"), "reserved");
      const commandId = CommandId.make("delete-project");
      const input = {
        commandId,
        projectId,
        commandType: "project.delete",
        acceptedAt: now,
        event: {
          type: "project.deleted" as const,
          eventId: EventId.make("project-delete"),
          aggregateKind: "project" as const,
          aggregateId: projectId,
          occurredAt: DateTime.formatIso(now),
          commandId,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          payload: { projectId, deletedAt: DateTime.formatIso(now) },
        },
      };
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER fail_close BEFORE UPDATE ON overseer_links WHEN NEW.close_reason = 'project_deleted' BEGIN SELECT RAISE(ABORT, 'fixture'); END`;
      assert.isTrue(
        Option.isSome(yield* sink.commitProjectCommand(input).pipe(Effect.flip, Effect.option)),
      );
      assert.equal((yield* store.reservationFor(observer, "retry:reserved"))!.state, "active");
      assert.equal(
        (yield* sql<{
          readonly deleted_at: string | null;
        }>`SELECT deleted_at FROM projection_projects WHERE project_id = ${projectId}`)[0]!
          .deleted_at,
        null,
      );
      yield* sql`DROP TRIGGER fail_close`;
      assert.isTrue((yield* sink.commitProjectCommand(input)).committed);
      const before = yield* store.mailbox(observer);
      assert.isFalse((yield* sink.commitProjectCommand(input)).committed);
      assert.deepEqual(yield* store.mailbox(observer), before);
      assert.equal(
        (yield* store.reservationFor(observer, "retry:reserved"))!.close_reason,
        "project_deleted",
      );
      assert.equal((yield* store.reservationFor(observer, "retry:1"))!.state, "closed");
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "source failure rolls back core events, receipt, outbox and fact; notification happens only after commit",
  () =>
    Effect.gen(function* () {
      const { sql, start, finish, sink, facts } = yield* setup();
      const run = makeRun(1);
      yield* sql`CREATE TRIGGER fail_occurrence BEFORE INSERT ON overseer_occurrences BEGIN SELECT RAISE(ABORT, 'fixture'); END`;
      assert.isTrue(
        Option.isSome(
          yield* start(run, {}, [
            {
              id: "provider-start",
              commandId: CommandId.make("start"),
              threadId: target,
              request: { type: "provider-turn.start", runId: run.id },
            },
          ]).pipe(Effect.flip, Effect.option),
        ),
      );
      assert.equal((yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`).length, 0);
      assert.deepEqual(yield* occurrences, []);
      assert.equal((yield* sql`SELECT run_id FROM orchestration_v2_projection_runs`).length, 0);
      assert.equal(
        (yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type = 'message.dispatch'`)
          .length,
        0,
      );
      yield* sql`DROP TRIGGER fail_occurrence`;
      yield* start(run);
      yield* facts.awaitAvailable;
      const notified = yield* Deferred.make<void>();
      const waiter = yield* facts.awaitAvailable.pipe(
        Effect.andThen(Deferred.succeed(notified, undefined)),
        Effect.forkChild,
      );
      yield* finish(run);
      yield* Deferred.await(notified);
      assert.equal((yield* occurrences)[0]!.ready, 1);
      yield* Fiber.join(waiter);
      const latest = yield* sink.latestSequence();
      assert.isAbove(latest, 0);
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("rebuild, replay and repeated passive reads cannot record or settle facts", () =>
  Effect.gen(function* () {
    const { start, finish, facts, store, sink } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    yield* finish(run);
    const before = yield* occurrences;
    const mailbox = yield* store.mailbox(observer);
    const maintenance = yield* Maintenance.ProjectionMaintenanceV2;
    yield* maintenance.rebuild;
    for (let i = 0; i < 3; i++) {
      yield* store.linksForEndpoint(observer);
      yield* store.mailbox(observer);
      yield* facts.completionOutcome("link:1", run.id);
      yield* sink
        .stream({ afterSequence: 0, threadId: target })
        .pipe(Stream.take(1), Stream.runCollect);
    }
    assert.deepEqual(yield* occurrences, before);
    assert.deepEqual(yield* store.mailbox(observer), mailbox);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("guarded writers record only admitted events and preserve attempt ownership", () =>
  Effect.gen(function* () {
    const { start, sink, write, store } = yield* setup();
    const run = makeRun(1, { activeAttemptId: RunAttemptId.make("attempt:1") });
    yield* start(run);
    const terminal: OrchestrationV2DomainEvent = {
      id: EventId.make("guarded-terminal"),
      type: "run.updated",
      threadId: target,
      occurredAt: now,
      payload: { ...run, status: "completed", completedAt: now },
    };
    const guard = {
      threadId: target,
      runId: run.id,
      activeAttemptId: RunAttemptId.make("wrong"),
      expectedStatus: "running" as const,
      events: [terminal],
    };
    assert.isFalse((yield* sink.writeIfRunCurrent(guard)).committed);
    assert.equal((yield* occurrences)[0]!.ready, 0);
    assert.isTrue(
      (yield* sink.writeIfRunCurrent({ ...guard, activeAttemptId: run.activeAttemptId! }))
        .committed,
    );
    assert.equal((yield* occurrences)[0]!.ready, 1);
    const box = (yield* store.mailbox(observer))!;
    const batch = yield* store.stageBatch({
      observerThreadId: observer,
      epoch: box.epoch,
      revision: box.revision,
    });
    assert.equal(batch.state, "ready");
    if (batch.state !== "ready") return;
    yield* store.freezeAttempt({
      observerThreadId: observer,
      epoch: batch.batch.epoch,
      revision: batch.batch.revision,
      batchNumber: batch.batch.batch_number,
      commandId: CommandId.make("late-roster-batch"),
      messageId: MessageId.make("late-roster-batch"),
    });
    const provider = makeProvider(1, [{ taskId: "monitor", kind: "monitor" }]);
    yield* write([{ type: "provider-thread.updated", threadId: target, payload: provider }]);
    assert.equal((yield* occurrences)[0]!.ready, 0);
    assert.isUndefined(yield* store.pendingAttempt(observer));
    const clear: OrchestrationV2DomainEvent = {
      id: EventId.make("guarded-clear"),
      type: "provider-thread.updated",
      threadId: target,
      occurredAt: now,
      payload: makeProvider(1),
    };
    assert.isFalse(
      (yield* sink.writeIfProviderThreadOwner({
        providerThreadId: providerId,
        runId: run.id,
        activeAttemptId: RunAttemptId.make("wrong"),
        expectedLastRunOrdinal: 1,
        events: [clear],
      })).committed,
    );
    assert.equal((yield* occurrences)[0]!.ready, 0);
    assert.isTrue(
      (yield* sink.writeIfProviderThreadOwner({
        providerThreadId: providerId,
        runId: run.id,
        activeAttemptId: run.activeAttemptId!,
        expectedLastRunOrdinal: 1,
        events: [clear],
      })).committed,
    );
    assert.equal((yield* occurrences)[0]!.ready, 1);
    assert.isFalse(
      (yield* sink.writeIfProviderThreadUnchanged({
        expected: provider,
        events: [{ ...clear, id: EventId.make("stale-provider") }],
      })).committed,
    );
    const current = (yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(target, [
      "providerThreads",
    ])).providerThreads[0]!;
    assert.isTrue(
      (yield* sink.writeIfProviderThreadUnchanged({
        expected: current,
        events: [{ ...clear, id: EventId.make("unchanged-provider") }],
      })).committed,
    );
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "Stop holds only the observer mailbox until explicit work or queue resume, never automatic input",
  () =>
    Effect.gen(function* () {
      const { write, store, start } = yield* setup();
      yield* write(
        [
          {
            type: "thread.metadata-updated",
            threadId: observer,
            payload: makeThread(observer, observerProject),
          },
        ],
        [],
        "thread.stop",
      );
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
      const automatic = makeRun(1, {
        threadId: observer,
        id: RunId.make("observer:auto"),
        userMessageId: MessageId.make("observer:auto"),
      });
      yield* start(automatic, {
        createdBy: "agent",
        creationSource: "server",
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Notice",
        },
      });
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
      yield* start(
        makeRun(2, {
          threadId: observer,
          id: RunId.make("observer:work"),
          userMessageId: MessageId.make("observer:work"),
        }),
      );
      assert.equal((yield* store.mailbox(observer))!.hold_reason, null);
      yield* write(
        [
          {
            type: "thread.metadata-updated",
            threadId: observer,
            payload: makeThread(observer, observerProject),
          },
        ],
        [],
        "thread.stop",
      );
      yield* write(
        [
          {
            type: "thread.metadata-updated",
            threadId: observer,
            payload: makeThread(observer, observerProject),
          },
        ],
        [],
        "queue.resume",
      );
      assert.equal((yield* store.mailbox(observer))!.hold_reason, null);
      assert.deepEqual(yield* occurrences, []);
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("unlink rejects late source commits and never stops the worker", () =>
  Effect.gen(function* () {
    const { start, finish, store, sql } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    yield* store.unlink("link:1");
    yield* finish(run);
    yield* start(makeRun(2));
    yield* finish(makeRun(2));
    assert.deepEqual(
      (yield* occurrences).map((row) => row.disposition),
      ["withdrawn"],
    );
    assert.equal(
      (yield* sql`SELECT run_id FROM orchestration_v2_projection_runs WHERE status = 'completed'`)
        .length,
      2,
    );
    assert.equal((yield* sql`SELECT run_id FROM overseer_work_runs`).length, 0);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "the first unstarted run cancelled by typed runtime recovery is interrupted before launch bookkeeping",
  () =>
    Effect.gen(function* () {
      const { start, write, facts, store } = yield* setup();
      const run = makeRun(1, { status: "preparing", startedAt: null, providerThreadId: null });
      yield* start(run);
      yield* write(
        [
          {
            type: "run.updated",
            threadId: target,
            payload: { ...run, status: "cancelled", completedAt: now },
          },
        ],
        [],
        "provider-runtime.reconcile",
      );
      assert.equal((yield* occurrences)[0]!.ready, 1);
      assert.equal((yield* store.reservationFor(observer, "retry:1"))!.launch_accepted, 0);
      assert.equal(yield* facts.completionOutcome("link:1", run.id), "interrupted");
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("a committed fact survives a fresh SQLite scope before in-process publication", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const folder = yield* fs.makeTempDirectoryScoped({ prefix: "wake-fact-crash-" });
    const snapshot = `${folder}/snapshot.sqlite`;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const { start, sink, sql } = yield* setup();
        const run = makeRun(1);
        yield* start(run);
        const events = yield* Events.EventStoreV2;
        const committed = yield* Deferred.make<void, Sink.EventSinkV2Error>();
        const held = yield* Deferred.make<void>();
        const gated = yield* Sink.EventSinkV2.pipe(
          Effect.provide(Layer.fresh(Sink.layerFromStores)),
          Effect.provideService(Events.EventStoreV2, {
            ...events,
            publishCommitted: () =>
              Deferred.succeed(committed, undefined).pipe(Effect.andThen(Deferred.await(held))),
          }),
        );
        const fiber = yield* gated
          .write({
            events: [
              {
                id: EventId.make("terminal-before-publish"),
                type: "run.updated",
                threadId: target,
                occurredAt: now,
                payload: { ...run, status: "completed", completedAt: now },
              },
            ],
          })
          .pipe(
            Effect.onError((cause) => Deferred.failCause(committed, cause)),
            Effect.forkChild,
          );
        yield* Deferred.await(committed);
        yield* sql.unsafe(`VACUUM INTO '${snapshot.replaceAll("'", "''")}'`);
        assert.isAbove(yield* sink.latestSequence(), 0);
        yield* Deferred.succeed(held, undefined);
        yield* Fiber.join(fiber);
      }).pipe(enabled, Effect.provide(layerTest)),
    );
    yield* Effect.gen(function* () {
      assert.deepEqual(
        (yield* occurrences).map((row) => [row.source_id, row.ready]),
        [["run:1", 1]],
      );
      const store = yield* Store.OverseerStore;
      assert.deepEqual(yield* store.recoveryMailboxes(), [observer]);
    }).pipe(Effect.provide(layers(Sqlite.layerFromPath(snapshot))));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "source hooks skip token traffic and use indexed feature/source lookups with unrelated threads",
  () =>
    Effect.gen(function* () {
      const { sql, write, start, finish, question } = yield* setup();
      const threads = Array.from({ length: 300 }, (_, i) =>
        makeThread(ThreadId.make(`unrelated:${i}`)),
      );
      yield* write(
        threads.map((thread) => ({ type: "thread.created", threadId: thread.id, payload: thread })),
      );
      const queries: Array<readonly [string, ReadonlyArray<unknown>]> = [];
      const capture: Statement.Transformer = (statement) => {
        queries.push(statement.compile());
        return Effect.succeed(statement);
      };
      const run = makeRun(1);
      yield* Effect.gen(function* () {
        yield* start(run);
        yield* question(run, "indexed-question");
        yield* write([
          {
            type: "provider-thread.updated",
            threadId: target,
            payload: makeProvider(1, [{ taskId: "background", kind: "background_task" }]),
          },
        ]);
        yield* finish(run);
      }).pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      const plans: Array<string> = [];
      for (const [query, parameters] of queries) {
        if (
          !query.includes("overseer_") &&
          !query.includes("AS persistent") &&
          !query.includes("completionDelivery.state")
        )
          continue;
        const rows = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${query}`,
          parameters,
        );
        const plan = rows.map((row) => row.detail).join("\n");
        assert.notMatch(
          plan,
          /SCAN (?:overseer_|orchestration_v2_projection_|w\b|r\b|p\b|owner\b)/,
        );
        if (query.includes("source_id >")) assert.notInclude(plan, "TEMP B-TREE");
        plans.push(plan);
      }
      assert.isTrue(plans.some((plan) => plan.includes("overseer_work_runs_root_idx")));
      assert.isTrue(
        plans.some((plan) => plan.includes("overseer_occurrences_link_candidates_idx")),
      );
      const tokenQueries: Array<string> = [];
      const captureTokens: Statement.Transformer = (statement) => {
        tokenQueries.push(statement.compile()[0]);
        return Effect.succeed(statement);
      };
      yield* Effect.gen(function* () {
        for (let i = 0; i < 100; i++)
          yield* write([
            {
              type: "message.updated",
              threadId: target,
              payload: {
                ...makeMessage(run),
                id: MessageId.make("output"),
                role: "assistant",
                text: `chunk ${i}`,
                streaming: true,
              },
            },
          ]);
      }).pipe(Effect.provideService(Statement.CurrentTransformer, captureTokens));
      assert.isFalse(tokenQueries.some((query) => query.includes("overseer_")));
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("a worker's own delegated result delivery holds its work", () =>
  Effect.gen(function* () {
    const { start, finish, write } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    const task = {
      id: NodeId.make("delegate"),
      threadId: target,
      runId: run.id,
      parentNodeId: NodeId.make("parent"),
      origin: "app_owned" as const,
      createdBy: "agent" as const,
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: selection.instanceId,
      providerThreadId: providerId,
      childThreadId: ThreadId.make("ordinary-child"),
      nativeTaskRef: null,
      prompt: "Work",
      title: null,
      model: null,
      status: "completed" as const,
      result: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      completionDelivery: { state: "pending" as const, observedByRunId: null },
    };
    yield* write([{ type: "subagent.updated", threadId: target, payload: task }]);
    yield* finish(run);
    assert.equal((yield* occurrences)[0]!.ready, 0);
    yield* write([
      {
        type: "subagent.updated",
        threadId: target,
        payload: { ...task, completionDelivery: { state: "delivered", observedByRunId: null } },
      },
    ]);
    assert.equal((yield* occurrences)[0]!.ready, 1);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect.each(["thread.stop", "run.interrupt", "provider.switch"])(
  "%s settles a source after cancelling its continuation in the same commit",
  (commandType) =>
    Effect.gen(function* () {
      const { start, finish, sink } = yield* setup();
      const run = makeRun(1);
      yield* start(run);
      yield* finish(run, "cancelled", [
        {
          id: `effect:restart-continuation:${run.id}`,
          commandId: CommandId.make("continue"),
          threadId: target,
          request: { type: "provider-runtime.continue", sourceRunId: run.id },
        },
      ]);
      assert.equal((yield* occurrences)[0]!.ready, 0);
      yield* sink.commitCommand({
        commandId: CommandId.make("stop-worker"),
        threadId: target,
        commandType,
        acceptedAt: now,
        events: [
          {
            id: EventId.make("stop-worker"),
            type: "thread.metadata-updated",
            threadId: target,
            occurredAt: now,
            payload: makeThread(target),
          },
        ],
        effects: [],
        cancelUnsettledEffects: {
          effectTypes: ["provider-runtime.continue"],
          reason: "Fixture Stop",
        },
      });
      assert.equal((yield* occurrences)[0]!.ready, 1);
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect.each(["candidate", "queued", "frozen"] as const)(
  "rollback withdraws a pending %s completion",
  (phase) =>
    Effect.gen(function* () {
      const { start, finish, store } = yield* setup();
      const run = makeRun(1);
      yield* start(run);
      if (phase !== "candidate") {
        yield* finish(run);
        const box = (yield* store.mailbox(observer))!;
        const result = yield* store.stageBatch({
          observerThreadId: observer,
          epoch: box.epoch,
          revision: box.revision,
        });
        assert.equal(result.state, "ready");
        if (result.state === "ready" && phase === "frozen")
          yield* store.freezeAttempt({
            observerThreadId: observer,
            epoch: result.batch.epoch,
            revision: result.batch.revision,
            batchNumber: result.batch.batch_number,
            commandId: CommandId.make("rollback-batch"),
            messageId: MessageId.make("rollback-batch"),
          });
      }
      yield* finish(run, "rolled_back");
      assert.equal((yield* occurrences)[0]!.disposition, "withdrawn");
      assert.isUndefined(yield* store.pendingAttempt(observer));
      const box = (yield* store.mailbox(observer))!;
      assert.equal(
        (yield* store.stageBatch({
          observerThreadId: observer,
          epoch: box.epoch,
          revision: box.revision,
        })).state,
        "idle",
      );
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "an excluded turn's monitor cannot become later explicit work's background obligation",
  () =>
    Effect.gen(function* () {
      const { start, finish, write, sql } = yield* setup();
      const automatic = makeRun(1);
      yield* start(automatic, {
        createdBy: "agent",
        creationSource: "server",
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Notice",
        },
      });
      yield* write([
        {
          type: "provider-thread.updated",
          threadId: target,
          payload: makeProvider(1, [{ taskId: "automatic-monitor", kind: "monitor" }]),
        },
      ]);
      yield* finish(automatic);
      const work = makeRun(2);
      yield* start(work);
      yield* write([
        {
          type: "provider-thread.updated",
          threadId: target,
          payload: makeProvider(2, [{ taskId: "automatic-monitor", kind: "monitor" }]),
        },
      ]);
      yield* finish(work);
      assert.deepEqual(
        (yield* occurrences).map((row) => [row.source_id, row.ready]),
        [[work.id, 1]],
      );
      assert.deepEqual(yield* sql`SELECT root_run_id FROM overseer_background_owners`, [
        { root_run_id: null },
      ]);
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect(
  "authoritative command thread owns Stop and every explicit queue/message rearm signals after commit",
  () =>
    Effect.gen(function* () {
      const { sink, facts, store, write } = yield* setup();
      yield* sink.commitCommand({
        commandId: CommandId.make("multi-thread-stop"),
        threadId: observer,
        commandType: "thread.stop",
        acceptedAt: now,
        events: [
          {
            id: EventId.make("stop-peer-metadata"),
            threadId: target,
            type: "thread.metadata-updated",
            occurredAt: now,
            payload: makeThread(target),
          },
        ],
        effects: [],
      });
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
      yield* facts.awaitAvailable;
      // A queued input may have a message event without a new run in this commit.
      const input = makeRun(1, { threadId: observer });
      yield* write(
        [{ type: "message.updated", threadId: observer, payload: makeMessage(input) }],
        [],
        "message.dispatch",
      );
      yield* facts.awaitAvailable;
      assert.equal((yield* store.mailbox(observer))!.hold_reason, null);
      for (const commandType of ["queued-run.start", "queue.resume"]) {
        yield* write(
          [
            {
              type: "thread.metadata-updated",
              threadId: observer,
              payload: makeThread(observer, observerProject),
            },
          ],
          [],
          "thread.stop",
        );
        yield* facts.awaitAvailable;
        yield* write(
          [
            {
              type: "thread.metadata-updated",
              threadId: observer,
              payload: makeThread(observer, observerProject),
            },
          ],
          [],
          commandType,
        );
        yield* facts.awaitAvailable;
        assert.equal((yield* store.mailbox(observer))!.hold_reason, null);
      }
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect.each(["succeed", "fail"] as const)(
  "a post-%s reconciliation failure does not rewrite or retry a settled continuation",
  (result) =>
    Effect.gen(function* () {
      const { start, finish, sql, facts } = yield* setup();
      const run = makeRun(1);
      const effectId = `effect:restart-continuation:${run.id}`;
      yield* start(run);
      yield* finish(run, "cancelled", [
        {
          id: effectId,
          commandId: CommandId.make("continue"),
          threadId: target,
          request: { type: "provider-runtime.continue", sourceRunId: run.id },
        },
      ]);
      yield* sql`CREATE TRIGGER fail_readiness BEFORE UPDATE ON overseer_occurrences WHEN NEW.ready = 1 BEGIN SELECT RAISE(ABORT, 'fixture'); END`;
      const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
        execute: () =>
          result === "succeed"
            ? Effect.void
            : Effect.fail(
                new Worker.OrchestrationEffectExecutionError({
                  effectId,
                  effectType: "provider-runtime.continue",
                  cause: "fixture",
                }),
              ),
      });
      const worker = yield* Worker.OrchestrationEffectWorkerV2.pipe(
        Effect.provide(Worker.layerWithOptions({ maxAttempts: 1 }).pipe(Layer.provide(executor))),
      );
      assert.equal(yield* worker.drain(), 1);
      const outbox = yield* Outbox.EffectOutboxV2;
      const row = yield* outbox.get(effectId);
      assert.isTrue(Option.isSome(row));
      if (Option.isSome(row))
        assert.equal(row.value.status, result === "succeed" ? "succeeded" : "failed");
      assert.equal((yield* occurrences)[0]!.ready, 0);
      yield* sql`DROP TRIGGER fail_readiness`;
      yield* facts.continuationSettled(run.id);
      assert.equal((yield* occurrences)[0]!.ready, 1);
    }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("persistent tools and command roster entries do not hold settled work", () =>
  Effect.gen(function* () {
    const { start, finish, write } = yield* setup();
    const run = makeRun(1);
    yield* start(run);
    yield* write([
      {
        type: "provider-thread.updated",
        threadId: target,
        payload: makeProvider(1, [{ taskId: "shell", kind: "command" }]),
      },
      {
        type: "turn-item.updated",
        threadId: target,
        payload: {
          id: TurnItemId.make("persistent"),
          threadId: target,
          runId: run.id,
          nodeId: null,
          providerThreadId: providerId,
          providerTurnId: null,
          parentItemId: null,
          nativeItemRef: null,
          ordinal: 1,
          type: "dynamic_tool",
          status: "running",
          title: null,
          toolName: "dev-server",
          input: { persistent: true },
          output: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        },
      },
    ]);
    yield* finish(run);
    assert.equal((yield* occurrences)[0]!.ready, 1);
  }).pipe(enabled, Effect.provide(layerTest)),
);

it.effect("disabled hooks perform no feature SQL even for linked source events", () =>
  Effect.gen(function* () {
    const { start, finish } = yield* enabled(setup());
    const queries: Array<string> = [];
    const transformer: Statement.Transformer = (statement) => {
      queries.push(statement.compile()[0]);
      return Effect.succeed(statement);
    };
    yield* Effect.gen(function* () {
      const run = makeRun(1);
      yield* start(run);
      yield* finish(run);
    }).pipe(Effect.provideService(Statement.CurrentTransformer, transformer));
    assert.deepEqual(yield* occurrences, []);
    assert.isFalse(queries.some((query) => query.includes("overseer_")));
  }).pipe(Effect.provide(layerTest)),
);

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProviderSessionId,
  RuntimeRequestId,
  ProjectId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Overseer from "./OverseerStore.ts";

const database = Sqlite.layerMemory;
const stores = Layer.mergeAll(Overseer.layer, EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(database),
);
const layerTest = ProjectionMaintenance.layer.pipe(Layer.provideMerge(stores));
const observer = ThreadId.make("observer");
const observerProject = ProjectId.make("project-observer");
const targetProject = ProjectId.make("project-target");
const timestamp = "2026-10-21T00:00:00.000Z";

const reservation = (suffix = "1") => ({
  linkId: `link-${suffix}`,
  observerThreadId: observer,
  observerProjectId: observerProject,
  targetThreadId: ThreadId.make(`target-${suffix}`),
  targetProjectId: targetProject,
  clientRequestId: `retry-${suffix}`,
  fingerprint: `fingerprint-${suffix}`,
  launchCommandId: CommandId.make(`launch-${suffix}`),
  launchMessageId: MessageId.make(`initial-${suffix}`),
  launchInputJson: JSON.stringify({ workspace: "root", model: "test", initialMessage: "work" }),
});

const seedThread = Effect.fn("test.seedThread")(function* (id: ThreadId, project: ProjectId) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_threads (
    thread_id, project_id, title, default_provider, provider_instance_id, runtime_mode, interaction_mode,
    created_at, updated_at, payload_json
  ) VALUES (${id}, ${project}, 'test', 'codex', 'codex', 'full-access', 'default', ${timestamp}, ${timestamp}, '{}')`;
});
const setup = Effect.fn("test.setup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const id of [observerProject, targetProject])
    yield* sql`INSERT INTO projection_projects
    (project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${id}, ${id}, '/test', '[]', ${timestamp}, ${timestamp})`;
  yield* seedThread(observer, observerProject);
  return yield* Overseer.OverseerStore;
});
const launch = Effect.fn("test.launch")(function* (suffix = "1") {
  const store = yield* Overseer.OverseerStore;
  const input = reservation(suffix);
  yield* store.reserve(input);
  yield* seedThread(input.targetThreadId, input.targetProjectId);
  yield* store.acceptLaunch(input.linkId);
  return input;
});
const record = Effect.fn("test.record")(function* (
  linkId: string,
  sourceId: string,
  sourceSequence = 1,
  ready = true,
) {
  return yield* (yield* Overseer.OverseerStore).record({
    linkId,
    kind: "completion",
    sourceId,
    sourceSequence,
    ready,
  });
});
const occurrences = Effect.fn("test.occurrences")(function* (linkId: string) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly source_id: string;
    readonly disposition: string;
    readonly batch_number: number | null;
  }>`
    SELECT * FROM overseer_occurrences WHERE link_id = ${linkId} ORDER BY source_sequence, kind, source_id`;
});
const fence = Effect.fn("test.fence")(function* (id = observer) {
  const box = yield* (yield* Overseer.OverseerStore).mailbox(id);
  assert.isDefined(box);
  return { observerThreadId: id, epoch: box!.epoch, revision: box!.revision };
});
const readyBatch = (
  result: Effect.Success<ReturnType<Overseer.OverseerStore["Service"]["stageBatch"]>>,
) => {
  if (result.state !== "ready") return assert.fail(`Expected ready batch, got ${result.state}`);
  return result.batch;
};
const freeze = Effect.fn("test.freeze")(function* (suffix = "1", id = observer) {
  const store = yield* Overseer.OverseerStore;
  const current = yield* fence(id);
  const batch = readyBatch(yield* store.stageBatch(current));
  const messageId = MessageId.make(`wake-${suffix}`);
  const input = {
    ...current,
    epoch: batch.epoch,
    revision: batch.revision,
    batchNumber: batch.batch_number,
    commandId: CommandId.make(`dispatch-${suffix}`),
    messageId,
  };
  const attempt = yield* store.freezeAttempt(input);
  return { ...input, attempt };
});
const question = Effect.fn("test.question")(function* (
  linkId: string,
  id: string,
  capability: "live" | "message" = "live",
) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Overseer.OverseerStore;
  const link = (yield* sql<{
    readonly target_thread_id: string;
  }>`SELECT target_thread_id FROM overseer_links WHERE link_id = ${linkId}`)[0]!;
  const now = DateTime.makeUnsafe(timestamp);
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  yield* projections.apply({
    id: EventId.make(`request-${id}`),
    type: "runtime-request.updated",
    threadId: ThreadId.make(link.target_thread_id),
    occurredAt: now,
    payload: {
      id: RuntimeRequestId.make(id),
      nodeId: NodeId.make("node"),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input",
      status: "pending",
      responseCapability:
        capability === "live"
          ? { type: "live", providerSessionId: ProviderSessionId.make("session") }
          : { type: "message" },
      createdAt: now,
      resolvedAt: null,
    },
  });
  yield* store.record({ linkId, kind: "question", sourceId: id, sourceSequence: 2, ready: true });
});
const archive = Effect.fn("test.archive")(function* (id: ThreadId, archived: boolean) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Overseer.OverseerStore;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`UPDATE orchestration_v2_projection_threads SET archived_at = ${archived ? timestamp : null} WHERE thread_id = ${id}`;
      yield* archived ? store.endpointArchived(id) : store.endpointUnarchived(id);
    }),
  );
});

it.effect("rolls reservations and source facts back with the caller's core transaction", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const error = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE projection_projects SET title = 'rolled back' WHERE project_id = ${observerProject}`;
          yield* store.reserve(reservation());
          yield* record("link-1", "run-1");
          return yield* Effect.fail("rollback");
        }),
      )
      .pipe(Effect.flip);
    assert.equal(error, "rollback");
    assert.deepEqual(yield* store.incompleteLaunches(), []);
    assert.deepEqual(yield* occurrences("link-1"), []);
    assert.isUndefined(yield* store.mailbox(observer));
    assert.equal(
      (yield* sql<{
        readonly title: string;
      }>`SELECT title FROM projection_projects WHERE project_id = ${observerProject}`)[0]?.title,
      observerProject,
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "concurrent caller retry keys return one immutable reservation and reject changed input",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const gate = yield* Deferred.make<void>();
      const first = reservation();
      const second = {
        ...reservation("2"),
        clientRequestId: first.clientRequestId,
        fingerprint: first.fingerprint,
      };
      const left = yield* Deferred.await(gate).pipe(
        Effect.andThen(store.reserve(first)),
        Effect.forkChild,
      );
      const right = yield* Deferred.await(gate).pipe(
        Effect.andThen(store.reserve(second)),
        Effect.forkChild,
      );
      yield* Deferred.succeed(gate, undefined);
      const [a, b] = yield* Effect.all([Fiber.join(left), Fiber.join(right)]);
      assert.deepEqual(a, b);
      assert.equal((yield* store.incompleteLaunches()).length, 1);
      const mismatch = yield* store
        .reserve({ ...first, fingerprint: "different" })
        .pipe(Effect.flip);
      assert.instanceOf(mismatch, Overseer.OverseerConflict);
      assert.equal(mismatch._tag === "OverseerConflict" && mismatch.reason, "retry_input");
      const otherObserver = ThreadId.make("observer-2");
      yield* seedThread(otherObserver, observerProject);
      const targetCollision = yield* store
        .reserve({
          ...reservation("3"),
          observerThreadId: otherObserver,
          targetThreadId:
            a.target_thread_id === first.targetThreadId
              ? first.targetThreadId
              : second.targetThreadId,
        })
        .pipe(Effect.flip);
      assert.instanceOf(targetCollision, Overseer.OverseerStoreError);
      assert.isUndefined(yield* store.mailbox(otherObserver));
    }).pipe(Effect.provide(layerTest)),
);

it.effect("accepting a launch releases recovery input but retains its retry receipt", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const input = reservation();
    yield* store.reserve(input);
    assert.equal((yield* store.incompleteLaunches())[0]?.launch_input_json, input.launchInputJson);
    assert.isTrue(yield* store.acceptLaunch(input.linkId));
    assert.deepEqual(yield* store.incompleteLaunches(), []);
    const receipt = yield* store.reserve(input);
    assert.equal(receipt.launch_accepted, 1);
    assert.isNull(receipt.launch_input_json);
    assert.equal(receipt.target_thread_id, input.targetThreadId);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("keeps multiple questions per target and candidates until explicit settlement", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const { linkId } = yield* launch();
    yield* question(linkId, "question-live");
    yield* question(linkId, "question-message", "message");
    yield* record(linkId, "root-work", 1, false);
    assert.isFalse(
      yield* store.record({
        linkId,
        kind: "question",
        sourceId: "question-live",
        sourceSequence: 3,
        ready: true,
      }),
    );
    const batch = readyBatch(yield* store.stageBatch(yield* fence()));
    const before = yield* occurrences(linkId);
    assert.equal(before.length, 3);
    assert.equal(before.filter((row) => row.batch_number === batch?.batch_number).length, 2);
    assert.isTrue(yield* store.settleCompletion(linkId, "root-work"));
    assert.isFalse(yield* store.settleCompletion(linkId, "root-work"));
    yield* store.stageBatch(yield* fence());
    assert.deepEqual(
      (yield* occurrences(linkId)).map((row) => row.source_id),
      ["root-work", "question-live", "question-message"],
    );
    assert.equal((yield* occurrences(linkId)).filter((row) => row.batch_number !== null).length, 3);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "freezes at most eight entries and accepts only that membership, not later arrivals",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      for (let index = 0; index < 10; index++) yield* record(linkId, `run-${index}`, index);
      const attempt = yield* freeze();
      assert.deepEqual(yield* store.freezeAttempt(attempt), attempt.attempt);
      assert.equal(
        (yield* occurrences(linkId)).filter((row) => row.batch_number !== null).length,
        8,
      );
      yield* record(linkId, "later", 10);
      assert.equal((yield* store.stageBatch(yield* fence())).state, "busy");
      assert.deepEqual(yield* store.freezeAttempt(attempt), attempt.attempt);
      yield* store.bindAttempt(
        attempt.messageId,
        RunId.make("notification-run"),
        RunAttemptId.make("native-attempt"),
      );
      assert.isFalse(
        yield* store.bindAttempt(
          attempt.messageId,
          RunId.make("different-run"),
          RunAttemptId.make("native-attempt"),
        ),
      );
      assert.equal(
        (yield* store.attemptsForRun(RunId.make("notification-run")))[0]?.message_id,
        attempt.messageId,
      );
      assert.isTrue(yield* store.acceptAttempt(attempt.messageId, attempt.epoch));
      assert.isFalse(yield* store.acceptAttempt(attempt.messageId, attempt.epoch));
      const rows = yield* occurrences(linkId);
      assert.equal(rows.filter((row) => row.disposition === "accepted").length, 8);
      assert.equal(rows.filter((row) => row.disposition === "pending").length, 3);
      assert.isTrue(yield* store.checkpoint(attempt));
      assert.deepEqual(yield* store.recoveryMailboxes(), [observer]);
      const next = yield* freeze("2");
      assert.equal(next.batchNumber, attempt.batchNumber + 1);
      assert.equal(
        (yield* occurrences(linkId)).filter((row) => row.batch_number === next.batchNumber).length,
        3,
      );
      assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
      assert.isTrue(yield* store.checkpoint(next));
      assert.deepEqual(yield* store.recoveryMailboxes(), []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("rolls a frozen admission and its acceptance back atomically", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const { linkId } = yield* launch();
    yield* record(linkId, "run");
    const current = yield* fence();
    const batch = readyBatch(yield* store.stageBatch(current));
    const input = {
      ...current,
      batchNumber: batch!.batch_number,
      commandId: CommandId.make("dispatch"),
      messageId: MessageId.make("message"),
    };
    assert.equal(
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* store.freezeAttempt(input);
            yield* store.acceptAttempt(input.messageId, input.epoch);
            return yield* Effect.fail("rollback");
          }),
        )
        .pipe(Effect.flip),
      "rollback",
    );
    assert.isUndefined(yield* store.attemptForMessage(input.messageId));
    assert.equal((yield* occurrences(linkId))[0]?.disposition, "pending");
    yield* store.freezeAttempt(input);
    assert.isTrue(yield* store.acceptAttempt(input.messageId, input.epoch));
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "process loss retries with new transport identities; explicit cancellation never resurrects facts",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const { linkId } = yield* launch();
      yield* record(linkId, "run");
      const original = yield* freeze();
      assert.isTrue(yield* store.disposeAttempt(original.messageId, "process_loss"));
      assert.isFalse(yield* store.acceptAttempt(original.messageId, original.epoch));
      const retried = yield* freeze("2");
      assert.equal(retried.batchNumber, original.batchNumber);
      assert.equal(retried.attempt.attempt_number, 2);
      assert.isTrue(yield* store.disposeAttempt(retried.messageId, "cancel"));
      yield* sql`DELETE FROM orchestration_v2_effect_outbox`;
      yield* store.pruneClosed;
      assert.isFalse(yield* record(linkId, "run"));
      assert.equal((yield* occurrences(linkId))[0]?.disposition, "suppressed");
      assert.equal((yield* store.stageBatch(yield* fence())).state, "idle");
      assert.equal(
        (yield* store.attemptForMessage(original.messageId))?.disposal_reason,
        "process_loss",
      );
      yield* record(linkId, "next", 2);
      assert.isTrue(
        yield* store.acceptAttempt((yield* freeze("3")).messageId, (yield* fence()).epoch),
      );
      yield* store.pruneClosed;
      assert.isFalse(yield* record(linkId, "next", 2));
      assert.equal((yield* occurrences(linkId))[1]?.disposition, "accepted");
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "Stop advances the epoch, suppresses the current batch, and holds later work until explicit resume",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "cancelled-wake");
      const current = yield* freeze();
      yield* record(linkId, "later", 2);
      yield* store.stop(observer);
      const stopped = yield* store.mailbox(observer);
      assert.equal(stopped?.epoch, current.epoch + 1);
      assert.equal(stopped?.hold_reason, "stop");
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.isFalse(yield* store.checkpoint(current));
      assert.instanceOf(
        yield* store.stageBatch(current).pipe(Effect.flip),
        Overseer.OverseerConflict,
      );
      assert.equal((yield* store.stageBatch(yield* fence())).state, "held");
      yield* store.hold(observer, "provider");
      yield* store.resume(observer, "provider");
      assert.equal((yield* store.mailbox(observer))?.hold_reason, "stop");
      yield* store.resumeExplicitly(observer);
      const resumed = yield* freeze("2");
      yield* store.resumeExplicitly(observer);
      assert.equal((yield* store.mailbox(observer))?.epoch, resumed.epoch);
      assert.isTrue(yield* store.acceptAttempt(resumed.messageId, resumed.epoch));
      assert.deepEqual(
        (yield* occurrences(linkId)).map((row) => row.disposition),
        ["suppressed", "accepted"],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["queue", "snoozed", "provider", "authorization"] as const)(
  "holds %s obligations without accepting or discarding them",
  (reason) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "work");
      const current = yield* freeze();
      yield* store.hold(observer, reason);
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.equal((yield* occurrences(linkId))[0]?.disposition, "pending");
      const box = yield* fence();
      yield* store.checkpoint(box);
      assert.deepEqual(yield* store.recoveryMailboxes(), [observer]);
      yield* store.resume(observer, reason);
      const resumed = yield* freeze("2");
      assert.isTrue(yield* store.acceptAttempt(resumed.messageId, resumed.epoch));
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each([observer, ThreadId.make("target-1")])(
  "archive suspends %s and unarchive catches up only valid questions and completion facts",
  (endpoint) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const { linkId } = yield* launch();
      yield* record(linkId, "completed");
      yield* question(linkId, "live");
      yield* question(linkId, "resolved", "message");
      yield* question(linkId, "unavailable");
      const current = yield* freeze();
      yield* archive(endpoint, true);
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.equal((yield* store.stageBatch(yield* fence())).state, "blocked");
      yield* record(linkId, "while-archived", 3);
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status = 'resolved' WHERE runtime_request_id = 'resolved'`;
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET payload_json = '{"responseCapability":{"type":"not_resumable"}}' WHERE runtime_request_id = 'unavailable'`;
      yield* archive(endpoint, false);
      assert.equal((yield* store.stageBatch(yield* fence())).state, "reconciled");
      const next = yield* freeze("2");
      assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
      const rows = yield* occurrences(linkId);
      assert.deepEqual(
        rows.filter((row) => row.disposition === "accepted").map((row) => row.source_id),
        ["completed", "live", "while-archived"],
      );
      assert.deepEqual(
        rows.filter((row) => row.disposition === "withdrawn").map((row) => row.source_id),
        ["resolved", "unavailable"],
      );
      assert.equal((yield* store.linksForEndpoint(endpoint)).length, 1);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("rechecks request validity at admission and never accepts stale frozen questions", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const { linkId } = yield* launch();
    yield* question(linkId, "stale");
    const current = yield* fence();
    const batch = readyBatch(yield* store.stageBatch(current));
    const input = {
      ...current,
      batchNumber: batch!.batch_number,
      commandId: CommandId.make("dispatch"),
      messageId: MessageId.make("message"),
    };
    yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status = 'resolved' WHERE runtime_request_id = 'stale'`;
    assert.instanceOf(
      yield* store.freezeAttempt(input).pipe(Effect.flip),
      Overseer.OverseerConflict,
    );
    assert.equal((yield* store.stageBatch(current)).state, "reconciled");
    assert.equal((yield* occurrences(linkId))[0]?.disposition, "withdrawn");
    yield* question(linkId, "another");
    const attempt = yield* freeze();
    yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status = 'expired' WHERE runtime_request_id = 'another'`;
    assert.isFalse(yield* store.acceptAttempt(attempt.messageId, attempt.epoch));
    assert.isUndefined(yield* store.pendingAttempt(observer));
    assert.isTrue(yield* store.withdraw({ linkId, kind: "question", sourceId: "another" }));
    assert.isFalse(yield* store.withdraw({ linkId, kind: "question", sourceId: "another" }));
    assert.isFalse(
      yield* store.record({
        linkId,
        kind: "question",
        sourceId: "another",
        sourceSequence: 4,
        ready: true,
      }),
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "unlink closes a lifetime and releases unaffected mixed-batch facts without stopping either thread",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const first = yield* launch();
      const second = yield* launch("2");
      yield* record(first.linkId, "one");
      yield* record(second.linkId, "two", 2);
      const current = yield* freeze();
      assert.isTrue(yield* store.unlink(first.linkId));
      assert.isFalse(yield* store.unlink(first.linkId));
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.equal((yield* occurrences(first.linkId))[0]?.disposition, "withdrawn");
      assert.equal((yield* occurrences(second.linkId))[0]?.disposition, "pending");
      const next = yield* freeze("2");
      assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
      yield* store.pruneClosed;
      assert.deepEqual(yield* occurrences(first.linkId), []);
      assert.isFalse(yield* record(first.linkId, "one"));
      const tombstone = yield* store.reserve(first);
      assert.equal(tombstone.state, "closed");
      assert.isNull(tombstone.launch_input_json);
      assert.isFalse(yield* store.acceptLaunch(first.linkId));
      assert.equal(
        (yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads`).length,
        3,
      );
      assert.equal((yield* store.linksForEndpoint(observer)).length, 1);
      assert.equal((yield* store.attemptForMessage(current.messageId))?.state, "disposed");
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each([observer, ThreadId.make("target-1")])(
  "deletion of %s closes obligations but does not delete the opposite endpoint",
  (endpoint) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const input = yield* launch();
      yield* record(input.linkId, "work");
      const current = yield* freeze();
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE orchestration_v2_projection_threads SET deleted_at = ${timestamp} WHERE thread_id = ${endpoint}`;
          yield* store.endpointDeleted(endpoint);
        }),
      );
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.deepEqual(yield* store.linksForEndpoint(endpoint), []);
      assert.equal((yield* store.reserve(input)).close_reason, "endpoint_deleted");
      const survivor = endpoint === observer ? input.targetThreadId : observer;
      assert.isNull(
        (yield* sql<{
          readonly deleted_at: string | null;
        }>`SELECT deleted_at FROM orchestration_v2_projection_threads WHERE thread_id = ${survivor}`)[0]
          ?.deleted_at,
      );
      if (endpoint === observer) assert.deepEqual(yield* store.recoveryMailboxes(), []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each([observerProject, targetProject])(
  "project-ID cleanup of %s includes reservation-only targets",
  (project) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const launched = yield* launch();
      const reserved = reservation("2");
      yield* store.reserve(reserved);
      yield* record(launched.linkId, "work");
      const current = yield* freeze();
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE projection_projects SET deleted_at = ${timestamp} WHERE project_id = ${project}`;
          yield* store.projectDeleted(project);
        }),
      );
      assert.isFalse(yield* store.acceptAttempt(current.messageId, current.epoch));
      assert.deepEqual(yield* store.incompleteLaunches(), []);
      assert.equal((yield* store.reserve(reserved)).close_reason, "project_deleted");
      assert.isFalse(yield* store.acceptLaunch(reserved.linkId));
      assert.instanceOf(
        yield* store.reserve(reservation("3")).pipe(Effect.flip),
        Overseer.OverseerConflict,
      );
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL`)
          .length,
        2,
      );
      assert.isNull(
        (yield* sql<{
          readonly deleted_at: string | null;
        }>`SELECT deleted_at FROM projection_projects WHERE project_id = ${project === observerProject ? targetProject : observerProject}`)[0]
          ?.deleted_at,
      );
      if (project === observerProject) assert.deepEqual(yield* store.recoveryMailboxes(), []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "reservation and project deletion serialize to a closed obligation, never an active phantom",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const gate = yield* Deferred.make<void>();
      const reserve = yield* Deferred.await(gate).pipe(
        Effect.andThen(store.reserve(reservation())),
        Effect.match({
          onFailure: (error) => {
            assert.instanceOf(error, Overseer.OverseerConflict);
            assert.equal(error._tag === "OverseerConflict" && error.reason, "endpoint_unavailable");
          },
          onSuccess: (row) => assert.equal(row.link_id, "link-1"),
        }),
        Effect.forkChild,
      );
      const deletion = yield* Deferred.await(gate).pipe(
        Effect.andThen(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`UPDATE projection_projects SET deleted_at = ${timestamp} WHERE project_id = ${targetProject}`;
              yield* store.projectDeleted(targetProject);
            }),
          ),
        ),
        Effect.forkChild,
      );
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(reserve);
      yield* Fiber.join(deletion);
      assert.deepEqual(yield* store.incompleteLaunches(), []);
      assert.deepEqual(yield* store.linksForEndpoint(observer), []);
      assert.isFalse(yield* store.acceptLaunch("link-1"));
      assert.instanceOf(
        yield* store.reserve(reservation("2")).pipe(Effect.flip),
        Overseer.OverseerConflict,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "projection rebuild preserves all canonical side records and never recreates disposed obligations",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const input = yield* launch();
      const eventStore = yield* EventStore.EventStoreV2;
      for (const [id, project] of [
        [observer, observerProject],
        [input.targetThreadId, targetProject],
      ] as const) {
        const now = DateTime.makeUnsafe(timestamp);
        const event: OrchestrationV2DomainEvent = {
          id: EventId.make(`created-${id}`),
          type: "thread.created",
          threadId: id,
          occurredAt: now,
          payload: {
            id,
            projectId: project,
            title: "test",
            createdBy: "user",
            creationSource: "web",
            providerInstanceId: ProviderInstanceId.make("codex"),
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        };
        yield* eventStore.append({ events: [event] });
      }
      yield* record(input.linkId, "disposed");
      const current = yield* freeze();
      yield* store.disposeAttempt(current.messageId, "cancel");
      yield* record(input.linkId, "pending", 2);
      yield* store.reserve(reservation("2"));
      const beforeLinks = yield* sql`SELECT * FROM overseer_links ORDER BY link_id`;
      const beforeOccurrences = yield* occurrences(input.linkId);
      const beforeMailbox = yield* store.mailbox(observer);
      const beforeBatches = yield* sql`SELECT * FROM overseer_batches`;
      const beforeAttempts = yield* sql`SELECT * FROM overseer_attempts`;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual(yield* sql`SELECT * FROM overseer_links ORDER BY link_id`, beforeLinks);
      assert.deepEqual(yield* occurrences(input.linkId), beforeOccurrences);
      assert.deepEqual(yield* store.mailbox(observer), beforeMailbox);
      assert.deepEqual(yield* sql`SELECT * FROM overseer_batches`, beforeBatches);
      assert.deepEqual(yield* sql`SELECT * FROM overseer_attempts`, beforeAttempts);
      assert.isFalse(yield* record(input.linkId, "disposed"));
      const next = yield* freeze("2");
      assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "a full SQLite snapshot restores reservations, dispositions and transport provenance with fresh services",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-overseer-backup-" });
      const backup = `${directory}/backup.sqlite`;
      yield* Effect.gen(function* () {
        const store = yield* setup();
        const sql = yield* SqlClient.SqlClient;
        const input = yield* launch();
        yield* record(input.linkId, "work");
        const current = yield* freeze();
        yield* store.bindAttempt(
          current.messageId,
          RunId.make("wake-run"),
          RunAttemptId.make("wake-attempt"),
        );
        yield* store.acceptAttempt(current.messageId, current.epoch);
        yield* store.reserve(reservation("2"));
        yield* store.stop(observer);
        yield* sql`VACUUM INTO ${backup}`;
      }).pipe(Effect.provide(layerTest));
      yield* Effect.gen(function* () {
        const store = yield* Overseer.OverseerStore;
        assert.equal((yield* store.incompleteLaunches())[0]?.target_thread_id, "target-2");
        assert.equal((yield* store.mailbox(observer))?.hold_reason, "stop");
        assert.equal((yield* occurrences("link-1"))[0]?.disposition, "accepted");
        assert.equal((yield* store.attemptsForRun(RunId.make("wake-run")))[0]?.state, "accepted");
        assert.isFalse(yield* record("link-1", "work"));
      }).pipe(
        Effect.provide(Overseer.layer.pipe(Layer.provideMerge(Sqlite.layerFromPath(backup)))),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "pages candidate identities and link reads without changing mailbox or delivery state",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const first = yield* launch("a");
      const second = yield* launch("b");
      yield* record(first.linkId, "same-source", 1, false);
      yield* record(second.linkId, "same-source", 1, false);
      const before = yield* store.mailbox(observer);
      const page = yield* store.pendingCandidates(observer, undefined, 1);
      assert.equal(page.length, 1);
      const row = page[0]!;
      const next = yield* store.pendingCandidates(
        observer,
        { sequence: row.source_sequence, sourceId: row.source_id, linkId: row.link_id },
        1,
      );
      assert.equal(next.length, 1);
      assert.notEqual(next[0]?.link_id, row.link_id);
      const links = yield* store.linksForEndpoint(observer, "", 1);
      assert.equal(links[0]?.link_id, first.linkId);
      assert.equal(
        (yield* store.linksForEndpoint(observer, first.linkId, 1))[0]?.link_id,
        second.linkId,
      );
      assert.deepEqual(yield* store.mailbox(observer), before);
      assert.isTrue(yield* store.settleCompletion(first.linkId, "same-source"));
      assert.deepEqual(
        (yield* store.pendingCandidates(observer)).map((entry) => entry.link_id),
        [second.linkId],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "project deletion rollback preserves reservations, pending facts and the frozen attempt",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const sql = yield* SqlClient.SqlClient;
      const input = yield* launch();
      yield* store.reserve(reservation("2"));
      yield* record(input.linkId, "work");
      const attempt = yield* freeze();
      const before = yield* store.mailbox(observer);
      const failure = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE projection_projects SET deleted_at = ${timestamp} WHERE project_id = ${targetProject}`;
            yield* store.projectDeleted(targetProject);
            return yield* Effect.fail("rollback");
          }),
        )
        .pipe(Effect.flip);
      assert.equal(failure, "rollback");
      assert.deepEqual(yield* store.mailbox(observer), before);
      assert.equal((yield* store.incompleteLaunches()).length, 1);
      assert.equal((yield* store.linksForEndpoint(observer)).length, 2);
      assert.equal((yield* store.attemptForMessage(attempt.messageId))?.state, "pending");
      assert.isTrue(yield* store.acceptAttempt(attempt.messageId, attempt.epoch));
    }).pipe(Effect.provide(layerTest)),
);

it.effect("feature cleanup removes only namespaced data and is idempotent", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const input = yield* launch();
    yield* record(input.linkId, "work");
    yield* freeze();
    yield* sql`INSERT INTO orchestration_v2_effect_outbox (
      effect_id, command_id, thread_id, effect_type, payload_json, status, available_at, created_at, updated_at
    ) VALUES ('ordinary-effect', 'ordinary-command', ${observer}, 'test', '{}', 'pending', ${timestamp}, ${timestamp}, ${timestamp})`;
    const coreBefore =
      yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`;
    yield* store.clearFeatureData;
    yield* store.clearFeatureData;
    for (const table of [
      "overseer_occurrences",
      "overseer_attempts",
      "overseer_batches",
      "overseer_links",
      "overseer_mailboxes",
    ]) {
      assert.deepEqual(yield* sql`SELECT * FROM ${sql(table)}`, []);
    }
    assert.isFalse(yield* record(input.linkId, "work"));
    assert.instanceOf(yield* store.reserve(input).pipe(Effect.flip), Overseer.OverseerConflict);
    assert.deepEqual(
      yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`,
      coreBefore,
    );
    assert.equal(
      (yield* sql`SELECT * FROM orchestration_v2_effect_outbox`)[0]?.effect_id,
      "ordinary-effect",
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a capped backlog remains recoverable without any later source arrival", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const { linkId } = yield* launch();
    for (let index = 0; index < 10; index++) yield* record(linkId, `work-${index}`, index);
    const first = yield* freeze();
    assert.isTrue(yield* store.acceptAttempt(first.messageId, first.epoch));
    assert.isFalse(yield* store.checkpoint(first));
    assert.deepEqual(yield* store.recoveryMailboxes(), [observer]);
    const next = yield* freeze("2");
    assert.equal((yield* store.batchEntries(observer, next.batchNumber)).length, 2);
    assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
    assert.isTrue(yield* store.checkpoint(yield* fence()));
    assert.deepEqual(yield* store.recoveryMailboxes(), []);
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["candidate", "question"] as const)(
  "withdrawing an unbatched %s leaves frozen membership alone",
  (kind) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "frozen");
      const first = yield* freeze();
      if (kind === "candidate") yield* record(linkId, "unbatched", 2, false);
      else yield* question(linkId, "unbatched");
      assert.isTrue(
        yield* store.withdraw({
          linkId,
          kind: kind === "candidate" ? "completion" : "question",
          sourceId: "unbatched",
        }),
      );
      assert.equal((yield* store.pendingAttempt(observer))?.message_id, first.messageId);
      assert.isTrue(yield* store.acceptAttempt(first.messageId, first.epoch));
      assert.equal((yield* occurrences(linkId))[0]?.disposition, "accepted");
    }).pipe(Effect.provide(layerTest)),
);

it.effect("unbatched stale questions cannot invalidate a busy frozen batch", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const { linkId } = yield* launch();
    yield* record(linkId, "frozen");
    const first = yield* freeze();
    yield* question(linkId, "unbatched");
    yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status = 'resolved' WHERE runtime_request_id = 'unbatched'`;
    assert.equal((yield* store.stageBatch(yield* fence())).state, "busy");
    assert.isTrue(yield* store.acceptAttempt(first.messageId, first.epoch));
    assert.equal((yield* store.stageBatch(yield* fence())).state, "reconciled");
    assert.equal((yield* occurrences(linkId))[1]?.disposition, "withdrawn");
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "withdrawing a frozen member invalidates only its batch and releases surviving members",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "completion");
      yield* question(linkId, "question");
      const first = yield* freeze();
      assert.isTrue(yield* store.withdraw({ linkId, kind: "question", sourceId: "question" }));
      assert.isUndefined(yield* store.pendingAttempt(observer));
      assert.isFalse(yield* store.acceptAttempt(first.messageId, first.epoch));
      const next = yield* freeze("2");
      assert.deepEqual(
        (yield* store.batchEntries(observer, next.batchNumber)).map((row) => row.source_id),
        ["completion"],
      );
      assert.isTrue(yield* store.acceptAttempt(next.messageId, next.epoch));
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "process-loss retry preserves frozen membership across newer arrivals and older settlements",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "older-candidate", 0, false);
      for (let index = 1; index <= 8; index++) yield* record(linkId, `work-${index}`, index);
      const first = yield* freeze();
      const members = yield* store.batchEntries(observer, first.batchNumber);
      yield* record(linkId, "later", 9);
      yield* store.settleCompletion(linkId, "older-candidate");
      assert.deepEqual(yield* store.freezeAttempt(first), first.attempt);
      assert.isTrue(yield* store.disposeAttempt(first.messageId, "process_loss"));
      assert.isUndefined(yield* store.pendingAttempt(observer));
      const reused = yield* store.freezeAttempt(first).pipe(Effect.flip);
      assert.equal(reused._tag === "OverseerConflict" && reused.reason, "attempt_identity");
      const retry = yield* freeze("2");
      assert.equal(retry.batchNumber, first.batchNumber);
      assert.equal(retry.attempt.attempt_number, 2);
      assert.deepEqual(yield* store.batchEntries(observer, retry.batchNumber), members);
      assert.isTrue(yield* store.acceptAttempt(retry.messageId, retry.epoch));
      const next = yield* freeze("3");
      assert.deepEqual(
        (yield* store.batchEntries(observer, next.batchNumber)).map((row) => row.source_id),
        ["older-candidate", "later"],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect("project cleanup retires held mailboxes even when only closed tombstones remain", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    const { linkId } = yield* launch();
    yield* store.stop(observer);
    yield* store.unlink(linkId);
    const old = yield* fence();
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`UPDATE projection_projects SET deleted_at = ${timestamp} WHERE project_id = ${observerProject}`;
        yield* store.projectDeleted(observerProject);
      }),
    );
    assert.deepEqual(yield* store.recoveryMailboxes(), []);
    assert.equal((yield* store.mailbox(observer))?.hold_mask, 0);
    assert.instanceOf(yield* store.stageBatch(old).pipe(Effect.flip), Overseer.OverseerConflict);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("independent holds compose and automatic resumes cannot release Stop", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const { linkId } = yield* launch();
    yield* record(linkId, "work");
    yield* store.hold(observer, "snoozed");
    yield* store.hold(observer, "provider");
    yield* store.stop(observer);
    yield* store.resume(observer, "provider");
    assert.equal((yield* store.mailbox(observer))?.hold_reason, "stop");
    yield* store.resumeExplicitly(observer);
    assert.equal((yield* store.stageBatch(yield* fence())).state, "held");
    assert.equal((yield* store.mailbox(observer))?.hold_reason, "snoozed");
    yield* store.resume(observer, "snoozed");
    assert.equal((yield* store.stageBatch(yield* fence())).state, "ready");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("Stop suppresses a queued batch before admission", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const { linkId } = yield* launch();
    yield* record(linkId, "work");
    const current = yield* fence();
    const batch = readyBatch(yield* store.stageBatch(current));
    yield* store.stop(observer);
    assert.instanceOf(
      yield* store
        .freezeAttempt({
          ...current,
          batchNumber: batch.batch_number,
          commandId: CommandId.make("late"),
          messageId: MessageId.make("late"),
        })
        .pipe(Effect.flip),
      Overseer.OverseerConflict,
    );
    yield* store.resumeExplicitly(observer);
    assert.equal((yield* store.stageBatch(yield* fence())).state, "idle");
    assert.equal((yield* occurrences(linkId))[0]?.disposition, "suppressed");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("identical concurrent reservations reuse the same receipt and IDs", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const input = reservation();
    const rows = yield* Effect.all([store.reserve(input), store.reserve(input)], {
      concurrency: "unbounded",
    });
    assert.deepEqual(rows[0], rows[1]);
    assert.equal((yield* store.incompleteLaunches()).length, 1);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "transport command/message collisions are typed and several attempts can reference one steered run",
  () =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      yield* record(linkId, "work");
      const first = yield* freeze();
      const conflict = yield* store
        .freezeAttempt({ ...first, messageId: MessageId.make("different") })
        .pipe(Effect.flip);
      assert.equal(conflict._tag === "OverseerConflict" && conflict.reason, "attempt_identity");
      yield* store.bindAttempt(
        first.messageId,
        RunId.make("shared-run"),
        RunAttemptId.make("shared-native-attempt"),
      );
      yield* store.acceptAttempt(first.messageId, first.epoch);
      yield* record(linkId, "next", 2);
      const next = yield* freeze("2");
      assert.isTrue(
        yield* store.bindAttempt(
          next.messageId,
          RunId.make("shared-run"),
          RunAttemptId.make("shared-native-attempt"),
        ),
      );
      assert.equal((yield* store.attemptsForRun(RunId.make("shared-run"))).length, 2);
      assert.equal(
        (yield* store.attemptsForRun(RunId.make("shared-run"), first.messageId, 1))[0]?.message_id,
        next.messageId,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect("mailbox fences, batches and Stop are isolated between observers", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const first = yield* launch();
    const other = ThreadId.make("other-observer");
    yield* seedThread(other, observerProject);
    const second = { ...reservation("2"), observerThreadId: other };
    yield* store.reserve(second);
    yield* seedThread(second.targetThreadId, targetProject);
    yield* store.acceptLaunch(second.linkId);
    yield* record(first.linkId, "work-1");
    yield* record(second.linkId, "work-2");
    yield* freeze("one");
    const two = yield* freeze("two", other);
    yield* store.stop(observer);
    assert.isTrue(yield* store.acceptAttempt(two.messageId, two.epoch));
    assert.equal((yield* store.mailbox(other))?.hold_mask, 0);
    assert.equal((yield* occurrences(second.linkId))[0]?.disposition, "accepted");
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["completion", "question"] as const)(
  "staging a large %s backlog only revalidates selected entries",
  (kind) =>
    Effect.gen(function* () {
      const store = yield* setup();
      const { linkId } = yield* launch();
      for (let index = 0; index < 240; index++) {
        if (kind === "question") yield* question(linkId, `question-${index}`);
        else yield* record(linkId, `work-${index}`, index);
      }
      const reads: string[] = [];
      const capture: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (query.includes("FROM orchestration_v2_projection_runtime_requests")) reads.push(query);
        return Effect.succeed(statement);
      };
      const result = yield* store
        .stageBatch(yield* fence())
        .pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      const batch = readyBatch(result);
      assert.equal((yield* store.batchEntries(observer, batch.batch_number)).length, 8);
      assert.equal(reads.length, kind === "question" ? 8 : 0);
      for (const query of reads) assert.include(query, "r.runtime_request_id = ?");
    }).pipe(Effect.provide(layerTest)),
);

it.effect("uses bounded indexed production queries with 300 unrelated threads and ten links", () =>
  Effect.gen(function* () {
    const store = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    for (let index = 0; index < 300; index++)
      yield* seedThread(ThreadId.make(`unrelated-${index}`), targetProject);
    for (let index = 0; index < 10; index++) {
      const input = yield* launch(String(index));
      yield* record(input.linkId, `source-${index}`, index);
    }
    const queries: Array<ReturnType<Statement.Statement<unknown>["compile"]>> = [];
    const capture: Statement.Transformer = (statement) => {
      const compiled = statement.compile();
      if (/^\s*(?:SELECT|UPDATE|INSERT|DELETE)\b/i.test(compiled[0])) queries.push(compiled);
      return Effect.succeed(statement);
    };
    yield* Effect.gen(function* () {
      assert.equal((yield* store.linksForEndpoint(observer, "", 3)).length, 3);
      assert.equal((yield* store.linksForEndpoint(ThreadId.make("target-0"))).length, 1);
      assert.equal((yield* store.incompleteLaunches()).length, 0);
      yield* store.reserve(reservation("reserved"));
      assert.equal((yield* store.incompleteLaunches("", 1)).length, 1);
      yield* store.recoveryMailboxes("", 2);
      yield* record("link-0", "candidate", 11, false);
      yield* store.pendingCandidates(observer, undefined, 1);
      const attempt = yield* freeze();
      yield* store.bindAttempt(
        attempt.messageId,
        RunId.make("query-run"),
        RunAttemptId.make("query-attempt"),
      );
      assert.equal((yield* store.batchEntries(observer, attempt.batchNumber)).length, 8);
      yield* store.pendingAttempt(observer);
      yield* store.attemptForMessage(attempt.messageId);
      yield* store.attemptsForRun(RunId.make("query-run"));
      yield* store.acceptAttempt(attempt.messageId, attempt.epoch);
      yield* store.unlink("link-0");
      yield* store.projectDeleted(targetProject);
      yield* store.pruneClosed;
    }).pipe(Effect.provideService(Statement.CurrentTransformer, capture));
    const plans: Array<{ readonly query: string; readonly details: string }> = [];
    for (const [query, parameters] of queries) {
      const rows = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${query}`,
        parameters,
      );
      const details = rows.map((row) => row.detail).join("\n");
      assert.notMatch(
        details,
        /SCAN (?:worker|hub|t|r|p|destination|o|l|b|orchestration_v2_projection_threads|orchestration_v2_projection_runtime_requests|projection_projects)(?:\s|$)/,
      );
      assert.notMatch(details, /SCAN overseer_\w+(?:\n|$)/);
      if (query.includes("UNION SELECT observer_thread_id FROM overseer_mailboxes")) {
        for (const index of [
          "overseer_mailboxes_dirty_idx",
          "overseer_mailboxes_held_idx",
          "overseer_attempts_pending_idx",
        ])
          assert.include(details, index);
      }
      if (query.includes("AND observer_project_id =") && query.includes("UNION")) {
        assert.include(
          details,
          "overseer_links_observer_project_idx (observer_project_id=? AND state=?)",
        );
        assert.include(details, "overseer_links_target_project_idx (target_project_id=?)");
      }
      if (query.includes("ORDER BY source_sequence, source_id, link_id"))
        assert.include(details, "overseer_occurrences_candidates_idx");
      plans.push({ query, details });
    }
    const combined = plans.map((plan) => plan.details).join("\n");
    for (const index of [
      "overseer_links_observer_idx",
      "overseer_links_observer_project_idx",
      "overseer_links_target_project_idx",
      "overseer_links_launch_idx",
      "overseer_links_closed_idx",
      "overseer_occurrences_pending_idx",
      "overseer_occurrences_candidates_idx",
      "overseer_occurrences_batch_idx",
      "overseer_mailboxes_dirty_idx",
      "overseer_mailboxes_held_idx",
      "overseer_attempts_pending_idx",
    ])
      assert.include(combined, index);
    assert.match(combined, /SEARCH overseer_attempts USING INDEX \S+ \(message_id=\?\)/);
    assert.match(
      combined,
      /SEARCH overseer_attempts USING INDEX \S+ \(run_id=\? AND message_id>\?\)/,
    );
    assert.match(
      combined,
      /SEARCH overseer_links USING INDEX \S+ \(observer_thread_id=\? AND client_request_id=\?\)/,
    );
    assert.include(
      combined,
      "SEARCH overseer_links USING COVERING INDEX overseer_links_observer_project_idx (observer_project_id=? AND state=?)",
    );
    assert.include(
      combined,
      "SEARCH overseer_links USING COVERING INDEX overseer_links_target_project_idx (target_project_id=?)",
    );
    const pending = plans.find((plan) => plan.query.includes("ORDER BY o.source_sequence"));
    assert.isDefined(pending);
    assert.include(
      pending!.details,
      "SEARCH o USING INDEX overseer_occurrences_pending_idx (observer_thread_id=?)",
    );
    assert.notInclude(pending!.details, "USE TEMP B-TREE FOR ORDER BY");
  }).pipe(Effect.provide(layerTest)),
);

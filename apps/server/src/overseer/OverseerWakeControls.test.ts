import { assert, it } from "@effect/vitest";
import {
  NodeId,
  MessageId,
  ProviderSessionId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Schema from "effect/Schema";
import * as Statement from "effect/sql/Statement";
import * as Policy from "./OverseerWakePolicy.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Preferences from "./OverseerPreferences.ts";
import * as Idle from "./OverseerIdleScheduler.ts";
import * as Feature from "./OverseerFeature.ts";
import { enabled, layer, now, observer, setup, target } from "./OverseerMailbox.testkit.ts";
import { wakeText } from "./OverseerText.ts";

const isPreferences = Schema.is(Policy.Preferences);
const controlsLayer = Layer.mergeAll(Preferences.layer, Idle.layer).pipe(Layer.provideMerge(layer));
const configured = { ...Policy.defaults, idleEnabled: true, idleMinutes: 10 as const };
const controls = Effect.fn("test.overseerControls.setup")(function* () {
  const fixture = yield* setup();
  const preferences = yield* Preferences.OverseerPreferences;
  const idle = yield* Idle.OverseerIdleScheduler;
  const fence = Effect.gen(function* () {
    const box = (yield* fixture.store.mailbox(observer))!;
    return { observerThreadId: observer, epoch: box.epoch, revision: box.revision };
  });
  const complete = Effect.fn("test.overseerControls.complete")(function* (ordinal: number) {
    const work = yield* fixture.start(ordinal);
    yield* fixture.write([
      {
        type: "run.updated",
        threadId: target,
        payload: { ...work, status: "completed", completedAt: now },
      },
    ]);
    return work;
  });
  const approval = Effect.fn("test.overseerControls.approval")(function* (ordinal = 1) {
    const work = yield* fixture.start(ordinal);
    const request = {
      id: RuntimeRequestId.make(`approval:${ordinal}`),
      nodeId: NodeId.make(`node:${ordinal}`),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "permission" as const,
      status: "pending" as const,
      responseCapability: {
        type: "live" as const,
        providerSessionId: ProviderSessionId.make("session"),
      },
      createdAt: now,
      resolvedAt: null,
    };
    yield* fixture.write([
      { type: "runtime-request.updated", threadId: target, runId: work.id, payload: request },
    ]);
    return { work, request };
  });
  return { ...fixture, preferences, idle, fence, complete, approval };
});

it.effect("defaults are private, read-only and validate exactly the authorized options", () =>
  Effect.gen(function* () {
    const { preferences, sql, store } = yield* controls();
    const before = yield* store.mailbox(observer);
    assert.deepEqual(yield* preferences.get(observer), Policy.defaults);
    assert.deepEqual(yield* sql`SELECT * FROM overseer_preferences`, []);
    assert.deepEqual(yield* store.mailbox(observer), before);
    for (const cadenceMinutes of [1, 5, 10, 15, 30, 60, 180])
      assert.isTrue(isPreferences({ ...Policy.defaults, cadenceMinutes }));
    for (const idleMinutes of [10, 30, 60, 120, 360])
      assert.isTrue(isPreferences({ ...Policy.defaults, idleMinutes }));
    assert.isFalse(isPreferences({ ...Policy.defaults, idleMinutes: 5 }));
    assert.isFalse(isPreferences({ ...Policy.defaults, cadenceMinutes: 2 }));
    yield* preferences.configure(observer, configured);
    const configuredBox = yield* store.mailbox(observer);
    yield* preferences.configure(observer, configured);
    assert.deepEqual(yield* store.mailbox(observer), configuredBox);
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "passive mode retains completions, admits approvals without answering, and withdraws resolved notices",
  () =>
    Effect.gen(function* () {
      const { preferences, complete, approval, store, fence, sql, write } = yield* controls();
      yield* preferences.configure(observer, { ...Policy.defaults, autoWake: false });
      yield* complete(10);
      assert.notEqual((yield* store.stageBatch(yield* fence)).state, "ready");
      const { work, request } = yield* approval();
      const stage = yield* store.stageBatch(yield* fence);
      assert.equal(stage.state, "ready");
      if (stage.state !== "ready") return;
      assert.deepEqual(
        (yield* store.batchEntries(observer, stage.batch.batch_number)).map((row) => row.kind),
        ["approval"],
      );
      assert.deepEqual(
        yield* sql`SELECT status FROM orchestration_v2_projection_runtime_requests WHERE runtime_request_id = ${request.id}`,
        [{ status: "pending" }],
      );
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...request, status: "resolved", resolvedAt: now },
        },
      ]);
      assert.isFalse(yield* store.batchDispatchable(observer, stage.batch.batch_number));
      assert.deepEqual(
        yield* sql`SELECT kind, disposition FROM overseer_occurrences WHERE source_id = ${request.id}`,
        [{ kind: "approval", disposition: "withdrawn" }],
      );
      yield* preferences.configure(observer, Policy.defaults);
      assert.equal((yield* store.stageBatch(yield* fence)).state, "ready");
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "all five approval kinds require live capability; unrelated runtime requests never become notices",
  () =>
    Effect.gen(function* () {
      const { approval, write, sql } = yield* controls();
      const { work, request } = yield* approval();
      for (const kind of [...Policy.approvalKinds, "dynamic_tool_call", "auth_refresh"] as const) {
        const id = RuntimeRequestId.make(kind);
        yield* write([
          {
            type: "runtime-request.updated",
            threadId: target,
            runId: work.id,
            payload: { ...request, id, kind },
          },
        ]);
      }
      assert.equal(
        (yield* sql`SELECT 1 FROM overseer_occurrences WHERE kind = 'approval'`).length,
        6,
      );
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: {
            ...request,
            responseCapability: { type: "not_resumable", reason: "Fixture expired" },
            status: "expired",
            resolvedAt: now,
          },
        },
      ]);
      assert.deepEqual(
        yield* sql`SELECT disposition FROM overseer_occurrences WHERE source_id = ${request.id}`,
        [{ disposition: "withdrawn" }],
      );
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("attention takes a place ahead of a full routine backlog even in passive mode", () =>
  Effect.gen(function* () {
    const { complete, start, question, store, fence, preferences } = yield* controls();
    for (let ordinal = 1; ordinal <= 9; ordinal++) yield* complete(ordinal);
    yield* question(yield* start(10), "urgent");
    let stage = yield* store.stageBatch(yield* fence);
    assert.equal(stage.state, "ready");
    if (stage.state !== "ready") return;
    assert.equal(
      (yield* store.batchEntries(observer, stage.batch.batch_number)).filter(
        (row) => row.kind === "question",
      ).length,
      1,
    );
    yield* preferences.configure(observer, { ...Policy.defaults, autoWake: false });
    stage = yield* store.stageBatch(yield* fence);
    assert.equal(stage.state, "ready");
    if (stage.state === "ready")
      assert.equal((yield* store.batchEntries(observer, stage.batch.batch_number)).length, 1);
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "cadence starts at a provider receipt, coalesces not-before and lets attention pull it forward",
  () =>
    Effect.gen(function* () {
      const { preferences, complete, prepare, freeze, transport, sql, start, question, store } =
        yield* controls();
      yield* preferences.configure(observer, { ...Policy.defaults, cadenceEnabled: true });
      yield* complete(1);
      const identity = yield* freeze(yield* prepare());
      assert.isTrue(yield* transport.submit(identity));
      assert.isNull((yield* Policy.preferences(observer))!.last_routine_accepted_at);
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      const acceptedAt = yield* DateTime.now;
      yield* complete(2);
      let flush = (yield* store.mailbox(observer))!.flush_id;
      assert.deepEqual(
        yield* sql`SELECT available_at FROM orchestration_v2_effect_outbox WHERE effect_id = ${flush}`,
        [{ available_at: DateTime.formatIso(DateTime.add(acceptedAt, { minutes: 5 })) }],
      );
      yield* complete(3);
      assert.equal((yield* store.mailbox(observer))!.flush_id, flush);
      yield* TestClock.adjust("1 minute");
      yield* question(yield* start(4), "attention");
      flush = (yield* store.mailbox(observer))!.flush_id;
      assert.deepEqual(
        yield* sql`SELECT available_at FROM orchestration_v2_effect_outbox WHERE effect_id = ${flush}`,
        [{ available_at: DateTime.formatIso(yield* DateTime.now) }],
      );
      assert.isTrue((yield* Policy.admission(observer)).allowRoutine);
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "idle dispatch is one W3-only input, waits for receipt and rearms only after the next idle",
  () =>
    Effect.gen(function* () {
      const { preferences, idle, sync, transport, freeze, write, sql, store } = yield* controls();
      yield* preferences.configure(observer, configured);
      const commands: OrchestrationV2ServerCommand[] = [];
      const collect = (command: OrchestrationV2ServerCommand) =>
        Effect.sync(() => {
          commands.push(command);
        });
      yield* TestClock.adjust("9 minutes");
      yield* idle.runDue;
      assert.isNull((yield* Policy.preferences(observer))!.idle_pending_token);
      yield* TestClock.adjust("1 minute");
      yield* idle.runDue;
      yield* sync(collect);
      assert.equal(commands.length, 1);
      assert.equal(commands[0]!.type, "message.dispatch");
      if (commands[0]!.type !== "message.dispatch") return;
      assert.equal(commands[0]!.text, wakeText.W3);
      const offer = (yield* transport.open(observer))!;
      assert.equal(offer.members_json, "[]");
      assert.equal((yield* sql`SELECT 1 FROM overseer_occurrences`).length, 0);
      const identity = yield* freeze(offer);
      assert.isTrue(yield* transport.submit(identity));
      yield* TestClock.adjust("30 minutes");
      yield* idle.runDue;
      yield* sync(collect);
      assert.equal(commands.length, 1);
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      assert.isNull((yield* Policy.preferences(observer))!.idle_due_at);
      const projections = yield* Projections.ProjectionStoreV2;
      const wakeRun = (yield* projections.getThreadProjection(observer)).runs.find(
        (run) => run.id === identity.runId,
      )!;
      yield* write([
        {
          type: "run.updated",
          threadId: observer,
          payload: { ...wakeRun, status: "completed", completedAt: now },
        },
      ]);
      assert.isNotNull((yield* Policy.preferences(observer))!.idle_due_at);
      yield* TestClock.adjust("10 minutes");
      yield* idle.runDue;
      yield* sync(collect);
      assert.equal(commands.length, 2);
      assert.equal((yield* store.mailbox(observer))!.hold_mask, 0);
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "Stop and every hold disarm idle without being cleared by due work or preference changes",
  () =>
    Effect.gen(function* () {
      const { preferences, idle, store, sync, write } = yield* controls();
      yield* preferences.configure(observer, configured);
      yield* store.stop(observer);
      yield* preferences.configure(observer, { ...configured, autoWake: false });
      yield* TestClock.adjust("1 hour");
      yield* idle.runDue;
      let dispatches = 0;
      yield* sync(() =>
        Effect.sync(() => {
          dispatches++;
        }),
      );
      assert.equal(dispatches, 0);
      assert.equal((yield* store.mailbox(observer))!.hold_mask, 1);
      yield* write([], "queue.resume", observer);
      for (const reason of ["queue", "snoozed", "provider", "authorization"] as const) {
        yield* store.hold(observer, reason);
        yield* Policy.syncIdleState(observer);
        yield* idle.runDue;
        assert.isNull((yield* Policy.preferences(observer))!.idle_due_at);
        yield* store.resume(observer, reason);
      }
      assert.isNotNull((yield* Policy.preferences(observer))!.idle_due_at);
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "cadence expires without a new source event and passive preferences are per overseer",
  () =>
    Effect.gen(function* () {
      const { preferences, complete, prepare, freeze, transport, store, fence, sql, outbox } =
        yield* controls();
      yield* preferences.configure(observer, { ...Policy.defaults, cadenceEnabled: true });
      yield* complete(1);
      const identity = yield* freeze(yield* prepare());
      assert.isTrue(yield* transport.submit(identity));
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      yield* complete(2);
      assert.notEqual((yield* store.stageBatch(yield* fence)).state, "ready");
      const deadline = yield* outbox.nextClaimableAt;
      assert.equal(deadline._tag, "Some");
      assert.equal(
        (yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 }))._tag,
        "None",
      );
      yield* TestClock.adjust("5 minutes");
      assert.equal(
        (yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 }))._tag,
        "Some",
      );
      assert.equal((yield* store.stageBatch(yield* fence)).state, "ready");
      yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id) VALUES ('other')`;
      const other = ThreadId.make("other");
      yield* preferences.configure(other, { ...Policy.defaults, autoWake: false });
      assert.isTrue((yield* preferences.get(observer)).autoWake);
      assert.isFalse((yield* preferences.get(other)).autoWake);
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "an early running flush leaves a durable cadence successor and preserves failure backoff",
  () =>
    Effect.gen(function* () {
      const { preferences, complete, prepare, freeze, transport, store, outbox, sync, sql } =
        yield* controls();
      yield* preferences.configure(observer, { ...Policy.defaults, cadenceEnabled: true });
      yield* complete(1);
      const identity = yield* freeze(yield* prepare());
      assert.isTrue(yield* transport.submit(identity));
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      yield* complete(2);
      const early = (yield* store.mailbox(observer))!.flush_id!;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET available_at = ${DateTime.formatIso(yield* DateTime.now)} WHERE effect_id = ${early}`;
      assert.equal(
        (yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 }))._tag,
        "Some",
      );
      yield* sync(() => Effect.die("routine cadence must still hold"));
      const successor = (yield* store.mailbox(observer))!.flush_id!;
      assert.notEqual(successor, early);
      yield* outbox.succeed({ effectId: early, workerId: "worker" });
      const backoff = DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 6 }));
      yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count = 1, available_at = ${backoff} WHERE effect_id = ${successor}`;
      yield* store.touch(observer);
      assert.deepEqual(
        yield* sql`SELECT available_at FROM orchestration_v2_effect_outbox WHERE effect_id = ${successor}`,
        [{ available_at: backoff }],
      );
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("byte-trimmed attention-first text matches every frozen receipt member", () =>
  Effect.gen(function* () {
    const { complete, start, question, sync, transport, freeze, sql } = yield* controls();
    for (let i = 1; i <= 7; i++) yield* complete(i);
    const request = yield* question(yield* start(8), "urgent:" + "q".repeat(1500));
    yield* sync(() => Effect.void);
    const row = (yield* transport.open(observer))!;
    assert.include(row.text, request.id);
    const members = yield* sql<{
      readonly source_id: string;
    }>`SELECT source_id FROM overseer_occurrences WHERE batch_number = ${row.batch_number}`;
    assert.isBelow(members.length, 8);
    for (const member of members) assert.include(row.text, member.source_id);
    const identity = yield* freeze(row);
    assert.isTrue(yield* transport.submit(identity));
    assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
    assert.equal(
      (yield* sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
      members.length,
    );
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("passive approval withdrawal promptly cancels an already queued private input", () =>
  Effect.gen(function* () {
    const { approval, preferences, prepare, freeze, write, sync, store, outbox, sql } =
      yield* controls();
    yield* preferences.configure(observer, { ...Policy.defaults, autoWake: false });
    const { work, request } = yield* approval();
    const row = yield* prepare();
    yield* freeze(row, 100, "queued");
    const claimed = yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 });
    assert.equal(claimed._tag, "Some");
    if (claimed._tag !== "Some") return;
    yield* outbox.succeed({ effectId: claimed.value.id, workerId: "worker" });
    yield* write([
      {
        type: "runtime-request.updated",
        threadId: target,
        runId: work.id,
        payload: { ...request, status: "resolved", resolvedAt: now },
      },
    ]);
    const flush = (yield* store.mailbox(observer))!.flush_id;
    assert.notEqual(flush, claimed.value.id);
    assert.deepEqual(
      yield* sql`SELECT available_at FROM orchestration_v2_effect_outbox WHERE effect_id = ${flush}`,
      [{ available_at: DateTime.formatIso(yield* DateTime.now) }],
    );
    const commands: OrchestrationV2ServerCommand[] = [];
    yield* sync((command) =>
      Effect.sync(() => {
        commands.push(command);
      }),
    );
    assert.deepEqual(
      commands.map((command) => command.type),
      ["queued-run.cancel"],
    );
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "cancelled idle input rearms the interval instead of immediately redispatching its token",
  () =>
    Effect.gen(function* () {
      const { preferences, idle, sync, transport, freeze, write } = yield* controls();
      yield* preferences.configure(observer, configured);
      yield* TestClock.adjust("10 minutes");
      yield* idle.runDue;
      yield* sync(() => Effect.void);
      const row = (yield* transport.open(observer))!;
      const identity = yield* freeze(row, 100, "queued");
      const projection = yield* Projections.ProjectionStoreV2;
      const run = (yield* projection.getThreadProjection(observer)).runs.find(
        (run) => run.id === identity.runId,
      )!;
      yield* write(
        [
          {
            type: "run.updated",
            threadId: observer,
            payload: { ...run, status: "cancelled", completedAt: now },
          },
        ],
        "queued-run.cancel",
      );
      const prefs = (yield* Policy.preferences(observer))!;
      assert.isNull(prefs.idle_pending_token);
      assert.equal(
        prefs.idle_due_at,
        DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 10 })),
      );
      yield* idle.runDue;
      yield* sync(() => Effect.die("cancel must not cause immediate W3"));
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("queued idle batches refresh their fence after intervening dirty revisions", () =>
  Effect.gen(function* () {
    const { preferences, idle, sync, store, fence } = yield* controls();
    yield* preferences.configure(observer, configured);
    yield* TestClock.adjust("10 minutes");
    yield* idle.runDue;
    assert.isDefined(yield* store.stageIdleBatch(yield* fence));
    yield* store.touch(observer);
    let count = 0;
    yield* sync(() =>
      Effect.sync(() => {
        count++;
      }),
    );
    assert.equal(count, 1);
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("preferences cannot retire a submitted idle attempt or erase its restart identity", () =>
  Effect.gen(function* () {
    const { preferences, idle, sync, transport, freeze, sql } = yield* controls();
    yield* preferences.configure(observer, configured);
    yield* TestClock.adjust("10 minutes");
    yield* idle.runDue;
    yield* sync(() => Effect.void);
    const first = (yield* transport.open(observer))!;
    const identity = yield* freeze(first);
    assert.isTrue(yield* transport.submit(identity));
    yield* preferences.configure(observer, { ...configured, idleMinutes: 30 });
    assert.isNotNull((yield* Policy.preferences(observer))!.idle_pending_token);
    assert.deepEqual(
      yield* sql`SELECT state FROM overseer_attempts WHERE message_id = ${first.message_id}`,
      [{ state: "pending" }],
    );
    yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'interrupted', payload_json = json_set(payload_json, '$.status', 'interrupted') WHERE run_id = ${identity.runId}`;
    yield* transport.afterProcessLoss(observer);
    yield* sync(() => Effect.void);
    assert.equal((yield* transport.open(observer))!.batch_number, first.batch_number);
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("a busy transition after the idle claim fences the stale W3 before submission", () =>
  Effect.gen(function* () {
    const { preferences, idle, sync, transport, freeze, write } = yield* controls();
    yield* preferences.configure(observer, configured);
    yield* TestClock.adjust("10 minutes");
    yield* idle.runDue;
    yield* sync(() => Effect.void);
    const offer = (yield* transport.open(observer))!;
    const identity = yield* freeze(offer);
    const projections = yield* Projections.ProjectionStoreV2;
    const wake = (yield* projections.getThreadProjection(observer)).runs[0]!;
    yield* write([
      {
        type: "run.created",
        threadId: observer,
        payload: {
          ...wake,
          userMessageId: MessageId.make("competing"),
          id: RunId.make("competing"),
          ordinal: 101,
        },
      },
    ]);
    assert.isFalse(yield* transport.submit(identity));
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "process loss retries an unreceipted idle input under the same batch ID, with a fresh identity",
  () =>
    Effect.gen(function* () {
      const { preferences, idle, sync, transport, freeze, sql } = yield* controls();
      yield* preferences.configure(observer, configured);
      yield* TestClock.adjust("10 minutes");
      yield* idle.runDue;
      yield* sync(() => Effect.void);
      const first = (yield* transport.open(observer))!;
      const identity = yield* freeze(first);
      assert.isTrue(yield* transport.submit(identity));
      yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'interrupted', payload_json = json_set(payload_json, '$.status', 'interrupted') WHERE run_id = ${identity.runId}`;
      yield* transport.afterProcessLoss(observer);
      yield* sync(() => Effect.void);
      const retry = (yield* transport.open(observer))!;
      assert.equal(retry.batch_number, first.batch_number);
      assert.notEqual(retry.message_id, first.message_id);
      assert.equal(retry.text, wakeText.W3);
    }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect("idle due work pages only the indexed due set, not unrelated threads or settings", () =>
  Effect.gen(function* () {
    const { preferences, idle, sql } = yield* controls();
    yield* preferences.configure(observer, configured);
    for (let i = 0; i < 110; i++) {
      const id = `unrelated:${i}`;
      yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id) VALUES (${id})`;
      yield* sql`INSERT INTO overseer_preferences(observer_thread_id, idle_enabled, idle_due_at) VALUES (${id}, 1, '1970-01-01T00:00:00.000Z')`;
    }
    yield* TestClock.adjust("10 minutes");
    const captured: Array<readonly [string, ReadonlyArray<unknown>]> = [];
    const capture: Statement.Transformer = (statement) => {
      captured.push(statement.compile());
      return Effect.succeed(statement);
    };
    yield* idle.runDue.pipe(Effect.provideService(Statement.CurrentTransformer, capture));
    const dueQueries = captured.filter(([query]) => query.includes("ORDER BY idle_due_at"));
    assert.isAtLeast(dueQueries.length, 2);
    for (const [query, parameters] of dueQueries) {
      const plan = (yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${query}`,
        parameters,
      ))
        .map((row) => row.detail)
        .join("\n");
      assert.include(plan, "overseer_preferences_idle_due_idx");
      assert.notInclude(plan, "TEMP B-TREE");
      assert.notMatch(plan, /SCAN/);
    }
    for (const [query, parameters] of captured.filter(([query]) =>
      query.includes("FROM overseer_mailboxes b"),
    )) {
      const plan = (yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${query}`,
        parameters,
      ))
        .map((row) => row.detail)
        .join("\n");
      assert.notMatch(plan, /SCAN (?:b|t|p|r|q|s|i|pt|owner|pending)\b/);
    }
    assert.equal(
      (yield* sql`SELECT 1 FROM overseer_preferences WHERE idle_pending_token IS NOT NULL`).length,
      1,
    );
  }).pipe(enabled, Effect.provide(controlsLayer)),
);

it.effect(
  "default-off idle scheduling and preference mutations leave ordinary state untouched",
  () =>
    Effect.gen(function* () {
      const { preferences, idle, sql, store } = yield* controls();
      const before = yield* store.mailbox(observer);
      yield* preferences.configure(observer, configured).pipe(Effect.flip);
      yield* idle.runDue;
      assert.deepEqual(yield* store.mailbox(observer), before);
      assert.deepEqual(yield* sql`SELECT * FROM overseer_preferences`, []);
    }).pipe(Effect.provideService(Feature.Enabled, false), Effect.provide(controlsLayer)),
);

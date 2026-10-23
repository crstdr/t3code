import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  RunAttemptId,
  RunId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Statement from "effect/sql/Statement";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import { enabled, layer, now, observer, setup, target } from "./OverseerMailbox.testkit.ts";
import { renderWake } from "./OverseerText.ts";
import * as Native from "./OverseerNative.ts";
import { TransportError } from "./OverseerTransport.ts";
import { OverseerNoticeGrant } from "./OverseerNoticeGrant.ts";

it.effect("folds pending flushes and commits exactly one successor to a running generation", () =>
  Effect.gen(function* () {
    const { store, outbox, start, question, sql } = yield* setup();
    const work = yield* start(1);
    const first = (yield* store.mailbox(observer))!.flush_id!;
    yield* question(work, "question:1");
    assert.equal((yield* store.mailbox(observer))!.flush_id, first);
    const claimed = yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 });
    assert.isTrue(Option.isSome(claimed));
    assert.equal(Option.getOrThrow(claimed).id, first);
    yield* question(work, "question:2");
    const successor = (yield* store.mailbox(observer))!.flush_id!;
    assert.notEqual(successor, first);
    yield* question(work, "question:3");
    assert.equal((yield* store.mailbox(observer))!.flush_id, successor);
    assert.isTrue(yield* outbox.succeed({ effectId: first, workerId: "worker" }));
    const rows = yield* sql<{
      readonly status: string;
    }>`SELECT status FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush' ORDER BY rowid`;
    assert.deepEqual(
      rows.map((row) => row.status),
      ["succeeded", "pending"],
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("rolls back a new source, revision, generation and outbox insert together", () =>
  Effect.gen(function* () {
    const { sql, store, gate } = yield* setup();
    const before = (yield* store.mailbox(observer))!;
    yield* gate
      .commit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* store.record({
              linkId: "link",
              kind: "completion",
              sourceId: "source",
              sourceSequence: 1,
              ready: true,
            });
            yield* sql`INSERT INTO missing_rollback_fixture VALUES (1)`;
          }),
        ),
      )
      .pipe(Effect.flip);
    assert.deepEqual(yield* store.mailbox(observer), before);
    assert.deepEqual(yield* sql`SELECT 1 FROM overseer_occurrences`, []);
    assert.deepEqual(
      yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush'`,
      [],
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "keeps each overseer flush in a lane separate from ordinary provider work and title generation",
  () =>
    Effect.gen(function* () {
      const { store, outbox, start, sql } = yield* setup();
      yield* start(1);
      const id = (yield* store.mailbox(observer))!.flush_id!;
      yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES ('ordinary', 'command:ordinary', ${observer}, 'provider-turn.start', '{"type":"provider-turn.start","runId":"ordinary"}', 'running', 1,
      '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z', '2000-01-01T00:00:00.000Z')`;
      const claimed = Option.getOrThrow(
        yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 }),
      );
      assert.equal(claimed.id, id);
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "splits nine facts without accepting the ninth and dirties a successor after receipt",
  () =>
    Effect.gen(function* () {
      const { start, question, store, transport, prepare, freeze, sql } = yield* setup();
      const work = yield* start(1);
      for (let index = 0; index < 9; index++) yield* question(work, `question:${index}`);
      const row = yield* prepare();
      assert.equal((yield* store.batchEntries(observer, row.batch_number)).length, 8);
      const identity = yield* freeze(row);
      assert.isTrue(yield* transport.submit(identity));
      assert.equal(
        (yield* sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
        0,
      );
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      const rows = yield* sql<{
        readonly source_id: string;
        readonly disposition: string;
      }>`SELECT source_id, disposition FROM overseer_occurrences WHERE kind = 'question' ORDER BY source_id`;
      assert.equal(rows.filter((item) => item.disposition === "accepted").length, 8);
      assert.deepEqual(
        rows.filter((item) => item.disposition === "pending").map((item) => item.source_id),
        ["question:8"],
      );
      assert.isFalse(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      const box = (yield* store.mailbox(observer))!;
      const next = yield* store.stageBatch({
        observerThreadId: observer,
        epoch: box.epoch,
        revision: box.revision,
      });
      assert.equal(next.state, "ready");
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "matches receipts by exact attempt, UUID and provider session, not enqueue or running state",
  () =>
    Effect.gen(function* () {
      const { start, question, transport, prepare, freeze, sql } = yield* setup();
      yield* question(yield* start(1), "question");
      const identity = yield* freeze(yield* prepare());
      const input = { ...identity, nativeUuid: "7a75fd48-f69e-4ed7-8c10-82e8bce6ad88" };
      assert.isTrue(yield* transport.submit(input));
      assert.isFalse(yield* transport.submit(input));
      assert.isFalse(
        yield* transport.receipt({ ...input, nativeUuid: "wrong", kind: "claude_echo" }),
      );
      assert.isFalse(
        yield* transport.receipt({ ...input, providerSessionId: "other", kind: "claude_echo" }),
      );
      assert.isFalse(
        yield* transport.receipt({
          ...input,
          attemptId: RunAttemptId.make("old"),
          kind: "claude_started",
        }),
      );
      assert.isFalse(yield* transport.receipt({ ...input, kind: "codex_response" }));
      assert.equal(
        (yield* sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
        0,
      );
      assert.isTrue(yield* transport.receipt({ ...input, kind: "claude_started" }));
      assert.isFalse(yield* transport.receipt({ ...input, kind: "claude_echo" }));
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "holds a submitted mixed batch as a receipt barrier while withdrawing only its resolved question",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, write, sync, sql } = yield* setup();
      const work = yield* start(1);
      const first = yield* question(work, "first");
      yield* question(work, "second");
      const identity = yield* freeze(yield* prepare());
      assert.isTrue(yield* transport.submit(identity));
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...first, status: "resolved", resolvedAt: now },
        },
      ]);
      let dispatches = 0;
      yield* sync(() =>
        Effect.sync(() => {
          dispatches++;
        }),
      );
      assert.equal(dispatches, 0);
      assert.equal((yield* transport.open(observer))!.phase, "submitted");
      assert.isTrue(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      const rows = yield* sql<{
        readonly source_id: string;
        readonly disposition: string;
      }>`SELECT source_id, disposition FROM overseer_occurrences WHERE kind = 'question' ORDER BY source_id`;
      assert.deepEqual(rows, [
        { source_id: "first", disposition: "withdrawn" },
        { source_id: "second", disposition: "accepted" },
      ]);
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "Stop suppresses snapshot survivors after membership was invalidated, and a late receipt cannot rearm them",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, write, store, sql } = yield* setup();
      const work = yield* start(1);
      const first = yield* question(work, "first");
      yield* question(work, "second");
      const identity = yield* freeze(yield* prepare());
      yield* transport.submit(identity);
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...first, status: "resolved", resolvedAt: now },
        },
      ]);
      yield* write([], "thread.stop");
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
      yield* store.resumeExplicitly(observer);
      assert.isFalse(yield* transport.receipt({ ...identity, kind: "codex_response" }));
      assert.deepEqual(
        yield* sql`SELECT source_id FROM overseer_occurrences WHERE kind = 'question' AND disposition = 'suppressed'`,
        [{ source_id: "second" }],
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each(["stop", "unlink"] as const)(
  "%s committed before dispatch admission prevents any native submission",
  (fence) =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, store } = yield* setup();
      yield* question(yield* start(1), "question");
      const identity = yield* freeze(yield* prepare());
      if (fence === "stop") yield* store.stop(observer);
      else yield* store.unlink("link");
      let nativeWrites = 0;
      if (yield* transport.submit(identity)) nativeWrites++;
      assert.equal(nativeWrites, 0);
      assert.isFalse(yield* transport.receipt({ ...identity, kind: "codex_response" }));
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "retains an unreceipted completed input without sending a second attempt on later facts",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, sync, sql, write } = yield* setup();
      const work = yield* start(1);
      yield* question(work, "first");
      const identity = yield* freeze(yield* prepare());
      yield* transport.submit(identity);
      const projection = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(observer, [
        "runs",
      ]);
      const wake = projection.runs.find((run) => run.id === identity.runId)!;
      yield* write([
        {
          type: "run.updated",
          threadId: observer,
          payload: { ...wake, status: "completed", completedAt: now },
        },
      ]);
      yield* question(work, "second");
      let sends = 0;
      yield* sync(() =>
        Effect.sync(() => {
          sends++;
        }),
      );
      yield* sync(() =>
        Effect.sync(() => {
          sends++;
        }),
      );
      assert.equal(sends, 0);
      assert.equal((yield* transport.forMessage(identity.messageId))!.phase, "submitted");
      assert.equal((yield* sql`SELECT 1 FROM overseer_attempts`).length, 1);
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect("uses the largest byte-bounded prefix as the exact persisted membership", () =>
  Effect.gen(function* () {
    const { start, question, sync, transport, store } = yield* setup();
    const work = yield* start(1);
    for (let index = 0; index < 8; index++) yield* question(work, `${index}:${"λ".repeat(180)}`);
    const commands: OrchestrationV2ServerCommand[] = [];
    yield* sync((command) =>
      Effect.sync(() => {
        commands.push(command);
      }),
    );
    const row = (yield* transport.open(observer))!;
    const members = yield* store.batchEntries(observer, row.batch_number);
    assert.isAbove(members.length, 0);
    assert.isBelow(members.length, 8);
    assert.equal(commands.length, 1);
    const expected = renderWake(
      `${observer}:${row.batch_number}`,
      members.map((member) => ({
        kind: "question",
        threadId: target,
        requestId: member.source_id,
      })),
    )!;
    assert.equal(row.text, expected.text);
    assert.equal(expected.count, members.length);
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("Stop winning the feature gate denies the adapter enqueue without a deadlock", () =>
  Effect.gen(function* () {
    const { start, question, prepare, freeze, transport, store, gate } = yield* setup();
    yield* question(yield* start(1), "question");
    const identity = yield* freeze(yield* prepare());
    const stopInside = yield* Deferred.make<void>();
    const finishStop = yield* Deferred.make<void>();
    const stop = yield* gate
      .commit(
        Deferred.succeed(stopInside, undefined).pipe(
          Effect.andThen(Deferred.await(finishStop)),
          Effect.andThen(store.stop(observer)),
        ),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(stopInside);
    let enqueues = 0;
    const send = yield* transport
      .handoff(identity, (enqueued) =>
        Effect.sync(() => {
          enqueues++;
        }).pipe(Effect.andThen(enqueued)),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.succeed(finishStop, undefined);
    yield* Fiber.join(stop);
    assert.isTrue(Option.isNone(yield* Fiber.join(send)));
    assert.equal(enqueues, 0);
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "handoff winning the gate releases at enqueue, not provider response, and Stop fences every later send",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, write } = yield* setup();
      yield* question(yield* start(1), "question");
      const identity = yield* freeze(yield* prepare());
      const finalCheck = yield* Deferred.make<void>();
      const allowEnqueue = yield* Deferred.make<void>();
      const enqueued = yield* Deferred.make<void>();
      const response = yield* Deferred.make<void>();
      const stopRequested = yield* Deferred.make<void>();
      const order: string[] = [];
      const send = yield* transport
        .handoff(identity, (release) =>
          Deferred.succeed(finalCheck, undefined).pipe(
            Effect.andThen(Deferred.await(allowEnqueue)),
            Effect.andThen(
              Effect.sync(() => {
                order.push("enqueue");
              }),
            ),
            Effect.andThen(release),
            Effect.andThen(Deferred.succeed(enqueued, undefined)),
            Effect.andThen(Deferred.await(response)),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(finalCheck);
      const stop = yield* Deferred.succeed(stopRequested, undefined).pipe(
        Effect.andThen(write([], "thread.stop")),
        Effect.andThen(
          Effect.sync(() => {
            order.push("stop");
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(stopRequested);
      yield* Deferred.succeed(allowEnqueue, undefined);
      yield* Deferred.await(enqueued);
      yield* Fiber.join(stop);
      assert.deepEqual(order, ["enqueue", "stop"]);
      assert.isFalse(yield* Deferred.isDone(response));
      assert.isTrue(
        Option.isNone(
          yield* transport.handoff(identity, () =>
            Effect.sync(() => {
              order.push("late enqueue");
            }),
          ),
        ),
      );
      assert.deepEqual(order, ["enqueue", "stop"]);
      yield* Deferred.succeed(response, undefined);
      yield* Fiber.join(send);
      assert.isFalse(yield* transport.receipt({ ...identity, kind: "codex_response" }));
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "a failed enqueue releases the feature gate but retains the uncertain receipt barrier",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, store } = yield* setup();
      yield* question(yield* start(1), "question");
      const identity = yield* freeze(yield* prepare());
      yield* transport.handoff(identity, () => Effect.fail("fixture")).pipe(Effect.flip);
      assert.equal((yield* transport.open(observer))!.phase, "submitted");
      yield* store.stop(observer);
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "does not schedule private work when disabled and rejects a reserved message without ownership",
  () =>
    Effect.gen(function* () {
      const { store, transport, sql } = yield* setup();
      yield* store.record({
        linkId: "link",
        kind: "completion",
        sourceId: "source",
        sourceSequence: 1,
        ready: true,
      });
      assert.deepEqual(
        yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush'`,
        [],
      );
      assert.isFalse(
        yield* transport.submit({
          messageId: MessageId.make("message:overseer:missing"),
          runId: RunId.make("run"),
          attemptId: RunAttemptId.make("attempt"),
          providerSessionId: "session",
        }),
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["session", "attempt", "terminal"] as const)(
  "refuses native handoff after its exact core %s binding becomes stale",
  (field) =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, write } = yield* setup();
      yield* question(yield* start(1), "question");
      const identity = yield* freeze(yield* prepare());
      if (field === "terminal") {
        const projection = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(
          observer,
          ["runs"],
        );
        const wake = projection.runs.find((run) => run.id === identity.runId)!;
        yield* write([
          {
            type: "run.updated",
            threadId: observer,
            payload: { ...wake, status: "completed", completedAt: now },
          },
        ]);
      }
      const stale =
        field === "session"
          ? { ...identity, providerSessionId: "other" }
          : field === "attempt"
            ? { ...identity, attemptId: RunAttemptId.make("other") }
            : identity;
      let enqueues = 0;
      const result = yield* transport.handoff(stale, () =>
        Effect.sync(() => {
          enqueues++;
        }),
      );
      assert.isTrue(Option.isNone(result));
      assert.equal(enqueues, 0);
      if (field === "terminal") assert.isUndefined(yield* transport.open(observer));
      else {
        assert.equal((yield* transport.open(observer))!.phase, "frozen");
        assert.isTrue(yield* transport.submit(identity));
      }
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each(["completed", "failed", "cancelled", "interrupted"] as const)(
  "renders the settled completion outcome %s from its exact causal root",
  (outcome) =>
    Effect.gen(function* () {
      const { start, write, sync, store, transport } = yield* setup();
      const work = yield* start(1);
      yield* write([
        {
          type: "run.updated",
          threadId: target,
          payload: { ...work, status: outcome, completedAt: now },
        },
      ]);
      const commands: OrchestrationV2ServerCommand[] = [];
      yield* sync((command) =>
        Effect.sync(() => {
          commands.push(command);
        }),
      );
      const command = commands[0];
      if (command?.type !== "message.dispatch") return assert.fail("missing wake");
      assert.include(command.text, `Thread ${target}, run ${work.id}: ${outcome}.`);
      const row = (yield* transport.forMessage(command.messageId))!;
      assert.deepEqual(
        (yield* store.batchEntries(observer, row.batch_number)).map((member) => [
          member.kind,
          member.source_id,
        ]),
        [["completion", work.id]],
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each([false, true])("allows queued maintenance after withdrawal=%s", (withdraw) =>
  Effect.gen(function* () {
    const { start, question, prepare, freeze, transport, write } = yield* setup();
    const work = yield* start(1);
    const first = yield* question(work, "first");
    yield* question(work, "second");
    const identity = yield* freeze(yield* prepare(), 100, "queued");
    if (withdraw)
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...first, status: "resolved", resolvedAt: now },
        },
      ]);
    const projection = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(observer, [
      "runs",
    ]);
    const wake = projection.runs.find((run) => run.id === identity.runId)!;
    yield* write([
      { type: "run.updated", threadId: observer, payload: { ...wake, queueHeld: true } },
    ]);
    const after = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(observer, [
      "runs",
    ]);
    assert.isTrue(after.runs.find((run) => run.id === wake.id)!.queueHeld);
    assert.isFalse(yield* transport.submit(identity));
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "an invalid private queued body rolls back without vetoing sibling cancellation maintenance",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, write, sql } = yield* setup();
      yield* question(yield* start(1), "first");
      const row = yield* prepare();
      const identity = yield* freeze(row, 100, "queued");
      const projection = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(observer, [
        "runs",
        "messages",
      ]);
      const wake = projection.runs.find((run) => run.id === identity.runId)!;
      const message = projection.messages.find((message) => message.id === identity.messageId)!;
      yield* write([
        {
          type: "message.updated",
          threadId: observer,
          payload: { ...message, text: "invalid private fixture" },
        },
      ]).pipe(
        Effect.provideService(OverseerNoticeGrant, { messageId: identity.messageId, queued: true }),
        Effect.flip,
      );
      yield* write(
        [{ type: "run.updated", threadId: observer, payload: { ...wake, queueHeld: true } }],
        "queued-run.cancel",
      );
      assert.deepEqual(
        yield* sql`SELECT disposition FROM overseer_occurrences WHERE kind = 'question'`,
        [{ disposition: "pending" }],
      );
      const after = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(observer, [
        "messages",
      ]);
      assert.equal(
        after.messages.find((message) => message.id === identity.messageId)!.text,
        row.text,
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each(["attempt", "session"] as const)(
  "releases an unsent target whose persisted %s changes",
  (field) =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, sql, sync } = yield* setup();
      yield* question(yield* start(1), "first");
      const identity = yield* freeze(yield* prepare());
      if (field === "attempt")
        yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = json_set(payload_json, '$.activeAttemptId', 'replacement') WHERE run_id = ${identity.runId}`;
      else
        yield* sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id = 'replacement' WHERE provider_thread_id = 'provider:observer'`;
      assert.isFalse(yield* transport.submit(identity));
      assert.isUndefined(yield* transport.open(observer));
      let dispatches = 0;
      yield* sync(() =>
        Effect.sync(() => {
          dispatches++;
        }),
      );
      assert.equal(dispatches, 1);
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each(["queued", "running"] as const)(
  "Stop suppresses invalidated %s snapshots by bounded keys",
  (status) =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, write, store, sql } = yield* setup();
      const work = yield* start(1);
      const first = yield* question(work, "first");
      yield* question(work, "second");
      yield* freeze(yield* prepare(), 100, status);
      yield* write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...first, status: "resolved", resolvedAt: now },
        },
      ]);
      for (let index = 0; index < 64; index++)
        yield* store.record({
          linkId: "link",
          kind: "completion",
          sourceId: `backlog:${index}`,
          sourceSequence: 1000 + index,
          ready: true,
        });
      const updates: string[] = [];
      const capture: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (query.includes("UPDATE overseer_occurrences")) updates.push(query);
        return Effect.succeed(statement);
      };
      yield* store
        .stop(observer)
        .pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      assert.deepEqual(
        yield* sql`SELECT source_id FROM overseer_occurrences WHERE disposition = 'suppressed'`,
        [{ source_id: "second" }],
      );
      assert.equal(
        (yield* sql`SELECT 1 FROM overseer_occurrences WHERE source_id LIKE 'backlog:%' AND disposition = 'pending'`)
          .length,
        64,
      );
      assert.isTrue(updates.some((query) => query.includes("WHERE link_id =")));
      assert.isFalse(updates.some((query) => query.includes("json_each")));
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect("retries a failed private cancellation without abandoning its queued target", () =>
  Effect.gen(function* () {
    const { start, question, prepare, freeze, write, sync, transport, sql } = yield* setup();
    const work = yield* start(1);
    const first = yield* question(work, "first");
    const identity = yield* freeze(yield* prepare(), 100, "queued");
    yield* write([
      {
        type: "runtime-request.updated",
        threadId: target,
        runId: work.id,
        payload: { ...first, status: "resolved", resolvedAt: now },
      },
    ]);
    yield* sync(() => Effect.die("one cancellation failure")).pipe(Effect.exit);
    assert.equal((yield* transport.open(observer))!.run_id, identity.runId);
    let cancels = 0;
    yield* sync((command) =>
      Effect.gen(function* () {
        assert.equal(command.type, "queued-run.cancel");
        if (command.type !== "queued-run.cancel") return;
        assert.equal(command.runId, identity.runId);
        const projection = yield* (yield* Projections.ProjectionStoreV2).getThreadRecords(
          observer,
          ["runs"],
        );
        const wake = projection.runs.find((run) => run.id === identity.runId)!;
        yield* write(
          [
            {
              type: "run.updated",
              threadId: observer,
              payload: { ...wake, status: "cancelled", completedAt: now },
            },
          ],
          "queued-run.cancel",
        );
        cancels++;
      }),
    );
    assert.equal(cancels, 1);
    assert.isUndefined(yield* transport.open(observer));
    assert.deepEqual(
      yield* sql`SELECT disposition FROM overseer_occurrences WHERE kind = 'question'`,
      [{ disposition: "withdrawn" }],
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("retries receipt storage without another native input or response", () =>
  Effect.gen(function* () {
    const { start, question, prepare, freeze, transport, sql } = yield* setup();
    yield* question(yield* start(1), "first");
    const identity = yield* freeze(yield* prepare());
    let attempts = 0;
    let enqueues = 0;
    const observation = Native.observation(
      {
        ...transport,
        receipt: (input) =>
          Effect.suspend(() => {
            attempts++;
            return attempts === 1
              ? Effect.fail(new TransportError({ operation: "fixture", cause: "transient" }))
              : transport.receipt(input);
          }),
      },
      identity,
    )!;
    yield* observation.handoff(undefined, (release) =>
      Effect.sync(() => {
        enqueues++;
      }).pipe(Effect.andThen(release)),
    );
    yield* observation.received("codex_response");
    assert.equal(enqueues, 1);
    assert.equal(attempts, 2);
    assert.deepEqual(
      yield* sql`SELECT disposition FROM overseer_occurrences WHERE kind = 'question'`,
      [{ disposition: "accepted" }],
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("a stale generation succeeds so its durable successor is immediately claimable", () =>
  Effect.gen(function* () {
    const { start, question, sync, store, outbox } = yield* setup();
    const work = yield* start(1);
    yield* question(work, "first");
    const claimed = Option.getOrThrow(
      yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 }),
    );
    const stage = store.stageBatch;
    const spy = vi
      .spyOn(store, "stageBatch")
      .mockImplementationOnce((fence, limit) =>
        question(work, "second").pipe(Effect.orDie, Effect.andThen(stage(fence, limit))),
      );
    yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
    yield* sync(() => Effect.die("the stale generation cannot publish"));
    const next = (yield* store.mailbox(observer))!.flush_id!;
    assert.notEqual(next, claimed.id);
    yield* outbox.succeed({ effectId: claimed.id, workerId: "worker" });
    assert.equal(
      Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker", leaseDurationMs: 1000 })).id,
      next,
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect.each([false, true])(
  "known pre-handoff abort preserves submitted=%s uncertainty",
  (submitted) =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, store, sync, write } = yield* setup();
      yield* question(yield* start(1), "first");
      const identity = yield* freeze(yield* prepare());
      if (submitted) yield* transport.submit(identity);
      yield* transport.abandonUnsent(identity.messageId);
      if (submitted) {
        assert.equal((yield* transport.open(observer))!.phase, "submitted");
        assert.equal((yield* store.mailbox(observer))!.hold_mask, 0);
      } else {
        assert.isUndefined(yield* transport.open(observer));
        assert.equal((yield* store.mailbox(observer))!.hold_reason, "provider");
        let dispatches = 0;
        yield* sync(() =>
          Effect.sync(() => {
            dispatches++;
          }),
        );
        assert.equal(dispatches, 0);
        yield* write([], "queue.resume");
        yield* sync(() =>
          Effect.sync(() => {
            dispatches++;
          }),
        );
        assert.equal(dispatches, 1);
      }
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect("a receipt cannot settle a completion deferred after submission", () =>
  Effect.gen(function* () {
    const { start, write, prepare, freeze, transport, store, sql } = yield* setup();
    const work = yield* start(1);
    yield* write([
      {
        type: "run.updated",
        threadId: target,
        payload: { ...work, status: "completed", completedAt: now },
      },
    ]);
    const identity = yield* freeze(yield* prepare());
    yield* transport.submit(identity);
    yield* store.deferCompletion("link", work.id);
    yield* transport.receipt({ ...identity, kind: "codex_response" });
    assert.deepEqual(
      yield* sql`SELECT disposition, ready FROM overseer_occurrences WHERE source_id = ${work.id}`,
      [{ disposition: "pending", ready: 0 }],
    );
    yield* store.settleCompletion("link", work.id);
    assert.deepEqual(
      yield* sql`SELECT disposition, ready FROM overseer_occurrences WHERE source_id = ${work.id}`,
      [{ disposition: "pending", ready: 1 }],
    );
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("replaces an unsent command identity after its durable rejection", () =>
  Effect.gen(function* () {
    const { start, question, prepare, sink } = yield* setup();
    yield* question(yield* start(1), "first");
    const first = yield* prepare();
    yield* sink.commitRejectedCommand({
      commandId: CommandId.make(first.command_id),
      threadId: observer,
      commandType: "message.dispatch",
      rejectedAt: now,
      error: "fixture",
    });
    const second = yield* prepare();
    assert.notEqual(first.command_id, second.command_id);
    assert.notEqual(first.message_id, second.message_id);
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect("an interrupted handoff releases the gate without clearing its submitted barrier", () =>
  Effect.gen(function* () {
    const { start, question, prepare, freeze, transport, store } = yield* setup();
    yield* question(yield* start(1), "first");
    const identity = yield* freeze(yield* prepare());
    const inside = yield* Deferred.make<void>();
    const send = yield* transport
      .handoff(identity, () =>
        Deferred.succeed(inside, undefined).pipe(Effect.andThen(Effect.never)),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(inside);
    yield* Fiber.interrupt(send);
    assert.equal((yield* transport.open(observer))!.phase, "submitted");
    yield* store.stop(observer);
    assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "explains the actual transport and scheduler queries without a feature or core overview scan",
  () =>
    Effect.gen(function* () {
      const { start, question, prepare, freeze, transport, store, sql } = yield* setup();
      yield* question(yield* start(1), "question");
      const queries: Array<readonly [string, ReadonlyArray<unknown>]> = [];
      const capture: Statement.Transformer = (statement) => {
        queries.push(statement.compile());
        return Effect.succeed(statement);
      };
      yield* Effect.gen(function* () {
        const identity = yield* freeze(yield* prepare());
        yield* transport.open(observer);
        yield* transport.handoff(identity, (enqueued) => enqueued);
        yield* transport.receipt({ ...identity, kind: "codex_response" });
        yield* store.touch(observer);
      }).pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      const plans: string[] = [];
      for (const [query, parameters] of queries) {
        if (!query.includes("overseer_")) continue;
        const rows = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${query}`,
          parameters,
        );
        const plan = rows.map((row) => row.detail).join("\n");
        assert.notMatch(plan, /SCAN (?:overseer_|orchestration_v2_projection_|b\b|m\b|r\b|o\b)/);
        plans.push(plan);
      }
      assert.isTrue(plans.some((plan) => plan.includes("overseer_transports_open_idx")));
      assert.isTrue(plans.some((plan) => plan.includes("sqlite_autoindex_overseer_transports_1")));
    }).pipe(enabled, Effect.provide(layer)),
);

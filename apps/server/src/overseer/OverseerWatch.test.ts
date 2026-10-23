import { assert, it } from "@effect/vitest";
import {
  EventId,
  NodeId,
  RuntimeRequestId,
  OversightWatchFrame,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as Gate from "./OverseerGate.ts";
import * as Store from "./OverseerStore.ts";
import * as Service from "./OverseerService.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import { harness, setup, enabled, observer, request } from "./OverseerLaunch.testkit.ts";

const encodeFrame = Schema.encodeEffect(OversightWatchFrame);

it.effect("real source commits update both endpoint watches and terminal statuses encode", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const launched = yield* saga.launch(request);
    const target = launched.projection.thread.id;
    const service = yield* Service.OverseerService;
    const sink = yield* Sink.EventSinkV2;
    const outgoing = yield* Stream.toPull(service.watch({ threadId: observer }));
    const incoming = yield* Stream.toPull(service.watch({ threadId: target }));
    const initialOut = (yield* outgoing)[0]!;
    const initialIn = (yield* incoming)[0]!;
    const now = DateTime.makeUnsafe("2026-10-23T00:00:00.000Z");
    yield* sink.write({
      events: [
        {
          id: EventId.make("rename"),
          type: "thread.metadata-updated",
          threadId: target,
          occurredAt: now,
          payload: { ...launched.projection.thread, title: "Renamed" },
        },
      ],
    });
    assert.equal((yield* outgoing)[0]!.links[0]!.title, "Renamed");
    assert.isAbove((yield* incoming)[0]!.revision, initialIn.revision);
    const run = launched.projection.runs[0]!;
    for (const status of ["queued", "completed", "cancelled", "rolled_back"] as const) {
      yield* sink.write({
        events: [
          {
            id: EventId.make(`status:${status}`),
            type: "run.updated",
            threadId: target,
            occurredAt: now,
            payload: { ...run, status },
          },
        ],
      });
      const frame = (yield* outgoing)[0]!;
      assert.equal(frame.links[0]!.workerStatus, status);
      assert.isAbove(frame.revision, initialOut.revision);
      yield* encodeFrame(frame);
    }
    yield* sink.write({
      events: [
        {
          id: EventId.make("question"),
          type: "runtime-request.updated",
          threadId: target,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: RuntimeRequestId.make("question"),
            nodeId: NodeId.make("question-node"),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        },
      ],
    });
    assert.equal((yield* outgoing)[0]!.links[0]!.pendingRequestCount, 1);
  }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "cancelling a gated operation releases the permit and conservatively invalidates its view",
  () =>
    Effect.gen(function* () {
      const gate = yield* Gate.OverseerGate;
      const sub = yield* gate.commit(gate.listen(["thread"]));
      const entered = yield* Deferred.make<void>();
      const fiber = yield* gate
        .commit(
          gate
            .changed("thread")
            .pipe(
              Effect.andThen(Deferred.succeed(entered, undefined)),
              Effect.andThen(Effect.never),
            ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      assert.equal(yield* Queue.take(sub.queue), yield* gate.revision);
      yield* gate.commit(Effect.void);
    }).pipe(enabled, Effect.provide(Gate.layer)),
);

it.effect(
  "clear and project deletion invalidate even an empty mounted page without scanning threads",
  () =>
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Service.OverseerService;
      const store = yield* Store.OverseerStore;
      const gate = yield* Gate.OverseerGate;
      const sql = yield* SqlClient.SqlClient;
      const pull = yield* Stream.toPull(service.watch({ threadId: observer }));
      yield* pull;
      yield* store.clearFeatureData;
      assert.deepEqual((yield* pull)[0]!.links, []);
      yield* gate.commit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE projection_projects SET deleted_at = '2026-10-23T00:00:00Z' WHERE project_id = 'project:a'`;
            yield* store.projectDeleted(ProjectId.make("project:a"));
          }),
        ),
      );
      assert.equal((yield* pull.pipe(Effect.flip))._tag, "OversightError");
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "a caught-up queued signal never rereads the page after a refresh blocked on the gate",
  () =>
    Effect.gen(function* () {
      const saga = yield* setup();
      yield* saga.launch(request);
      const service = yield* Service.OverseerService;
      const store = yield* Store.OverseerStore;
      const gate = yield* Gate.OverseerGate;
      let pageReads = 0;
      const capture: Statement.Transformer = (statement) => {
        if (statement.compile()[0].includes("WHERE state = 'active' AND observer_thread_id"))
          pageReads++;
        return Effect.succeed(statement);
      };
      const pull = yield* Stream.toPull(
        service
          .watch({ threadId: observer })
          .pipe(Stream.provideService(Statement.CurrentTransformer, capture)),
      );
      yield* pull;
      yield* store.hold(observer, "provider");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holding = yield* gate
        .commit(
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(gate.changed(observer)),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const refreshing = yield* pull.pipe(Effect.forkChild);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holding);
      yield* Fiber.join(refreshing);
      assert.equal(pageReads, 2);
      // A later real change lets the next pull drain any stale signal before its new frame.
      yield* store.resume(observer, "provider");
      yield* pull;
      assert.equal(pageReads, 3);
    }).pipe(enabled, Effect.provide(harness())),
);

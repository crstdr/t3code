import { assert, it } from "@effect/vitest";
import { MessageId, RunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import { continueRestartedRun } from "../orchestration-v2/RestartContinuation.ts";
import * as Ownership from "./OverseerContinuation.ts";
import * as Feature from "./OverseerFeature.ts";
import { enabled, layerServices, observer, setup, target } from "./OverseerMailbox.testkit.ts";

const layer = Layer.mergeAll(layerServices, Ownership.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);

it.effect.each(["frozen", "submitted", "received", "retired"] as const)(
  "only an accepted private wake permits generic continuation or delegated settlement (%s)",
  (phase) =>
    Effect.gen(function* () {
      const f = yield* setup();
      yield* f.question(yield* f.start(1), "pending");
      const row = yield* f.prepare();
      const identity = yield* f.freeze(row);
      if (phase === "submitted" || phase === "received")
        assert.isTrue(yield* f.transport.submit(identity));
      if (phase === "received")
        assert.isTrue(yield* f.transport.receipt({ ...identity, kind: "codex_response" }));
      if (phase === "retired") yield* f.transport.retire(identity.messageId);
      const expected = phase === "received" ? "ordinary" : "mailbox_owned";
      assert.equal(yield* Ownership.disposition({ runId: identity.runId }), expected);
      assert.equal(
        yield* Ownership.disposition({ runId: identity.runId, messageId: identity.messageId }),
        expected,
      );
      const projections = yield* Projections.ProjectionStoreV2;
      let settled = 0;
      yield* continueRestartedRun({ threadId: observer, sourceRunId: identity.runId }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Settings.layerTest({ continueThreadsAfterServerUpdate: false }),
            Layer.mock(Threads.ThreadManagementService)({
              getThreadRecords: (thread, fields, filter) =>
                projections.getThreadRecords(thread, fields, filter).pipe(Effect.orDie),
              recoverDelegatedTask: () =>
                Effect.sync(() => {
                  settled++;
                }),
            }),
          ),
        ),
      );
      assert.equal(settled, phase === "received" ? 1 : 0);
      assert.equal(
        yield* Ownership.disposition({ runId: identity.runId }).pipe(
          Effect.provideService(Feature.Enabled, false),
        ),
        expected,
      );
      if (phase === "received") {
        yield* continueRestartedRun({ threadId: observer, sourceRunId: identity.runId }).pipe(
          Effect.provide(
            Layer.mergeAll(
              Settings.layerTest({ continueThreadsAfterServerUpdate: false }),
              Layer.mock(Threads.ThreadManagementService)({
                getThreadRecords: (thread, fields, filter) =>
                  projections.getThreadRecords(thread, fields, filter).pipe(Effect.orDie),
                recoverDelegatedTask: () =>
                  Effect.sync(() => {
                    settled++;
                  }),
              }),
            ),
          ),
          Effect.provideService(Feature.Enabled, false),
        );
        assert.equal(settled, 2);
      }
      assert.equal(
        (yield* f.transport.open(observer))?.phase,
        phase === "retired" || phase === "received" ? undefined : phase,
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "genuine work, its continuation descendants and unowned notification-shaped input remain ordinary",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const work = yield* f.start(1);
      yield* f.question(work, "steered-question");
      const steer = yield* f.prepare();
      // A native steer is bound to the receiving work run, not its own private run.
      yield* f.sql`UPDATE overseer_transports SET phase = 'submitted', run_id = ${work.id} WHERE message_id = ${steer.message_id}`;
      const descendant = {
        ...work,
        id: RunId.make("work:descendant"),
        ordinal: 2,
        userMessageId: MessageId.make("work:restart"),
        restartContinuationOfRunId: work.id,
      };
      const unowned = {
        ...work,
        id: RunId.make("unowned"),
        ordinal: 3,
        userMessageId: MessageId.make("notification:unowned"),
      };
      yield* f.write([
        { type: "run.created", threadId: target, payload: descendant },
        { type: "run.created", threadId: target, payload: unowned },
      ]);
      const projections = yield* Projections.ProjectionStoreV2;
      for (const run of [work, descendant, unowned]) {
        assert.equal(yield* Ownership.disposition({ runId: run.id }), "ordinary");
        let settled = false;
        yield* continueRestartedRun({ threadId: target, sourceRunId: run.id }).pipe(
          Effect.provide(
            Layer.mergeAll(
              Settings.layerTest({ continueThreadsAfterServerUpdate: false }),
              Layer.mock(Threads.ThreadManagementService)({
                getThreadRecords: (thread, fields, filter) =>
                  projections.getThreadRecords(thread, fields, filter).pipe(Effect.orDie),
                recoverDelegatedTask: () =>
                  Effect.sync(() => {
                    settled = true;
                  }),
              }),
            ),
          ),
        );
        assert.isTrue(settled);
      }
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "disabled or missing feature ownership fails closed for the reserved private namespace only",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const work = yield* f.start(1);
      yield* f.question(work, "pending");
      const identity = yield* f.freeze(yield* f.prepare());
      assert.equal(
        yield* Ownership.disposition({ runId: identity.runId }).pipe(
          Effect.provideService(Feature.Enabled, false),
        ),
        "mailbox_owned",
      );
      yield* f.sql`DELETE FROM overseer_transports WHERE message_id = ${identity.messageId}`;
      assert.equal(yield* Ownership.disposition({ runId: identity.runId }), "mailbox_owned");
      assert.equal(yield* Ownership.disposition({ runId: work.id }), "ordinary");
    }).pipe(enabled, Effect.provide(layer)),
);

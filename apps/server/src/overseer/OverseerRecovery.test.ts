import { assert, it } from "@effect/vitest";
import {
  MessageId,
  ProviderSessionId,
  ProviderInstanceId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Statement from "effect/sql/Statement";
import * as Logger from "effect/Logger";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import * as Preferences from "./OverseerPreferences.ts";
import * as Policy from "./OverseerWakePolicy.ts";
import * as Idle from "./OverseerIdleScheduler.ts";
import { wakeText } from "./OverseerText.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as CoreRecovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Recovery from "./OverseerRecovery.ts";
import * as Ownership from "./OverseerContinuation.ts";
import * as Feature from "./OverseerFeature.ts";
import { enabled, layerServices, observer, setup, target } from "./OverseerMailbox.testkit.ts";

const services = Layer.mergeAll(layerServices, Recovery.layer, Ownership.layer).pipe(
  Layer.provide(Settings.layerTest()),
);
const atPath = (path: string, overrides?: Parameters<typeof Settings.layerTest>[0]) =>
  Layer.mergeAll(layerServices, Recovery.layer, Ownership.layer).pipe(
    Layer.provide(Settings.layerTest(overrides)),
    Layer.provideMerge(Sqlite.layerFromPath(path)),
  );
const recover = Effect.flatMap(Recovery.OverseerRecovery, (service) => service.recover);
const normalize = Effect.gen(function* () {
  const service = yield* CoreRecovery.make;
  yield* service.reconcile("startup");
}).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Settings.layerTest())));
const directory = Effect.flatMap(FileSystem.FileSystem, (fs) =>
  fs.makeTempDirectoryScoped({ prefix: "t3-overseer-recovery-" }),
);

it.effect(
  "fresh SQLite services recover the submitted idle check-in without inventing an occurrence",
  () =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const idleLayers = () =>
        Layer.mergeAll(Preferences.layer, Idle.layer).pipe(Layer.provideMerge(atPath(path)));
      const before = yield* Effect.gen(function* () {
        const fixture = yield* setup();
        const preferences = yield* Preferences.OverseerPreferences;
        yield* preferences.configure(observer, {
          ...Policy.defaults,
          idleEnabled: true,
          idleMinutes: 10,
        });
        yield* TestClock.adjust("10 minutes");
        yield* (yield* Idle.OverseerIdleScheduler).runDue;
        yield* fixture.sync(() => Effect.void);
        const row = (yield* fixture.transport.open(observer))!;
        const identity = yield* fixture.freeze(row);
        assert.isTrue(yield* fixture.transport.submit(identity));
        return row;
      }).pipe(enabled, Effect.provide(idleLayers()));
      yield* Effect.gen(function* () {
        const fixture = yield* setup({ seed: false, eventPrefix: "idle-reopen:" });
        yield* normalize;
        yield* recover;
        yield* fixture.sync(() => Effect.void);
        const row = (yield* fixture.transport.open(observer))!;
        assert.equal(row.batch_number, before.batch_number);
        assert.notEqual(row.message_id, before.message_id);
        assert.equal(row.text, wakeText.W3);
        assert.equal(row.members_json, "[]");
        assert.deepEqual(yield* fixture.sql`SELECT * FROM overseer_occurrences`, []);
        const identity = yield* fixture.freeze(row, 101);
        assert.isTrue(yield* fixture.transport.submit(identity));
        assert.isTrue(yield* fixture.transport.receipt({ ...identity, kind: "codex_response" }));
        yield* recover;
        assert.isUndefined(yield* fixture.transport.open(observer));
      }).pipe(enabled, Effect.provide(idleLayers()));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "source",
  "prepared",
  "queued",
  "running_flush",
  "frozen",
  "submitted",
  "received",
] as const)(
  "fresh SQLite services recover the %s crash boundary without losing or double accepting facts",
  (boundary) =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const before = yield* Effect.gen(function* () {
        const fixture = yield* setup();
        const work = yield* fixture.start(1);
        yield* fixture.question(work, "question:1");
        if (boundary === "source") {
          // The committed source is sufficient even if its private effect disappeared.
          yield* fixture.sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush'`;
          return undefined;
        }
        if (boundary === "running_flush")
          yield* fixture.outbox.claimNext({ workerId: "dead-worker", leaseDurationMs: 1000 });
        const row = yield* fixture.prepare();
        if (boundary === "prepared") return row;
        if (boundary === "queued" || boundary === "running_flush") {
          yield* fixture.freeze(row, 100, "queued");
          return row;
        }
        const identity = yield* fixture.freeze(row);
        if (boundary !== "frozen") assert.isTrue(yield* fixture.transport.submit(identity));
        if (boundary === "received")
          assert.isTrue(yield* fixture.transport.receipt({ ...identity, kind: "codex_response" }));
        return row;
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const fixture = yield* setup({ seed: false, eventPrefix: "reopened:" });
        yield* normalize;
        yield* recover;
        if (boundary === "received") {
          assert.isUndefined(yield* fixture.transport.open(observer));
          assert.equal(
            (yield* fixture.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`)
              .length,
            1,
          );
          return;
        }
        const box = (yield* fixture.store.mailbox(observer))!;
        assert.isDefined(box.flush_id);
        assert.equal(
          (yield* fixture.sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_id = ${box.flush_id} AND status = 'pending'`)
            .length,
          1,
        );
        assert.equal(
          (yield* fixture.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`)
            .length,
          0,
        );
        if (boundary === "prepared" || boundary === "queued" || boundary === "running_flush") {
          assert.equal((yield* fixture.transport.open(observer))?.message_id, before?.message_id);
          if (boundary !== "prepared") {
            const p = yield* Projections.ProjectionStoreV2;
            const queued = (yield* p.getRuntimeRecoveryProjection(observer)).runs.find(
              (run) => run.id === "wake:100",
            )!;
            assert.equal(queued.status, "queued");
            assert.isTrue(queued.queueHeld);
            yield* fixture.sync(() => Effect.die("recovery cannot resume a held queue"));
            yield* fixture.write(
              [
                {
                  type: "run.updated",
                  threadId: observer,
                  payload: { ...queued, queueHeld: false },
                },
              ],
              "queue.resume",
            );
          }
          let dispatched = false;
          yield* fixture.sync(() =>
            Effect.sync(() => {
              dispatched = true;
            }),
          );
          assert.isTrue(dispatched);
          const refreshed = (yield* fixture.transport.open(observer))!;
          const identity = yield* fixture.freeze(refreshed);
          assert.isTrue(yield* fixture.transport.submit(identity));
          assert.isTrue(yield* fixture.transport.receipt({ ...identity, kind: "codex_response" }));
          return;
        }
        if (before) {
          assert.isUndefined(yield* fixture.transport.open(observer));
          assert.isTrue(yield* fixture.transport.restoreRetry(observer));
          const next = yield* fixture.prepare();
          assert.equal(next.batch_number, before.batch_number);
          assert.notEqual(next.message_id, before.message_id);
          const identity = yield* fixture.freeze(next, 101);
          assert.isTrue(yield* fixture.transport.submit(identity));
          // Calling startup twice inside this process cannot invent another loss.
          yield* recover;
          assert.equal((yield* fixture.transport.open(observer))?.phase, "submitted");
          assert.isFalse(
            yield* fixture.transport.receipt({
              ...identity,
              messageId: MessageId.make(before.message_id),
              kind: "codex_response",
            }),
          );
          assert.isTrue(yield* fixture.transport.receipt({ ...identity, kind: "codex_response" }));
          assert.isNull((yield* fixture.store.mailbox(observer))!.retry_message_id);
        }
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "submitted invalidation retains its uncertainty barrier, then retries surviving frozen members under the same batch ID",
  () =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const original = yield* Effect.gen(function* () {
        const f = yield* setup();
        const work = yield* f.start(1);
        const question = yield* f.question(work, "first");
        yield* f.question(work, "survivor");
        const row = yield* f.prepare();
        assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
        yield* f.write([
          {
            type: "runtime-request.updated",
            threadId: target,
            runId: work.id,
            payload: { ...question, status: "resolved" },
          },
        ]);
        yield* f.question(work, "later");
        assert.equal((yield* f.transport.open(observer))?.phase, "submitted");
        assert.isTrue(yield* f.transport.restoreRetry(observer));
        assert.equal((yield* f.transport.open(observer))?.message_id, row.message_id);
        return row;
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false, eventPrefix: "next:" });
        yield* recover;
        assert.isTrue(yield* f.transport.restoreRetry(observer));
        yield* f.sync((command) =>
          Effect.sync(() => {
            assert.equal(command.type, "message.dispatch");
            if (command.type === "message.dispatch")
              assert.include(
                command.text,
                `Oversight updates (batch ${observer}:${original.batch_number}).`,
              );
          }),
        );
        const next = (yield* f.transport.open(observer))!;
        assert.equal(next.batch_number, original.batch_number);
        assert.deepEqual(JSON.parse(next.members_json), [
          { linkId: "link", kind: "question", sourceId: "survivor" },
        ]);
        const identity = yield* f.freeze(next, 101);
        assert.isFalse(yield* f.transport.receipt({ ...identity, kind: "codex_response" }));
        // No receipt can settle a merely frozen attempt.
        assert.equal((yield* f.transport.open(observer))?.phase, "frozen");
        assert.isTrue(yield* f.transport.submit(identity));
        assert.isTrue(yield* f.transport.receipt({ ...identity, kind: "codex_response" }));
        assert.equal(
          (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE source_id = 'later' AND disposition = 'pending' AND batch_number IS NULL`)
            .length,
          1,
        );
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "disabled startup preserves dedupe and links but requires explicit rearm after re-enable",
  () =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const original = yield* Effect.gen(function* () {
        const f = yield* setup();
        yield* f.question(yield* f.start(1), "pending");
        const row = yield* f.prepare();
        assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
        return row;
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* recover;
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
        assert.equal((yield* f.sql`SELECT 1 FROM overseer_links WHERE state = 'active'`).length, 1);
        assert.equal(
          (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'pending'`).length,
          2,
        );
        assert.equal(
          (yield* f.sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush' AND status IN ('pending', 'running')`)
            .length,
          0,
        );
      }).pipe(Effect.provideService(Feature.Enabled, false), Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* recover;
        assert.isFalse(yield* f.transport.restoreRetry(observer));
        yield* f.store.resume(observer, "authorization");
        assert.isTrue(yield* f.transport.restoreRetry(observer));
        assert.equal((yield* f.prepare()).batch_number, original.batch_number);
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "core startup expires dead live requests before mailbox recovery while retaining message-answerable questions",
  () =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      yield* Effect.gen(function* () {
        const f = yield* setup();
        const work = yield* f.start(1);
        yield* f.question(work, "message-question");
        const live = yield* f.question(work, "live-question");
        yield* f.write([
          {
            type: "runtime-request.updated",
            threadId: target,
            runId: work.id,
            payload: {
              ...live,
              responseCapability: {
                type: "live",
                providerSessionId: ProviderSessionId.make("dead-session"),
              },
            },
          },
        ]);
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* normalize;
        yield* recover;
        const rows = yield* f.sql<{
          readonly source_id: string;
          readonly disposition: string;
        }>`SELECT source_id, disposition FROM overseer_occurrences WHERE kind = 'question' ORDER BY source_id`;
        assert.deepEqual(rows, [
          { source_id: "live-question", disposition: "withdrawn" },
          { source_id: "message-question", disposition: "pending" },
        ]);
        assert.equal(
          (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE kind = 'completion' AND ready = 1`)
            .length,
          1,
        );
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "a disabled provider parks uncertain facts without input or a retry loop, then explicit re-enable recovers the same batch",
  () =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const before = yield* Effect.gen(function* () {
        const f = yield* setup();
        yield* f.question(yield* f.start(1), "pending");
        const row = yield* f.prepare();
        assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
        return row;
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* normalize;
        yield* recover;
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 8, 8);
        assert.isFalse(yield* f.transport.restoreRetry(observer));
        yield* f.sync(() => Effect.die("disabled provider must not receive input"));
        assert.equal(
          (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
          0,
        );
      }).pipe(
        enabled,
        Effect.provide(
          atPath(path, {
            projectSettingsOverrides: {
              [ProjectId.make("project")]: {
                providerInstances: { [ProviderInstanceId.make("codex")]: { enabled: false } },
              },
            },
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* recover;
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 8, 0);
        assert.isTrue(yield* f.transport.restoreRetry(observer));
        assert.equal((yield* f.prepare()).batch_number, before.batch_number);
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["abandon", "failed_run"] as const)(
  "a failure-induced %s hold survives fresh SQLite recovery until explicit queue control",
  (failure) =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      yield* Effect.gen(function* () {
        const f = yield* setup();
        yield* f.question(yield* f.start(1), "pending");
        const row = yield* f.prepare();
        const identity = yield* f.freeze(row);
        if (failure === "abandon") yield* f.transport.abandonUnsent(identity.messageId);
        const projection = yield* Projections.ProjectionStoreV2;
        const wake = (yield* projection.getThreadProjection(observer)).runs.find(
          (run) => run.id === identity.runId,
        )!;
        yield* f.write([
          {
            type: "run.updated",
            threadId: observer,
            payload: { ...wake, status: "failed", completedAt: yield* DateTime.now },
          },
        ]);
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask, 24);
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false, eventPrefix: "failure-reopen:" });
        yield* normalize;
        yield* recover;
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
        yield* f.sync(() => Effect.die("restart is not an explicit failure rearm"));
        assert.equal(
          (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
          0,
        );
        yield* f.write([], "queue.resume", observer);
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask, 0);
        let dispatched = false;
        yield* f.sync(() =>
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isTrue(dispatched);
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["stop", "unlink", "archive", "delete", "project_delete"] as const)(
  "recovery preserves the %s fence instead of resurrecting uncertain input",
  (fence) =>
    Effect.gen(function* () {
      const path = `${yield* directory}/state.sqlite`;
      const original = yield* Effect.gen(function* () {
        const f = yield* setup();
        yield* f.question(yield* f.start(1), "pending");
        const row = yield* f.prepare();
        assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
        if (fence === "stop") yield* f.store.stop(observer);
        else if (fence === "unlink") yield* f.store.unlink("link");
        else if (fence === "project_delete")
          yield* f.sql`UPDATE projection_projects SET deleted_at = '2026-10-21T00:00:00.000Z' WHERE project_id = 'project'`;
        else {
          const p = yield* Projections.ProjectionStoreV2;
          const thread = yield* p.getThread(target);
          const at = yield* DateTime.now;
          // A core lifecycle commit while disabled is deliberately not observed.
          yield* f
            .write([
              {
                type: fence === "archive" ? "thread.archived" : "thread.deleted",
                threadId: target,
                payload: {
                  ...thread,
                  archivedAt: fence === "archive" ? at : null,
                  deletedAt: fence === "delete" ? at : null,
                },
              },
            ])
            .pipe(Effect.provideService(Feature.Enabled, false));
        }
        return row;
      }).pipe(enabled, Effect.provide(atPath(path)));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false, eventPrefix: "fence:" });
        yield* normalize;
        yield* recover;
        assert.isUndefined(yield* f.transport.open(observer));
        if (fence === "archive") {
          assert.isTrue(yield* f.transport.restoreRetry(observer));
          assert.isNull((yield* f.store.mailbox(observer))!.retry_message_id);
          const p = yield* Projections.ProjectionStoreV2;
          yield* f.write([
            {
              type: "thread.unarchived",
              threadId: target,
              payload: { ...(yield* p.getThread(target)), archivedAt: null },
            },
          ]);
          assert.isTrue(yield* f.transport.restoreRetry(observer));
          assert.notEqual((yield* f.prepare()).batch_number, original.batch_number);
        } else {
          assert.isTrue(yield* f.transport.restoreRetry(observer));
          assert.isNull((yield* f.store.mailbox(observer))!.retry_message_id);
          assert.equal(
            (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE kind = 'question' AND disposition = 'pending'`)
              .length,
            0,
          );
          if (fence === "stop") assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 1, 1);
        }
      }).pipe(enabled, Effect.provide(atPath(path)));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "startup pages only indexed feature identities and exact sources despite unrelated core threads",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const p = yield* Projections.ProjectionStoreV2;
      const worker = yield* p.getThread(target);
      for (let index = 0; index < 300; index++) {
        const id = ThreadId.make(`unrelated:${index}`);
        yield* f.write([{ type: "thread.created", threadId: id, payload: { ...worker, id } }]);
      }
      const work = yield* f.start(1);
      for (let index = 0; index < 110; index++) yield* f.question(work, `question:${index}`);
      const row = yield* f.prepare();
      assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
      const queries: Array<ReturnType<Statement.Statement<unknown>["compile"]>> = [];
      const capture: Statement.Transformer = (statement) => {
        const compiled = statement.compile();
        if (/^\s*(?:SELECT|UPDATE|INSERT|DELETE)\b/i.test(compiled[0])) queries.push(compiled);
        return Effect.succeed(statement);
      };
      yield* recover.pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      const allPlans: string[] = [];
      for (const [query, parameters] of queries) {
        const plan = (yield* f.sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${query}`,
          parameters,
        ))
          .map((row) => row.detail)
          .join("\n");
        assert.notMatch(
          plan,
          /SCAN (?:orchestration_v2_projection_\w+|projection_projects|[rtlpo])(?:\s|$)/,
        );
        assert.notMatch(plan, /SCAN overseer_\w+(?:\n|$)/);
        assert.notMatch(query, /FROM orchestration_v2_projection_(?:messages|turn_items)/);
        allPlans.push(plan);
      }
      const combined = allPlans.join("\n");
      for (const index of [
        "overseer_links_active_idx",
        "overseer_links_observer_idx",
        "overseer_mailboxes_dirty_idx",
        "overseer_mailboxes_held_idx",
        "overseer_mailboxes_retry_idx",
        "overseer_transports_open_idx",
        "overseer_occurrences_pending_idx",
        "overseer_occurrences_candidates_idx",
      ])
        assert.include(combined, index);
      assert.isAtLeast(
        queries.filter(([query]) =>
          query.includes("ORDER BY source_sequence, kind, source_id, link_id LIMIT 100"),
        ).length,
        2,
      );
      assert.equal(yield* f.transport.open(observer), undefined);
    }).pipe(enabled, Effect.provide(services.pipe(Layer.provideMerge(Sqlite.layerMemory)))),
);

it.effect(
  "an unreceipted terminal input warns once, retains durable submission age and never resends",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      yield* f.question(yield* f.start(1), "pending");
      const row = yield* f.prepare();
      const identity = yield* f.freeze(row);
      assert.isTrue(yield* f.transport.submit(identity));
      const projections = yield* Projections.ProjectionStoreV2;
      const run = (yield* projections.getRuntimeRecoveryProjection(observer)).runs.find(
        (run) => run.id === identity.runId,
      )!;
      const warnings: string[] = [];
      const logger = Logger.layer(
        [
          Logger.make(({ message }) => {
            warnings.push(String(message));
          }),
        ],
        { mergeWithExisting: false },
      );
      const events = yield* f
        .write([
          { type: "run.updated", threadId: observer, payload: { ...run, status: "completed" } },
        ])
        .pipe(Effect.provide(logger));
      const current = (yield* f.transport.open(observer))!;
      assert.equal(current.phase, "submitted");
      assert.isNotNull(current.submitted_at);
      assert.isNotNull(current.unconfirmed_logged_at);
      yield* f.transport.warnUnconfirmed(events).pipe(Effect.provide(logger));
      assert.equal(
        warnings.filter((message) =>
          message.includes("Overseer input ended without a provider receipt"),
        ).length,
        1,
      );
      assert.deepEqual(yield* f.transport.open(observer), current);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
        0,
      );
      yield* f.sync(() => Effect.die("must not resend"));
    }).pipe(enabled, Effect.provide(services.pipe(Layer.provideMerge(Sqlite.layerMemory)))),
);

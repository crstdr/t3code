import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ThreadId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Settings from "../serverSettings.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as Recovery from "./OverseerRecovery.ts";
import * as Startup from "./OverseerStartup.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Ownership from "./OverseerContinuation.ts";
import { enabled, layerServices, observer, setup, target } from "./OverseerMailbox.testkit.ts";

const services = Layer.mergeAll(layerServices, Ownership.layer);
const layer = services.pipe(Layer.provideMerge(Sqlite.layerMemory));
const recoveryLayer = Recovery.layer.pipe(Layer.provide(Settings.layerTest()));
const recover = Recovery.OverseerRecovery.use((service) => service.recover).pipe(
  Effect.provide(recoveryLayer),
);

it.effect.each([true, false])(
  "rejected private cleanup cannot fail startup (enabled=%s) or prevent recovery of another observer",
  (flag) =>
    Effect.gen(function* () {
      const f = yield* setup();
      const p = yield* Projections.ProjectionStoreV2;
      const other = ThreadId.make("other");
      const shell = yield* p.getThread(observer);
      yield* f.write([
        { type: "thread.created", threadId: other, payload: { ...shell, id: other } },
      ]);
      yield* f.store.reserve({
        linkId: "other-link",
        observerThreadId: other,
        observerProjectId: shell.projectId,
        targetThreadId: ThreadId.make("other-worker"),
        targetProjectId: shell.projectId,
        clientRequestId: "other",
        fingerprint: "input",
        launchCommandId: CommandId.make("launch:other"),
        launchMessageId: MessageId.make("initial:other"),
        launchInputJson: "{}",
      });
      yield* f.question(yield* f.start(1), "pending");
      const row = yield* f.prepare();
      yield* f.freeze(row, 100, "queued");
      yield* f.transport.retire(MessageId.make(row.message_id));
      yield* f.store.touch(other);
      yield* f.sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush'`;
      let rejected = 0;
      const dependencies = Layer.mergeAll(
        Settings.layerTest(),
        Layer.mock(Orchestrator.OrchestratorV2)({
          dispatch: (command) => {
            rejected++;
            return new Orchestrator.OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause: "fixture rejection",
            });
          },
        }),
      );
      yield* Startup.recover.pipe(
        Effect.provide(Recovery.layer.pipe(Layer.provide(dependencies))),
        Effect.provideService(Feature.Enabled, flag),
      );
      assert.equal(rejected, 1);
      assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
      const otherBox = (yield* f.store.mailbox(other))!;
      assert.equal(otherBox.hold_mask & 16, flag ? 0 : 16);
      assert.equal((yield* f.sql`SELECT 1 FROM overseer_links WHERE state = 'active'`).length, 2);
      if (flag)
        assert.equal(
          (yield* f.sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE effect_id = ${otherBox.flush_id} AND status = 'pending'`)
            .length,
          1,
        );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect("disabled recovery never reads settings, with or without feature obligations", () =>
  Effect.gen(function* () {
    const settings = Layer.mock(Settings.ServerSettingsService)({
      getSettings: Effect.die("disabled recovery must not read settings"),
    });
    yield* Recovery.OverseerRecovery.use((service) => service.recover).pipe(
      Effect.provide(Recovery.layer.pipe(Layer.provide(settings))),
    );
    const f = yield* setup();
    yield* f.question(yield* f.start(1), "pending");
    yield* Recovery.OverseerRecovery.use((service) => service.recover).pipe(
      Effect.provide(Recovery.layer.pipe(Layer.provide(settings))),
    );
    assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
  }).pipe(Effect.provideService(Feature.Enabled, false), Effect.provide(layer)),
);

it.effect(
  "a crash in disabled retirement cannot commit retry provenance without its authorization hold",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-overseer-disarm-" });
      const path = `${directory}/state.sqlite`;
      const atPath = services.pipe(Layer.provideMerge(Sqlite.layerFromPath(path)));
      const batch = yield* Effect.gen(function* () {
        const f = yield* setup();
        yield* f.question(yield* f.start(1), "pending");
        const row = yield* f.prepare();
        assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
        yield* f.sql`CREATE TRIGGER fail_retirement BEFORE UPDATE OF phase ON overseer_transports WHEN NEW.phase = 'retired' BEGIN SELECT RAISE(ABORT, 'crash fixture'); END`;
        yield* recover.pipe(Effect.provideService(Feature.Enabled, false));
        assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
        assert.equal((yield* f.transport.open(observer))?.phase, "submitted");
        yield* f.sql`DROP TRIGGER fail_retirement`;
        return row.batch_number;
      }).pipe(enabled, Effect.provide(atPath));
      yield* Effect.gen(function* () {
        const f = yield* setup({ seed: false });
        yield* recover;
        assert.isFalse(yield* f.transport.restoreRetry(observer));
        yield* f.sync(() => Effect.die("must remain disarmed"));
        yield* f.store.resume(observer, "authorization");
        assert.isTrue(yield* f.transport.restoreRetry(observer));
        assert.equal((yield* f.prepare()).batch_number, batch);
      }).pipe(enabled, Effect.provide(atPath));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["observer", "worker", "project"])(
  "a missing %s projection parks rather than irreversibly closing canonical obligations",
  (missing) =>
    Effect.gen(function* () {
      const f = yield* setup();
      yield* f.store.acceptLaunch("link");
      yield* f.question(yield* f.start(1), "pending");
      if (missing === "project")
        yield* f.sql`DELETE FROM projection_projects WHERE project_id = 'project'`;
      else
        yield* f.sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${missing}`;
      yield* recover;
      assert.equal((yield* f.store.mailbox(observer))!.hold_mask & 16, 16);
      assert.equal((yield* f.sql`SELECT 1 FROM overseer_links WHERE state = 'active'`).length, 1);
      assert.equal(
        (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'pending'`).length,
        2,
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "a deferred original does not block eligible originals, and remains pending after their receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const work = yield* f.start(1);
      yield* f.question(work, "pending");
      yield* f.write([
        { type: "run.updated", threadId: target, payload: { ...work, status: "completed" } },
      ]);
      const row = yield* f.prepare();
      assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
      yield* recover;
      yield* f.store.deferCompletion("link", work.id);
      assert.isTrue(yield* f.transport.restoreRetry(observer));
      const retry = yield* f.prepare();
      assert.equal(retry.batch_number, row.batch_number);
      assert.deepEqual(JSON.parse(retry.members_json), [
        { linkId: "link", kind: "question", sourceId: "pending" },
      ]);
      const identity = yield* f.freeze(retry, 101);
      assert.isTrue(yield* f.transport.submit(identity));
      assert.isTrue(yield* f.transport.receipt({ ...identity, kind: "codex_response" }));
      assert.equal(
        (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE kind = 'completion' AND disposition = 'pending' AND ready = 0 AND batch_number IS NULL`)
          .length,
        1,
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "manual cancellation of an unstarted private input closes its batch even without an attempt",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      yield* f.question(yield* f.start(1), "pending");
      const row = yield* f.prepare();
      const identity = yield* f.freeze(row, 100, "queued");
      const p = yield* Projections.ProjectionStoreV2;
      const run = (yield* p.getRuntimeRecoveryProjection(observer)).runs.find(
        (run) => run.id === identity.runId,
      )!;
      yield* f.write(
        [{ type: "run.updated", threadId: observer, payload: { ...run, status: "cancelled" } }],
        "queued-run.cancel",
      );
      assert.equal(
        (yield* f.sql<{
          readonly state: string;
        }>`SELECT state FROM overseer_batches WHERE observer_thread_id = ${observer} AND batch_number = ${row.batch_number}`)[0]
          ?.state,
        "suppressed",
      );
      assert.isUndefined(yield* f.transport.open(observer));
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "a restored frozen snapshot cannot accept a suffix omitted by the actual envelope formatter",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const work = yield* f.start(1);
      yield* f.question(work, "short");
      yield* f.question(work, "x".repeat(1900));
      // Fault fixture: an older/corrupt transport claimed a snapshot larger than W1.
      const row = yield* f.prepare("legacy fixture");
      assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
      yield* recover;
      yield* f.sync(() => Effect.die("partial frozen membership must not be offered"));
      assert.isUndefined(yield* f.transport.open(observer));
      assert.equal(
        (yield* f.sql`SELECT 1 FROM overseer_occurrences WHERE disposition = 'accepted'`).length,
        0,
      );
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "one sync releases an empty retry snapshot and stages later arrivals with a fresh fence",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const work = yield* f.start(1);
      const first = yield* f.question(work, "first");
      const row = yield* f.prepare();
      assert.isTrue(yield* f.transport.submit(yield* f.freeze(row)));
      yield* f.write([
        {
          type: "runtime-request.updated",
          threadId: target,
          runId: work.id,
          payload: { ...first, status: "resolved" },
        },
      ]);
      yield* f.question(work, "later");
      yield* recover;
      let dispatched = false;
      yield* f.sync((command) =>
        Effect.sync(() => {
          assert.equal(command.type, "message.dispatch");
          if (command.type === "message.dispatch") {
            assert.include(command.text, "request later:");
            assert.notInclude(command.text, "request first:");
          }
          dispatched = true;
        }),
      );
      assert.isTrue(dispatched);
      assert.isNull((yield* f.store.mailbox(observer))!.retry_message_id);
      assert.notEqual((yield* f.transport.open(observer))!.batch_number, row.batch_number);
    }).pipe(enabled, Effect.provide(layer)),
);

it.effect("a terminal queued binding is retired instead of permanently blocking valid facts", () =>
  Effect.gen(function* () {
    const f = yield* setup();
    yield* f.question(yield* f.start(1), "pending");
    const row = yield* f.prepare();
    const identity = yield* f.freeze(row, 100, "queued");
    const p = yield* Projections.ProjectionStoreV2;
    const run = (yield* p.getRuntimeRecoveryProjection(observer)).runs.find(
      (run) => run.id === identity.runId,
    )!;
    yield* f.write(
      [{ type: "run.updated", threadId: observer, payload: { ...run, status: "cancelled" } }],
      "provider-runtime.reconcile",
    );
    let dispatched = false;
    yield* f.sync(() =>
      Effect.sync(() => {
        dispatched = true;
      }),
    );
    assert.isTrue(dispatched);
    assert.notEqual((yield* f.transport.open(observer))?.message_id, row.message_id);
  }).pipe(enabled, Effect.provide(layer)),
);

it.effect(
  "recovery crosses active-link and mailbox page boundaries without dropping clean observers",
  () =>
    Effect.gen(function* () {
      const f = yield* setup();
      const shell = yield* (yield* Projections.ProjectionStoreV2).getThread(observer);
      for (let index = 0; index < 105; index++) {
        const id = ThreadId.make(`observer:${String(index).padStart(3, "0")}`);
        yield* f.write([{ type: "thread.created", threadId: id, payload: { ...shell, id } }]);
        yield* f.store.reserve({
          linkId: `link:${index}`,
          observerThreadId: id,
          observerProjectId: shell.projectId,
          targetThreadId: ThreadId.make(`worker:${index}`),
          targetProjectId: shell.projectId,
          clientRequestId: "retry",
          fingerprint: "input",
          launchCommandId: CommandId.make(`launch:${index}`),
          launchMessageId: MessageId.make(`initial:${index}`),
          launchInputJson: "{}",
        });
        yield* f.store.touch(id);
      }
      yield* f.question(yield* f.start(1), "pending");
      yield* f.sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_type = 'overseer.mailbox.flush'`;
      yield* recover;
      assert.equal(
        (yield* f.sql`SELECT 1 FROM overseer_mailboxes b JOIN orchestration_v2_effect_outbox e ON e.effect_id = b.flush_id WHERE e.status = 'pending'`)
          .length,
        106,
      );
      assert.equal((yield* f.sql`SELECT 1 FROM overseer_links WHERE state = 'active'`).length, 106);
    }).pipe(enabled, Effect.provide(layer)),
);

import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Gate from "./OverseerGate.ts";
import * as Store from "./OverseerStore.ts";
import * as Policy from "./OverseerWakePolicy.ts";

export class IdleScheduleError extends Schema.TaggedError<IdleScheduleError>()(
  "OverseerIdleScheduleError",
  { cause: Schema.Defect() },
) {}
export class OverseerIdleScheduler extends Context.Service<
  OverseerIdleScheduler,
  {
    readonly runDue: Effect.Effect<void, IdleScheduleError>;
  }
>()("t3/overseer/OverseerIdleScheduler") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.OverseerStore;
  const gate = yield* Gate.OverseerGate;
  const outbox = yield* Outbox.EffectOutboxV2;
  const runDue = Effect.fn("OverseerIdleScheduler.runDue")(
    function* () {
      if (!(yield* Feature.Enabled)) return;
      const now = DateTime.formatIso(yield* DateTime.now);
      let afterDue = "";
      let afterObserver = "";
      let changed = false;
      for (;;) {
        const rows = yield* sql<{
          readonly observer_thread_id: string;
          readonly idle_due_at: string;
        }>`SELECT observer_thread_id, idle_due_at
        FROM overseer_preferences WHERE idle_enabled = 1 AND idle_due_at IS NOT NULL AND idle_pending_token IS NULL
          AND idle_due_at <= ${now} AND (idle_due_at, observer_thread_id) > (${afterDue}, ${afterObserver})
        ORDER BY idle_due_at, observer_thread_id LIMIT 100`;
        if (rows.length === 0) break;
        for (const row of rows)
          yield* gate.commit(
            sql.withTransaction(
              Effect.gen(function* () {
                const observer = ThreadId.make(row.observer_thread_id);
                if (!(yield* Policy.genuinelyIdle(observer))) {
                  yield* Policy.syncIdleState(observer);
                  return;
                }
                const claimed =
                  yield* sql`UPDATE overseer_preferences SET idle_pending_token = lower(hex(randomblob(16))), idle_due_at = NULL
          WHERE observer_thread_id = ${observer} AND idle_enabled = 1 AND idle_pending_token IS NULL AND idle_due_at = ${row.idle_due_at} RETURNING 1`;
                if (claimed.length === 0) return;
                yield* store.touch(observer);
                changed = true;
              }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
            ),
          );
        const last = rows[rows.length - 1]!;
        afterDue = last.idle_due_at;
        afterObserver = last.observer_thread_id;
      }
      if (changed) yield* outbox.notifyAvailable();
    },
    Effect.mapError((cause) => new IdleScheduleError({ cause })),
  );
  return OverseerIdleScheduler.of({ runDue: runDue() });
});
export const layer = Layer.effect(OverseerIdleScheduler, make).pipe(
  Layer.provide(Layer.mergeAll(Store.layer, Gate.layer, Outbox.layer)),
);
export const layerWorker = Layer.effectDiscard(
  Effect.gen(function* () {
    if (!(yield* Feature.Enabled)) return;
    const scheduler = yield* Scheduler.Scheduler;
    const idle = yield* OverseerIdleScheduler;
    yield* scheduler.register("overseer-idle", idle.runDue);
  }),
).pipe(Layer.provide(layer));

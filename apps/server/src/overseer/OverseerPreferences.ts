import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Feature from "./OverseerFeature.ts";
import * as Store from "./OverseerStore.ts";
import * as Gate from "./OverseerGate.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Policy from "./OverseerWakePolicy.ts";

export class PreferencesError extends Schema.TaggedError<PreferencesError>()(
  "OverseerPreferencesError",
  { cause: Schema.Defect() },
) {}
export class OverseerPreferences extends Context.Service<
  OverseerPreferences,
  {
    readonly get: (observer: ThreadId) => Effect.Effect<Policy.Preferences, PreferencesError>;
    readonly configure: (
      observer: ThreadId,
      settings: Policy.Preferences,
    ) => Effect.Effect<Policy.Preferences, PreferencesError>;
  }
>()("t3/overseer/OverseerPreferences") {}

const decodePreferences = Schema.decodeUnknownEffect(Policy.Preferences);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.OverseerStore;
  const gate = yield* Gate.OverseerGate;
  const outbox = yield* Outbox.EffectOutboxV2;
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError((cause) => new PreferencesError({ cause })));
  return OverseerPreferences.of({
    get: Effect.fn("OverseerPreferences.get")((observer) =>
      run(
        Policy.preferences(observer).pipe(
          Effect.map(Policy.decodePreferences),
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
      ),
    ),
    configure: Effect.fn("OverseerPreferences.configure")(function* (observer, input) {
      if (!(yield* Feature.Enabled)) return yield* new PreferencesError({ cause: "disabled" });
      const settings = yield* run(decodePreferences(input));
      yield* run(
        gate.commit(
          sql.withTransaction(
            Effect.gen(function* () {
              const exists = yield* store.mailbox(observer);
              if (!exists) return yield* new PreferencesError({ cause: "unknown observer" });
              const previous = yield* Policy.preferences(observer);
              const before = Policy.decodePreferences(previous);
              if (
                previous &&
                settings.autoWake === before.autoWake &&
                settings.cadenceEnabled === before.cadenceEnabled &&
                settings.cadenceMinutes === before.cadenceMinutes &&
                settings.idleEnabled === before.idleEnabled &&
                settings.idleMinutes === before.idleMinutes
              )
                return;
              yield* sql`INSERT INTO overseer_preferences(observer_thread_id, auto_wake, cadence_enabled, cadence_minutes, idle_enabled, idle_minutes)
          VALUES (${observer}, ${settings.autoWake ? 1 : 0}, ${settings.cadenceEnabled ? 1 : 0}, ${settings.cadenceMinutes}, ${settings.idleEnabled ? 1 : 0}, ${settings.idleMinutes})
          ON CONFLICT(observer_thread_id) DO UPDATE SET auto_wake = excluded.auto_wake,
            cadence_enabled = excluded.cadence_enabled, cadence_minutes = excluded.cadence_minutes,
            idle_enabled = excluded.idle_enabled, idle_minutes = excluded.idle_minutes`;
              const batches = yield* sql<{
                readonly batch_number: number;
              }>`SELECT batch_number FROM overseer_batches b
          WHERE observer_thread_id = ${observer} AND state IN ('queued', 'frozen')
            AND NOT EXISTS (SELECT 1 FROM overseer_transports t WHERE t.observer_thread_id = b.observer_thread_id
              AND t.batch_number = b.batch_number AND t.phase = 'submitted')`;
              for (const batch of batches)
                yield* store.releaseUnsentBatch(observer, batch.batch_number);
              yield* sql`UPDATE overseer_mailboxes SET epoch = epoch + 1 WHERE observer_thread_id = ${observer}`;
              if (
                !settings.idleEnabled ||
                !previous ||
                previous.idle_minutes !== settings.idleMinutes
              )
                yield* sql`UPDATE overseer_preferences SET idle_due_at = NULL,
              idle_pending_token = CASE WHEN EXISTS (SELECT 1 FROM overseer_transports WHERE observer_thread_id = ${observer}
                AND phase = 'submitted') THEN idle_pending_token ELSE NULL END WHERE observer_thread_id = ${observer}`;
              yield* Policy.syncIdleState(observer);
              yield* store.touch(observer);
            }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          ),
        ),
      );
      yield* outbox.notifyAvailable();
      return settings;
    }),
  });
});
export const layer = Layer.effect(OverseerPreferences, make).pipe(
  Layer.provide(Layer.mergeAll(Store.layer, Gate.layer, Outbox.layer)),
);

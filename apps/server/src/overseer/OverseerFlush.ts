import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as OverseerFeature from "./OverseerFeature.ts";
import * as Policy from "./OverseerWakePolicy.ts";

const encode = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({ type: Schema.Literal("overseer.mailbox.flush"), generation: Schema.String }),
  ),
);

// Called only inside the mailbox mutation's SQL transaction. Pending folds arrivals;
// running requires a successor, independently of lease settlement or outbox retention.
export const schedule = Effect.fn("OverseerFlush.schedule")(function* (observer: string) {
  if (!(yield* OverseerFeature.Enabled)) return;
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly flush_id: string | null; readonly status: string | null }>`
    SELECT b.flush_id, e.status FROM overseer_mailboxes b
    LEFT JOIN orchestration_v2_effect_outbox e ON e.effect_id = b.flush_id
    WHERE b.observer_thread_id = ${observer}`;
  if (!rows[0]) return;
  const policy = yield* Policy.admission(observer);
  if (!policy.shouldSchedule) return;
  const now = DateTime.formatIso(yield* DateTime.now);
  if (rows[0].status === "pending") {
    yield* sql`UPDATE orchestration_v2_effect_outbox SET available_at = CASE
      WHEN attempt_count > 0 AND available_at > ${policy.notBefore} THEN available_at ELSE ${policy.notBefore} END, updated_at = ${now}
      WHERE effect_id = ${rows[0].flush_id} AND status = 'pending'`;
    return;
  }
  const generation = (yield* sql<{
    readonly id: string;
  }>`SELECT lower(hex(randomblob(16))) AS id`)[0]!.id;
  const id = `effect:overseer-flush:${generation}`;
  yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES (${id}, ${`command:overseer-flush:${generation}`}, ${observer}, 'overseer.mailbox.flush',
      ${encode({ type: "overseer.mailbox.flush", generation })}, 'pending', 0, ${policy.notBefore}, ${now}, ${now})`;
  yield* sql`UPDATE overseer_mailboxes SET flush_id = ${id} WHERE observer_thread_id = ${observer}`;
});

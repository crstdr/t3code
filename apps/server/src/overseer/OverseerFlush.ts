import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";

const encode = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({ type: Schema.Literal("overseer.mailbox.flush"), generation: Schema.String }),
  ),
);
import * as OverseerFeature from "./OverseerFeature.ts";

// Called only inside the mailbox mutation's SQL transaction. Pending folds arrivals;
// running requires a successor, independently of lease settlement or outbox retention.
export const schedule = Effect.fn("OverseerFlush.schedule")(function* (observer: string) {
  if (!(yield* OverseerFeature.Enabled)) return;
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly flush_id: string | null; readonly status: string | null }>`
    SELECT b.flush_id, e.status FROM overseer_mailboxes b
    LEFT JOIN orchestration_v2_effect_outbox e ON e.effect_id = b.flush_id
    WHERE b.observer_thread_id = ${observer}`;
  if (!rows[0] || rows[0].status === "pending") return;
  const generation = (yield* sql<{
    readonly id: string;
  }>`SELECT lower(hex(randomblob(16))) AS id`)[0]!.id;
  const id = `effect:overseer-flush:${generation}`;
  const now = DateTime.formatIso(yield* DateTime.now);
  yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES (${id}, ${`command:overseer-flush:${generation}`}, ${observer}, 'overseer.mailbox.flush',
      ${encode({ type: "overseer.mailbox.flush", generation })}, 'pending', 0, ${now}, ${now}, ${now})`;
  yield* sql`UPDATE overseer_mailboxes SET flush_id = ${id} WHERE observer_thread_id = ${observer}`;
});

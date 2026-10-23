import { OversightPreferences as Preferences, type ProviderRequestKind } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export const approvalKinds = [
  "command",
  "file-read",
  "file-change",
  "mcp-elicitation",
  "permission",
] as const satisfies ReadonlyArray<ProviderRequestKind>;
export const isApprovalKind = Schema.is(Schema.Literals(approvalKinds));
export type OccurrenceKind = "completion" | "question" | "approval";
export { Preferences };
export const defaults: Preferences = {
  autoWake: true,
  cadenceEnabled: false,
  cadenceMinutes: 5,
  idleEnabled: false,
  idleMinutes: 30,
};
export type PreferencesRow = {
  readonly observer_thread_id: string;
  readonly auto_wake: number;
  readonly cadence_enabled: number;
  readonly cadence_minutes: Preferences["cadenceMinutes"];
  readonly idle_enabled: number;
  readonly idle_minutes: Preferences["idleMinutes"];
  readonly last_routine_accepted_at: string | null;
  readonly idle_due_at: string | null;
  readonly idle_pending_token: string | null;
};
export const preferences = Effect.fn("OverseerWakePolicy.preferences")(function* (
  observer: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<PreferencesRow>`SELECT * FROM overseer_preferences WHERE observer_thread_id = ${observer}`)[0];
});
export function decodePreferences(row: PreferencesRow | undefined): Preferences {
  return row
    ? {
        autoWake: row.auto_wake === 1,
        cadenceEnabled: row.cadence_enabled === 1,
        cadenceMinutes: row.cadence_minutes,
        idleEnabled: row.idle_enabled === 1,
        idleMinutes: row.idle_minutes,
      }
    : defaults;
}
export const admission = Effect.fn("OverseerWakePolicy.admission")(function* (observer: string) {
  const sql = yield* SqlClient.SqlClient;
  const row = yield* preferences(observer);
  const settings = decodePreferences(row);
  const now = yield* DateTime.now;
  const attention =
    (yield* sql`SELECT 1 FROM overseer_occurrences o INDEXED BY overseer_occurrences_attention_idx
      JOIN overseer_links l ON l.link_id = o.link_id
      JOIN orchestration_v2_projection_threads w ON w.thread_id = l.target_thread_id
      JOIN orchestration_v2_projection_runtime_requests r ON r.runtime_request_id = o.source_id AND r.thread_id = l.target_thread_id
      WHERE o.observer_thread_id = ${observer} AND o.disposition = 'pending' AND o.ready = 1 AND o.kind IN ('question', 'approval')
        AND l.state = 'active' AND w.archived_at IS NULL AND w.deleted_at IS NULL AND r.status = 'pending'
        AND ((o.kind = 'question' AND r.kind = 'user_input' AND json_extract(r.payload_json, '$.responseCapability.type') IN ('live', 'message'))
          OR (o.kind = 'approval' AND r.kind IN ${sql.in(approvalKinds)} AND json_extract(r.payload_json, '$.responseCapability.type') = 'live')) LIMIT 1`)
      .length > 0;
  const cleanup =
    (yield* sql`SELECT 1 FROM overseer_transports WHERE observer_thread_id = ${observer}
    AND phase IN ('prepared', 'queued', 'frozen') LIMIT 1`).length > 0;
  const due =
    settings.cadenceEnabled && row?.last_routine_accepted_at
      ? DateTime.add(DateTime.makeUnsafe(row.last_routine_accepted_at), {
          minutes: settings.cadenceMinutes,
        })
      : now;
  return {
    allowRoutine:
      settings.autoWake &&
      (attention || DateTime.toEpochMillis(due) <= DateTime.toEpochMillis(now)),
    shouldSchedule: cleanup || settings.autoWake || attention || row?.idle_pending_token != null,
    notBefore: DateTime.formatIso(
      cleanup ||
        attention ||
        row?.idle_pending_token != null ||
        DateTime.toEpochMillis(due) <= DateTime.toEpochMillis(now)
        ? now
        : due,
    ),
  };
});

// Only one known observer is inspected, through core thread/status indexes. The
// admitted idle input itself is excluded, never unrelated work or held queues.
export const genuinelyIdle = Effect.fn("OverseerWakePolicy.genuinelyIdle")(function* (
  observer: string,
  ownMessage: string | null = null,
) {
  const sql = yield* SqlClient.SqlClient;
  return (
    (yield* sql`SELECT 1 FROM overseer_mailboxes b
    JOIN orchestration_v2_projection_threads t ON t.thread_id = b.observer_thread_id
    JOIN projection_projects p ON p.project_id = t.project_id
    WHERE b.observer_thread_id = ${observer} AND b.hold_mask = 0 AND t.archived_at IS NULL AND t.deleted_at IS NULL
      AND p.deleted_at IS NULL AND json_extract(t.payload_json, '$.snoozedUntil') IS NULL
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id = ${observer}
        AND r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
        AND (${ownMessage} IS NULL OR json_extract(r.payload_json, '$.userMessageId') IS NOT ${ownMessage}))
      AND NOT EXISTS (SELECT 1 FROM overseer_transports pending WHERE pending.observer_thread_id = ${observer}
        AND pending.phase IN ('prepared', 'queued', 'frozen', 'submitted') AND pending.message_id IS NOT ${ownMessage})
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runtime_requests q WHERE q.thread_id = ${observer} AND q.status = 'pending')
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_subagents s WHERE s.thread_id = ${observer}
        AND (s.status IN ('pending', 'running', 'waiting') OR (s.status <> 'idle' AND json_extract(s.payload_json, '$.completionDelivery.state') IN ('pending', 'claimed'))))
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = ${observer}
        AND i.type IN ('command_execution', 'dynamic_tool', 'subagent') AND i.status IN ('pending', 'running', 'waiting')
        AND (i.type <> 'dynamic_tool' OR COALESCE(json_extract(i.payload_json, '$.input.persistent'), 0) <> 1)
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs owner WHERE owner.run_id = i.run_id AND owner.status = 'rolled_back'))
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_provider_threads pt, json_each(pt.payload_json, '$.pendingBackgroundTasks') task
        WHERE pt.thread_id = ${observer} AND (t.active_provider_thread_id IS NULL OR pt.provider_thread_id = t.active_provider_thread_id)
          AND json_extract(task.value, '$.kind') IN ('subagent', 'monitor', 'background_task'))`)
      .length > 0
  );
});

export const syncIdleState = Effect.fn("OverseerWakePolicy.syncIdleState")(function* (
  observer: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const row = yield* preferences(observer);
  if (!row || row.idle_enabled !== 1) return;
  const own = row.idle_pending_token
    ? ((yield* sql<{ readonly message_id: string }>`SELECT t.message_id FROM overseer_transports t
    JOIN overseer_batches b ON b.observer_thread_id = t.observer_thread_id AND b.batch_number = t.batch_number
    WHERE t.observer_thread_id = ${observer} AND b.purpose = 'idle' AND b.idle_token = ${row.idle_pending_token}
      AND t.phase IN ('prepared', 'queued', 'frozen', 'submitted')`)[0]?.message_id ?? null)
    : null;
  if (!(yield* genuinelyIdle(observer, own))) {
    // An uncertain submitted idle offer remains owned until receipt/process loss.
    const submitted = own
      ? yield* sql`SELECT 1 FROM overseer_transports WHERE message_id = ${own} AND phase = 'submitted'`
      : [];
    yield* sql`UPDATE overseer_preferences SET idle_due_at = NULL,
      idle_pending_token = CASE WHEN ${submitted.length} > 0 THEN idle_pending_token ELSE NULL END WHERE observer_thread_id = ${observer}
      AND (idle_due_at IS NOT NULL OR (${submitted.length} = 0 AND idle_pending_token IS NOT NULL))`;
    return;
  }
  if (row.idle_pending_token !== null || row.idle_due_at !== null) return;
  const now = yield* DateTime.now;
  yield* sql`UPDATE overseer_preferences SET idle_due_at = ${DateTime.formatIso(DateTime.add(now, { minutes: row.idle_minutes }))} WHERE observer_thread_id = ${observer}`;
});

export const releaseIdleBatch = Effect.fn("OverseerWakePolicy.releaseIdleBatch")(function* (
  observer: string,
  batch: number,
  suppress = false,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE overseer_preferences SET idle_due_at = NULL, idle_pending_token = NULL
    WHERE observer_thread_id = ${observer} AND idle_pending_token = (SELECT idle_token FROM overseer_batches
      WHERE observer_thread_id = ${observer} AND batch_number = ${batch} AND purpose = 'idle')
      AND (${suppress ? 1 : 0} = 1 OR NOT EXISTS (SELECT 1 FROM overseer_transports WHERE observer_thread_id = ${observer}
        AND batch_number = ${batch} AND phase = 'submitted'))`;
  yield* syncIdleState(observer);
});

export const idleBatchEligible = Effect.fn("OverseerWakePolicy.idleBatchEligible")(function* (
  observer: string,
  batch: number,
  restoring = false,
) {
  const sql = yield* SqlClient.SqlClient;
  const row = (yield* sql<{
    readonly message_id: string | null;
  }>`SELECT t.message_id FROM overseer_batches b
    JOIN overseer_preferences p ON p.observer_thread_id = b.observer_thread_id
    LEFT JOIN overseer_transports t ON t.observer_thread_id = b.observer_thread_id AND t.batch_number = b.batch_number
      AND t.phase IN ('prepared', 'queued', 'frozen', 'submitted')
    WHERE b.observer_thread_id = ${observer} AND b.batch_number = ${batch} AND b.purpose = 'idle'
      AND (b.state IN ('queued', 'frozen') OR (${restoring ? 1 : 0} = 1 AND b.state = 'disposed'))
      AND p.idle_enabled = 1 AND p.idle_pending_token = b.idle_token`)[0];
  return row !== undefined && (yield* genuinelyIdle(observer, row.message_id));
});

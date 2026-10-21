import {
  OrchestrationV2RunJson,
  type OrchestrationV2StoredEvent,
  type ApplicationProjectEvent,
  type RunId,
  type ThreadId,
  type OrchestrationV2ConversationMessage,
} from "@t3tools/contracts";
import {
  backgroundWorkHoldsCompletion,
  pendingBackgroundTurnItems,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { isNativeMaintenanceCommand } from "../orchestration-v2/CommandPolicy.ts";
import { runOutcome } from "../orchestration-v2/RestartBackgroundNote.ts";
import * as OverseerFeature from "./OverseerFeature.ts";
import * as OverseerStore from "./OverseerStore.ts";

export class WakeFactsError extends Schema.TaggedError<WakeFactsError>()("WakeFactsError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

type Result<A> = Effect.Effect<A, WakeFactsError>;
export type CommitContext = {
  readonly commandType: string;
  readonly threadId: ThreadId;
  readonly cancelledEffects: boolean;
};
type Link = { readonly link_id: string; readonly observer_thread_id: string };
type Work = { readonly root_run_id: string | null };
type Outcome = "completed" | "failed" | "cancelled" | "interrupted";

const Run = OrchestrationV2RunJson.mapFields((fields) => ({
  id: fields.id,
  ordinal: fields.ordinal,
  status: fields.status,
  userMessageId: fields.userMessageId,
  providerThreadId: fields.providerThreadId,
  restartContinuationOfRunId: fields.restartContinuationOfRunId,
  restartCancelledBackgroundWork: fields.restartCancelledBackgroundWork,
}));
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(Run));

const explicitMessage = (message: OrchestrationV2ConversationMessage) =>
  message.role === "user" &&
  message.notification === undefined &&
  message.delegatedCompletion === undefined &&
  (message.createdBy === "user" || message.creationSource === "mcp") &&
  !isNativeMaintenanceCommand(message);

const relevant = ({ event }: OrchestrationV2StoredEvent) => {
  switch (event.type) {
    case "run.created":
    case "run.updated":
    case "run.background-work-cancelled":
    case "provider-thread.updated":
    case "subagent.updated":
    case "runtime-request.updated":
    case "thread.archived":
    case "thread.unarchived":
    case "thread.deleted":
      return true;
    case "turn-item.updated":
      return event.payload.type === "dynamic_tool" || event.payload.type === "subagent";
    default:
      return false;
  }
};

export class OverseerWakeFacts extends Context.Service<
  OverseerWakeFacts,
  {
    // SQL only, after projections and outbox insertion, within the source commit.
    readonly threadEvents: (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      context?: CommitContext,
    ) => Result<void>;
    readonly projectEvent: (event: ApplicationProjectEvent) => Result<void>;
    readonly notifyThreadEvents: (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      context?: CommitContext,
    ) => Effect.Effect<void>;
    readonly notifyProjectEvent: (event: ApplicationProjectEvent) => Effect.Effect<void>;
    readonly awaitAvailable: Effect.Effect<void>;
    // No new occurrence: reconcile a durable candidate after an eventless continuation disposition.
    readonly continuationSettled: (sourceRunId: RunId) => Result<void>;
    readonly completionOutcome: (linkId: string, rootRunId: string) => Result<Outcome | undefined>;
  }
>()("t3/overseer/OverseerWakeFacts") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* OverseerStore.OverseerStore;
  const available = yield* Queue.sliding<void>(1);
  const notify = OverseerFeature.Enabled.pipe(
    Effect.flatMap((enabled) =>
      enabled ? Queue.offer(available, undefined).pipe(Effect.asVoid) : Effect.void,
    ),
  );
  const wrap = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError((cause) => new WakeFactsError({ operation, cause })));
  const workFor = Effect.fn("OverseerWakeFacts.workFor")(function* (runId: string) {
    return (yield* sql<Work>`SELECT root_run_id FROM overseer_work_runs WHERE run_id = ${runId}`)[0];
  });
  const runsFor = Effect.fn("OverseerWakeFacts.runsFor")(function* (
    linkId: string,
    rootRunId: string,
  ) {
    const rows = yield* sql<{
      readonly payload_json: string;
      readonly restart_interrupted: number;
    }>`
      SELECT r.payload_json, w.restart_interrupted FROM overseer_work_runs w
      JOIN orchestration_v2_projection_runs r ON r.run_id = w.run_id
      WHERE w.link_id = ${linkId} AND w.root_run_id = ${rootRunId}`;
    return yield* Effect.forEach(rows, (row) =>
      decodeRun(row.payload_json).pipe(
        Effect.map((run) => ({ ...run, restartInterrupted: row.restart_interrupted === 1 })),
      ),
    );
  });
  const completionOutcome = Effect.fn("OverseerWakeFacts.completionOutcome")(function* (
    linkId: string,
    rootRunId: string,
  ) {
    const runs = yield* runsFor(linkId, rootRunId);
    const latest = runs.toSorted((a, b) => b.ordinal - a.ordinal)[0];
    if (!latest) return;
    const outcome =
      latest.restartInterrupted && latest.status === "cancelled"
        ? "interrupted"
        : runOutcome(latest);
    return outcome === "completed" ||
      outcome === "failed" ||
      outcome === "cancelled" ||
      outcome === "interrupted"
      ? outcome
      : undefined;
  });
  const reconcile = Effect.fn("OverseerWakeFacts.reconcile")(function* (
    linkId: string,
    rootRunId: string,
  ) {
    const candidate = yield* sql`SELECT 1 FROM overseer_occurrences
      WHERE link_id = ${linkId} AND kind = 'completion' AND source_id = ${rootRunId}
        AND disposition = 'pending'`;
    if (candidate.length === 0) return;
    const defer = store.deferCompletion(linkId, rootRunId).pipe(Effect.asVoid);
    const runs = yield* runsFor(linkId, rootRunId);
    if (runs.length === 0) return;
    const latest = runs.toSorted((a, b) => b.ordinal - a.ordinal)[0]!;
    if (latest.status === "rolled_back") {
      yield* store.withdraw({ linkId, kind: "completion", sourceId: rootRunId });
      return;
    }
    if (
      runs.some(
        (run) =>
          !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(run.status),
      )
    )
      return yield* defer;
    for (const run of runs) {
      const continuation = yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox
        WHERE effect_id = ${`effect:restart-continuation:${run.id}`} AND status IN ('pending', 'running')`;
      if (continuation.length > 0) return yield* defer;
      const children = yield* sql`SELECT 1 FROM orchestration_v2_projection_subagents
        WHERE run_id = ${run.id} AND (status IN ('pending', 'running', 'waiting')
          OR (status <> 'idle' AND json_extract(payload_json, '$.completionDelivery.state') IN ('pending', 'claimed'))) LIMIT 1`;
      if (children.length > 0) return yield* defer;
      // Only execution metadata is read, never tool input/output or transcript content.
      const items = yield* sql<{
        readonly id: string;
        readonly type: "dynamic_tool" | "subagent";
        readonly status: "pending" | "running" | "waiting";
        readonly persistent: number | null;
      }>`SELECT turn_item_id AS id, type, status, json_extract(payload_json, '$.input.persistent') AS persistent
        FROM orchestration_v2_projection_turn_items WHERE run_id = ${run.id}
          AND type IN ('dynamic_tool', 'subagent') AND status IN ('pending', 'running', 'waiting')`;
      if (
        pendingBackgroundTurnItems({
          runs,
          turnItems: items.map((item) => ({
            ...item,
            runId: run.id,
            title: null,
            input: { persistent: item.persistent === 1 },
          })),
        }).length > 0
      )
        return yield* defer;
    }
    // A session roster is not run-owned. Preserve each task's original causal owner even as later explicit turns reuse the session.
    const tasks = yield* sql<{
      readonly kind: "command" | "subagent" | "monitor" | "background_task";
    }>`
      SELECT json_extract(task.value, '$.kind') AS kind FROM overseer_background_owners owner
      JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = owner.provider_thread_id
      JOIN json_each(p.payload_json, '$.pendingBackgroundTasks') task ON json_extract(task.value, '$.taskId') = owner.task_id
      WHERE owner.link_id = ${linkId} AND owner.root_run_id = ${rootRunId}`;
    if (backgroundWorkHoldsCompletion(tasks)) return yield* defer;
    yield* store.settleCompletion(linkId, rootRunId);
  });
  const reconcileLink = Effect.fn("OverseerWakeFacts.reconcileLink")(function* (linkId: string) {
    let after = "";
    while (true) {
      const rows = yield* sql<{
        readonly source_id: string;
      }>`SELECT source_id FROM overseer_occurrences
        WHERE link_id = ${linkId} AND kind = 'completion' AND disposition = 'pending' AND ready = 0
          AND source_id > ${after} ORDER BY source_id LIMIT 100`;
      for (const row of rows) yield* reconcile(linkId, row.source_id);
      if (rows.length < 100) return;
      after = rows[rows.length - 1]!.source_id;
    }
  });
  const captureRun = Effect.fn("OverseerWakeFacts.captureRun")(function* (
    link: Link,
    stored: OrchestrationV2StoredEvent,
    context?: CommitContext,
  ) {
    const event = stored.event;
    if (event.type !== "run.created" && event.type !== "run.updated") return;
    const run = event.payload;
    if (!(yield* workFor(run.id))) {
      let root: string | null = null;
      if (run.restartContinuationOfRunId !== undefined) {
        root = (yield* workFor(run.restartContinuationOfRunId))?.root_run_id ?? null;
      } else {
        const messages = yield* sql<{
          readonly text: string;
          readonly attachment_count: number;
          readonly created_by: string;
          readonly creation_source: string;
          readonly automatic: number;
        }>`
          SELECT json_extract(payload_json, '$.text') AS text,
            json_array_length(payload_json, '$.attachments') AS attachment_count,
            json_extract(payload_json, '$.createdBy') AS created_by,
            json_extract(payload_json, '$.creationSource') AS creation_source,
            (json_type(payload_json, '$.notification') IS NOT NULL OR json_type(payload_json, '$.delegatedCompletion') IS NOT NULL) AS automatic
          FROM orchestration_v2_projection_messages WHERE message_id = ${run.userMessageId} AND thread_id = ${event.threadId}`;
        const message = messages[0];
        if (!message) return;
        const explicit =
          message.automatic === 0 &&
          (message.created_by === "user" || message.creation_source === "mcp");
        if (
          explicit &&
          !(
            message.attachment_count === 0 &&
            isNativeMaintenanceCommand({ text: message.text, attachments: [] })
          )
        )
          root = run.id;
      }
      yield* sql`INSERT INTO overseer_work_runs(run_id, link_id, root_run_id)
        VALUES (${run.id}, ${link.link_id}, ${root})`;
      if (root === run.id)
        yield* store.record({
          linkId: link.link_id,
          kind: "completion",
          sourceId: root,
          sourceSequence: stored.sequence,
          ready: false,
        });
    }
    if (context?.commandType === "provider-runtime.reconcile" && run.status === "cancelled") {
      yield* sql`UPDATE overseer_work_runs SET restart_interrupted = 1 WHERE run_id = ${run.id}`;
    }
  });
  const threadEvents = Effect.fn("OverseerWakeFacts.threadEvents")(function* (
    events: ReadonlyArray<OrchestrationV2StoredEvent>,
    context?: CommitContext,
  ) {
    if (!(yield* OverseerFeature.Enabled)) return;
    const relevantEvents = events.filter(relevant);
    const affected = new Map<string, Link>();
    const affectedThreads = new Set(relevantEvents.map((stored) => stored.event.threadId));
    if (context?.cancelledEffects) affectedThreads.add(context.threadId);
    for (const threadId of affectedThreads) {
      const link = (yield* sql<Link>`SELECT link_id, observer_thread_id FROM overseer_links
        WHERE target_thread_id = ${threadId} AND state = 'active'`)[0];
      if (link) affected.set(threadId, link);
    }
    // Capture all run identities before request/roster provenance, regardless of event ordering within the atomic batch.
    for (const stored of relevantEvents) {
      const link = affected.get(stored.event.threadId);
      if (link) yield* captureRun(link, stored, context);
    }
    for (const { event, sequence } of relevantEvents) {
      if (event.type === "thread.archived") yield* store.endpointArchived(event.threadId);
      if (event.type === "thread.unarchived") yield* store.endpointUnarchived(event.threadId);
      if (event.type === "thread.deleted") yield* store.endpointDeleted(event.threadId);
      const link = affected.get(event.threadId);
      if (!link) continue;
      if (event.type === "runtime-request.updated") {
        const request = event.payload;
        const key = { linkId: link.link_id, kind: "question" as const, sourceId: request.id };
        if (
          request.kind !== "user_input" ||
          request.status !== "pending" ||
          (request.responseCapability.type !== "live" &&
            request.responseCapability.type !== "message")
        ) {
          yield* store.withdraw(key);
          continue;
        }
        const owners = yield* sql<Work>`SELECT root_run_id FROM overseer_work_runs
          WHERE link_id = ${link.link_id} AND run_id = COALESCE(
            (SELECT run_id FROM orchestration_v2_projection_nodes WHERE node_id = ${request.nodeId} AND thread_id = ${event.threadId}),
            (SELECT a.run_id FROM orchestration_v2_projection_provider_turns p
              JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id = p.run_attempt_id AND a.thread_id = p.thread_id
              WHERE p.provider_turn_id = ${request.providerTurnId} AND p.thread_id = ${event.threadId}),
            ${event.runId ?? null})`;
        if (owners[0]?.root_run_id != null)
          yield* store.record({ ...key, sourceSequence: sequence, ready: true });
      }
      if (event.type === "provider-thread.updated") {
        const provider = event.payload;
        yield* sql`DELETE FROM overseer_background_owners
          WHERE link_id = ${link.link_id} AND provider_thread_id = ${provider.id}
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_provider_threads p,
              json_each(p.payload_json, '$.pendingBackgroundTasks') task
              WHERE p.provider_thread_id = ${provider.id}
                AND json_extract(task.value, '$.taskId') = overseer_background_owners.task_id)`;
        // lastRunOrdinal is the provider's explicit ownership marker, not the thread's latest run.
        const owner = (yield* sql<Work>`SELECT w.root_run_id FROM orchestration_v2_projection_runs r
          JOIN overseer_work_runs w ON w.run_id = r.run_id AND w.link_id = ${link.link_id}
          WHERE r.thread_id = ${event.threadId} AND r.ordinal = ${provider.lastRunOrdinal}
            AND (r.provider_thread_id IS NULL OR r.provider_thread_id = ${provider.id})`)[0];
        if (owner !== undefined) {
          for (const task of provider.pendingBackgroundTasks ?? []) {
            if (!backgroundWorkHoldsCompletion([task])) continue;
            yield* sql`INSERT INTO overseer_background_owners(link_id, provider_thread_id, task_id, root_run_id)
              VALUES (${link.link_id}, ${provider.id}, ${task.taskId}, ${owner.root_run_id}) ON CONFLICT DO NOTHING`;
          }
        }
      }
    }
    const threadId = context?.threadId;
    if (threadId !== undefined && context?.commandType === "thread.stop")
      yield* store.stop(threadId);
    // These are explicit queue-control commands; automatic queue progression does not dispatch them.
    if (
      threadId !== undefined &&
      (context?.commandType === "queue.resume" || context?.commandType === "queued-run.start")
    )
      yield* store.resumeExplicitly(threadId);
    if (context?.commandType === "message.dispatch") {
      for (const { event } of events) {
        if (
          event.type === "message.updated" &&
          event.threadId === context.threadId &&
          explicitMessage(event.payload)
        )
          yield* store.resumeExplicitly(event.threadId);
      }
    }
    // Recheck changed roots even after readiness: late provider evidence or rollback can invalidate a queued/frozen completion.
    const changedRoots = new Map<string, Set<string>>();
    for (const { event } of relevantEvents) {
      const link = affected.get(event.threadId);
      if (!link) continue;
      const roots = changedRoots.get(link.link_id) ?? new Set<string>();
      changedRoots.set(link.link_id, roots);
      const runId =
        event.type === "run.created" || event.type === "run.updated"
          ? event.payload.id
          : event.type === "run.background-work-cancelled" ||
              event.type === "turn-item.updated" ||
              event.type === "subagent.updated"
            ? event.payload.runId
            : undefined;
      if (runId != null) {
        const work = yield* workFor(runId);
        if (work?.root_run_id != null) roots.add(work.root_run_id);
      }
      if (event.type === "provider-thread.updated") {
        const owners = yield* sql<Work>`SELECT DISTINCT root_run_id FROM overseer_background_owners
          WHERE link_id = ${link.link_id} AND provider_thread_id = ${event.payload.id} AND root_run_id IS NOT NULL`;
        for (const owner of owners) if (owner.root_run_id !== null) roots.add(owner.root_run_id);
      }
    }
    for (const [linkId, roots] of changedRoots)
      for (const root of roots) yield* reconcile(linkId, root);
    for (const link of affected.values()) yield* reconcileLink(link.link_id);
  });
  return OverseerWakeFacts.of({
    threadEvents: (events, context) => wrap("thread-events", threadEvents(events, context)),
    projectEvent: (event) =>
      wrap(
        "project-event",
        OverseerFeature.Enabled.pipe(
          Effect.flatMap((enabled) =>
            enabled && event.type === "project.deleted"
              ? store.projectDeleted(event.aggregateId)
              : Effect.void,
          ),
        ),
      ),
    notifyThreadEvents: (events, context) =>
      events.some(relevant) ||
      context?.commandType === "thread.stop" ||
      context?.commandType === "run.interrupt" ||
      context?.commandType === "queue.resume" ||
      context?.commandType === "queued-run.start" ||
      (context?.commandType === "message.dispatch" &&
        events.some(
          ({ event }) => event.type === "message.updated" && explicitMessage(event.payload),
        ))
        ? notify
        : Effect.void,
    notifyProjectEvent: (event) => (event.type === "project.deleted" ? notify : Effect.void),
    awaitAvailable: Queue.take(available),
    continuationSettled: Effect.fn("OverseerWakeFacts.continuationSettled")(
      function* (sourceRunId) {
        if (!(yield* OverseerFeature.Enabled)) return;
        yield* wrap(
          "continuation-settled",
          sql.withTransaction(
            Effect.gen(function* () {
              const rows = yield* sql<
                Work & { readonly link_id: string }
              >`SELECT w.link_id, w.root_run_id FROM overseer_work_runs w
          JOIN overseer_links l ON l.link_id = w.link_id AND l.state = 'active' WHERE w.run_id = ${sourceRunId}`;
              const row = rows[0];
              if (row?.root_run_id != null) yield* reconcile(row.link_id, row.root_run_id);
            }),
          ),
        );
        yield* notify;
      },
    ),
    completionOutcome: (linkId, rootRunId) =>
      wrap("completion-outcome", completionOutcome(linkId, rootRunId)),
  });
});

export const layer = Layer.effect(OverseerWakeFacts, make).pipe(Layer.provide(OverseerStore.layer));

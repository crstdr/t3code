import type {
  CommandId,
  MessageId,
  ProjectId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class OverseerStoreError extends Schema.TaggedError<OverseerStoreError>()(
  "OverseerStoreError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class OverseerConflict extends Schema.TaggedError<OverseerConflict>()("OverseerConflict", {
  reason: Schema.Literals([
    "retry_input",
    "endpoint_unavailable",
    "stale_mailbox",
    "attempt_identity",
  ]),
}) {}

const isOverseerConflict = Schema.is(OverseerConflict);

type StoreResult<A> = Effect.Effect<A, OverseerStoreError | OverseerConflict>;
type HoldReason = "stop" | "queue" | "snoozed" | "provider" | "authorization";
const holdBits = { stop: 1, queue: 2, snoozed: 4, provider: 8, authorization: 16 } as const;
const batchSize = 8;
type OccurrenceKey = {
  readonly linkId: string;
  readonly kind: "completion" | "question";
  readonly sourceId: string;
};
type RecordInput = Omit<OccurrenceKey, "kind"> & { readonly sourceSequence: number } & (
    | { readonly kind: "completion"; readonly ready: boolean }
    | { readonly kind: "question"; readonly ready: true }
  );
type Fence = {
  readonly observerThreadId: ThreadId;
  readonly epoch: number;
  readonly revision: number;
};
type Reservation = {
  readonly linkId: string;
  readonly observerThreadId: ThreadId;
  readonly observerProjectId: ProjectId;
  readonly targetThreadId: ThreadId;
  readonly targetProjectId: ProjectId;
  readonly clientRequestId: string;
  readonly fingerprint: string;
  readonly launchCommandId: CommandId;
  readonly launchMessageId: MessageId;
  readonly launchInputJson: string;
};
type LinkRow = {
  readonly link_id: string;
  readonly observer_thread_id: string;
  readonly observer_project_id: string;
  readonly target_thread_id: string;
  readonly target_project_id: string;
  readonly client_request_id: string;
  readonly fingerprint: string;
  readonly launch_command_id: string;
  readonly launch_message_id: string;
  readonly launch_input_json: string | null;
  readonly launch_accepted: number;
  readonly state: "active" | "closed";
  readonly close_reason: "unlink" | "endpoint_deleted" | "project_deleted" | null;
};
type MailboxRow = {
  readonly observer_thread_id: string;
  readonly epoch: number;
  readonly revision: number;
  readonly processed_revision: number;
  readonly next_batch_number: number;
  readonly hold_reason: HoldReason | null;
  readonly hold_mask: number;
};
type OccurrenceRow = {
  readonly link_id: string;
  readonly kind: "completion" | "question";
  readonly source_id: string;
  readonly observer_thread_id: string;
  readonly source_sequence: number;
  readonly ready: number;
  readonly disposition: "pending" | "accepted" | "withdrawn" | "suppressed";
  readonly batch_number: number | null;
};
type BatchRow = {
  readonly observer_thread_id: string;
  readonly batch_number: number;
  readonly epoch: number;
  readonly revision: number;
  readonly state: "queued" | "frozen" | "accepted" | "suppressed" | "disposed";
};
type AttemptRow = {
  readonly observer_thread_id: string;
  readonly batch_number: number;
  readonly attempt_number: number;
  readonly command_id: string;
  readonly message_id: string;
  readonly run_id: string | null;
  readonly run_attempt_id: string | null;
  readonly state: "pending" | "accepted" | "disposed";
  readonly disposal_reason: "process_loss" | "cancel" | "stop" | "lifecycle" | null;
};

type StageResult =
  | { readonly state: "ready"; readonly batch: BatchRow }
  | { readonly state: "busy"; readonly attempt: AttemptRow }
  | { readonly state: "held"; readonly reason: HoldReason }
  | { readonly state: "reconciled" | "idle" | "blocked" };

export class OverseerStore extends Context.Service<
  OverseerStore,
  {
    readonly reserve: (input: Reservation) => StoreResult<LinkRow>;
    readonly acceptLaunch: (linkId: string) => StoreResult<boolean>;
    readonly linksForEndpoint: (
      threadId: ThreadId,
      after?: string,
      limit?: number,
    ) => StoreResult<ReadonlyArray<LinkRow>>;
    readonly incompleteLaunches: (
      after?: string,
      limit?: number,
    ) => StoreResult<ReadonlyArray<LinkRow>>;
    readonly mailbox: (observer: ThreadId) => StoreResult<MailboxRow | undefined>;
    readonly recoveryMailboxes: (
      after?: string,
      limit?: number,
    ) => StoreResult<ReadonlyArray<string>>;
    readonly record: (input: RecordInput) => StoreResult<boolean>;
    readonly settleCompletion: (linkId: string, sourceId: string) => StoreResult<boolean>;
    readonly withdraw: (key: OccurrenceKey) => StoreResult<boolean>;
    readonly pendingCandidates: (
      observer: ThreadId,
      after?: { readonly sequence: number; readonly sourceId: string; readonly linkId: string },
      limit?: number,
    ) => StoreResult<ReadonlyArray<OccurrenceRow>>;
    readonly batchEntries: (
      observer: ThreadId,
      batchNumber: number,
    ) => StoreResult<ReadonlyArray<OccurrenceRow>>;
    readonly stageBatch: (fence: Fence) => StoreResult<StageResult>;
    readonly freezeAttempt: (
      input: Fence & {
        readonly batchNumber: number;
        readonly commandId: CommandId;
        readonly messageId: MessageId;
      },
    ) => StoreResult<AttemptRow>;
    readonly bindAttempt: (
      messageId: MessageId,
      runId: RunId,
      runAttemptId: RunAttemptId,
    ) => StoreResult<boolean>;
    readonly attemptForMessage: (messageId: MessageId) => StoreResult<AttemptRow | undefined>;
    readonly attemptsForRun: (
      runId: RunId,
      afterMessageId?: string,
      limit?: number,
    ) => StoreResult<ReadonlyArray<AttemptRow>>;
    readonly acceptAttempt: (messageId: MessageId, epoch: number) => StoreResult<boolean>;
    readonly disposeAttempt: (
      messageId: MessageId,
      reason: "process_loss" | "cancel",
    ) => StoreResult<boolean>;
    readonly checkpoint: (fence: Fence) => StoreResult<boolean>;
    readonly stop: (observer: ThreadId) => StoreResult<void>;
    readonly hold: (observer: ThreadId, reason: Exclude<HoldReason, "stop">) => StoreResult<void>;
    readonly resume: (observer: ThreadId, reason: Exclude<HoldReason, "stop">) => StoreResult<void>;
    // Only an explicit work turn or explicit queue-resume action may release Stop.
    readonly resumeExplicitly: (observer: ThreadId) => StoreResult<void>;
    readonly pendingAttempt: (observer: ThreadId) => StoreResult<AttemptRow | undefined>;
    readonly unlink: (linkId: string) => StoreResult<boolean>;
    // Call after applying the matching core projection, inside its SQL transaction.
    readonly endpointArchived: (threadId: ThreadId) => StoreResult<void>;
    readonly endpointUnarchived: (threadId: ThreadId) => StoreResult<void>;
    readonly endpointDeleted: (threadId: ThreadId) => StoreResult<void>;
    readonly projectDeleted: (projectId: ProjectId) => StoreResult<void>;
    readonly pruneClosed: StoreResult<void>;
    // The caller must stop feature workers first. No core rows or messages are removed.
    readonly clearFeatureData: StoreResult<void>;
  }
>()("t3/overseer/OverseerStore") {}

const pageSize = (limit = 20) => Math.max(1, Math.min(100, Math.trunc(limit) || 20));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const execute = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isOverseerConflict(cause) ? cause : new OverseerStoreError({ operation, cause }),
      ),
    );
  const transaction = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
    execute(operation, sql.withTransaction(effect));

  const mailbox = Effect.fn("OverseerStore.mailbox")(function* (observer: string) {
    return (yield* sql<MailboxRow>`SELECT * FROM overseer_mailboxes WHERE observer_thread_id = ${observer}`)[0];
  });
  const dirty = Effect.fn("OverseerStore.dirty")(function* (observer: string) {
    yield* sql`UPDATE overseer_mailboxes SET revision = revision + 1 WHERE observer_thread_id = ${observer}`;
  });
  const requireFence = Effect.fn("OverseerStore.requireFence")(function* (fence: Fence) {
    const row = yield* mailbox(fence.observerThreadId);
    if (!row || row.epoch !== fence.epoch || row.revision !== fence.revision) {
      return yield* new OverseerConflict({ reason: "stale_mailbox" });
    }
    return row;
  });
  const disposeBatch = Effect.fn("OverseerStore.disposeBatch")(function* (
    observer: string,
    batch: number,
    reason: "cancel" | "stop" | "lifecycle",
    suppress: boolean,
  ) {
    yield* sql`UPDATE overseer_attempts SET state = 'disposed', disposal_reason = ${reason}
      WHERE observer_thread_id = ${observer} AND batch_number = ${batch} AND state = 'pending'`;
    yield* sql`UPDATE overseer_occurrences SET disposition = ${suppress ? "suppressed" : "pending"}, batch_number = NULL
      WHERE observer_thread_id = ${observer} AND batch_number = ${batch} AND disposition = 'pending'`;
    yield* sql`UPDATE overseer_batches SET state = ${suppress ? "suppressed" : "disposed"}
      WHERE observer_thread_id = ${observer} AND batch_number = ${batch} AND state IN ('queued', 'frozen')`;
  });
  const invalidateLink = Effect.fn("OverseerStore.invalidateLink")(function* (linkId: string) {
    const batches = yield* sql<BatchRow>`SELECT DISTINCT b.* FROM overseer_occurrences o
      JOIN overseer_batches b ON b.observer_thread_id = o.observer_thread_id AND b.batch_number = o.batch_number
      WHERE o.link_id = ${linkId} AND o.disposition = 'pending' AND b.state IN ('queued', 'frozen')`;
    for (const batch of batches)
      yield* disposeBatch(batch.observer_thread_id, batch.batch_number, "lifecycle", false);
  });
  const closeLink = Effect.fn("OverseerStore.closeLink")(function* (
    linkId: string,
    reason: "unlink" | "endpoint_deleted" | "project_deleted",
  ) {
    const rows =
      yield* sql<LinkRow>`UPDATE overseer_links SET state = 'closed', close_reason = ${reason}, launch_input_json = NULL
      WHERE link_id = ${linkId} AND state = 'active' RETURNING *`;
    const row = rows[0];
    if (!row) return false;
    yield* invalidateLink(linkId);
    yield* sql`UPDATE overseer_occurrences SET disposition = 'withdrawn', batch_number = NULL
      WHERE link_id = ${linkId} AND disposition = 'pending'`;
    yield* dirty(row.observer_thread_id);
    return true;
  });
  const invalidateOccurrence = Effect.fn("OverseerStore.invalidateOccurrence")(function* (
    row: OccurrenceRow,
  ) {
    if (row.batch_number === null) return;
    const batch =
      yield* sql`SELECT 1 FROM overseer_batches WHERE observer_thread_id = ${row.observer_thread_id}
      AND batch_number = ${row.batch_number} AND state IN ('queued', 'frozen')`;
    if (batch.length > 0)
      yield* disposeBatch(row.observer_thread_id, row.batch_number, "lifecycle", false);
  });
  // Revalidate only selected membership, never scan the observer's entire question backlog.
  const reconcileQuestions = Effect.fn("OverseerStore.reconcileQuestions")(function* (
    observer: string,
    entries: ReadonlyArray<OccurrenceRow>,
  ) {
    let withdrawn = 0;
    for (const row of entries) {
      if (row.kind !== "question") continue;
      const valid = yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests r
        JOIN overseer_links l ON l.link_id = ${row.link_id}
        WHERE r.runtime_request_id = ${row.source_id} AND r.thread_id = l.target_thread_id
          AND r.kind = 'user_input' AND r.status = 'pending'
          AND json_extract(r.payload_json, '$.responseCapability.type') IN ('live', 'message')`;
      if (valid.length > 0) continue;
      yield* invalidateOccurrence(row);
      yield* sql`UPDATE overseer_occurrences SET disposition = 'withdrawn', batch_number = NULL
        WHERE link_id = ${row.link_id} AND kind = 'question' AND source_id = ${row.source_id}`;
      withdrawn++;
    }
    if (withdrawn > 0) yield* dirty(observer);
    return withdrawn;
  });
  const eligible = Effect.fn("OverseerStore.eligible")(function* (observer: string) {
    return yield* sql<OccurrenceRow>`SELECT o.* FROM overseer_occurrences o
    JOIN overseer_links l ON l.link_id = o.link_id
    JOIN orchestration_v2_projection_threads worker ON worker.thread_id = l.target_thread_id
    JOIN orchestration_v2_projection_threads hub ON hub.thread_id = l.observer_thread_id
    WHERE o.observer_thread_id = ${observer} AND o.disposition = 'pending' AND o.ready = 1
      AND l.state = 'active' AND worker.archived_at IS NULL AND worker.deleted_at IS NULL
      AND hub.archived_at IS NULL AND hub.deleted_at IS NULL
    ORDER BY o.source_sequence, o.kind, o.source_id, o.link_id LIMIT ${batchSize}`;
  });
  const hasUnfinished = Effect.fn("OverseerStore.hasUnfinished")(function* (observer: string) {
    const rows = yield* sql`SELECT 1 WHERE
      EXISTS (SELECT 1 FROM overseer_occurrences WHERE observer_thread_id = ${observer} AND disposition = 'pending' AND ready = 1)
      OR EXISTS (SELECT 1 FROM overseer_occurrences WHERE observer_thread_id = ${observer} AND disposition = 'pending' AND ready = 0)`;
    return rows.length > 0;
  });
  const batchEligible = Effect.fn("OverseerStore.batchEligible")(function* (
    observer: string,
    batch: number,
  ) {
    const rows = yield* sql<{
      readonly total: number;
      readonly valid: number;
    }>`SELECT COUNT(*) AS total,
      COALESCE(SUM(CASE WHEN l.state = 'active' AND o.ready = 1
        AND hub.thread_id IS NOT NULL AND hub.archived_at IS NULL AND hub.deleted_at IS NULL
        AND worker.thread_id IS NOT NULL AND worker.archived_at IS NULL AND worker.deleted_at IS NULL
        AND (o.kind = 'completion' OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runtime_requests r
          WHERE r.runtime_request_id = o.source_id AND r.thread_id = l.target_thread_id
            AND r.kind = 'user_input' AND r.status = 'pending'
            AND json_extract(r.payload_json, '$.responseCapability.type') IN ('live', 'message')))
        THEN 1 ELSE 0 END), 0) AS valid
      FROM overseer_occurrences o JOIN overseer_links l ON l.link_id = o.link_id
      LEFT JOIN orchestration_v2_projection_threads hub ON hub.thread_id = l.observer_thread_id
      LEFT JOIN orchestration_v2_projection_threads worker ON worker.thread_id = l.target_thread_id
      WHERE o.observer_thread_id = ${observer} AND o.batch_number = ${batch} AND o.disposition = 'pending'`;
    return (
      rows[0] !== undefined &&
      rows[0].total > 0 &&
      rows[0].total <= batchSize &&
      rows[0].total === rows[0].valid
    );
  });
  const retireMailbox = Effect.fn("OverseerStore.retireMailbox")(function* (observer: string) {
    yield* sql`UPDATE overseer_mailboxes SET epoch = epoch + 1, hold_mask = 0, processed_revision = revision
      WHERE observer_thread_id = ${observer}`;
  });
  const attemptForMessage = Effect.fn("OverseerStore.attemptForMessage")(function* (
    messageId: MessageId,
  ) {
    return (yield* sql<AttemptRow>`SELECT * FROM overseer_attempts WHERE message_id = ${messageId}`)[0];
  });
  const pendingAttempt = Effect.fn("OverseerStore.pendingAttempt")(function* (observer: string) {
    return (yield* sql<AttemptRow>`SELECT * FROM overseer_attempts WHERE observer_thread_id = ${observer} AND state = 'pending'`)[0];
  });
  const endpointLinks = Effect.fn("OverseerStore.endpointLinks")(function* (threadId: ThreadId) {
    return yield* sql<{ readonly link_id: string; readonly observer_thread_id: string }>`
      SELECT link_id, observer_thread_id FROM overseer_links
      WHERE state = 'active' AND (observer_thread_id = ${threadId} OR target_thread_id = ${threadId})`;
  });

  return OverseerStore.of({
    reserve: Effect.fn("OverseerStore.reserve")(function* (input) {
      return yield* transaction(
        "reserve",
        Effect.gen(function* () {
          const existing = (yield* sql<LinkRow>`SELECT * FROM overseer_links
          WHERE observer_thread_id = ${input.observerThreadId} AND client_request_id = ${input.clientRequestId}`)[0];
          if (existing) {
            if (existing.fingerprint !== input.fingerprint)
              return yield* new OverseerConflict({ reason: "retry_input" });
            return existing;
          }
          const endpoints = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads t
          JOIN projection_projects p ON p.project_id = t.project_id
          JOIN projection_projects destination ON destination.project_id = ${input.targetProjectId}
          WHERE t.thread_id = ${input.observerThreadId} AND t.project_id = ${input.observerProjectId}
            AND t.deleted_at IS NULL AND p.deleted_at IS NULL AND destination.deleted_at IS NULL
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${input.targetThreadId})`;
          if (endpoints.length === 0)
            return yield* new OverseerConflict({ reason: "endpoint_unavailable" });
          yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id) VALUES (${input.observerThreadId}) ON CONFLICT DO NOTHING`;
          yield* sql`INSERT INTO overseer_links (
          link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id,
          client_request_id, fingerprint, launch_command_id, launch_message_id, launch_input_json
        ) VALUES (${input.linkId}, ${input.observerThreadId}, ${input.observerProjectId}, ${input.targetThreadId},
          ${input.targetProjectId}, ${input.clientRequestId}, ${input.fingerprint}, ${input.launchCommandId},
          ${input.launchMessageId}, ${input.launchInputJson}) ON CONFLICT(observer_thread_id, client_request_id) DO NOTHING`;
          const row = (yield* sql<LinkRow>`SELECT * FROM overseer_links
          WHERE observer_thread_id = ${input.observerThreadId} AND client_request_id = ${input.clientRequestId}`)[0]!;
          if (row.fingerprint !== input.fingerprint)
            return yield* new OverseerConflict({ reason: "retry_input" });
          return row;
        }),
      );
    }),
    acceptLaunch: Effect.fn("OverseerStore.acceptLaunch")(function* (linkId) {
      return yield* transaction(
        "acceptLaunch",
        sql`UPDATE overseer_links SET launch_accepted = 1, launch_input_json = NULL
        WHERE link_id = ${linkId} AND state = 'active' RETURNING link_id`.pipe(
          Effect.map((rows) => rows.length > 0),
        ),
      );
    }),
    linksForEndpoint: Effect.fn("OverseerStore.linksForEndpoint")(function* (
      threadId,
      after = "",
      limit,
    ) {
      return yield* execute(
        "linksForEndpoint",
        sql<LinkRow>`SELECT * FROM overseer_links
        WHERE state = 'active' AND (observer_thread_id = ${threadId} OR target_thread_id = ${threadId})
          AND link_id > ${after} ORDER BY link_id LIMIT ${pageSize(limit)}`,
      );
    }),
    incompleteLaunches: Effect.fn("OverseerStore.incompleteLaunches")(function* (
      after = "",
      limit,
    ) {
      return yield* execute(
        "incompleteLaunches",
        sql<LinkRow>`SELECT * FROM overseer_links
        WHERE state = 'active' AND launch_accepted = 0 AND link_id > ${after} ORDER BY link_id LIMIT ${pageSize(limit)}`,
      );
    }),
    mailbox: (observer) => execute("mailbox", mailbox(observer)),
    recoveryMailboxes: Effect.fn("OverseerStore.recoveryMailboxes")(function* (after = "", limit) {
      const rows = yield* execute(
        "recoveryMailboxes",
        sql<{ readonly observer_thread_id: string }>`
        SELECT observer_thread_id FROM overseer_mailboxes WHERE revision > processed_revision AND observer_thread_id > ${after}
        UNION SELECT observer_thread_id FROM overseer_mailboxes WHERE hold_mask <> 0 AND observer_thread_id > ${after}
        UNION SELECT observer_thread_id FROM overseer_attempts WHERE state = 'pending' AND observer_thread_id > ${after}
        ORDER BY observer_thread_id LIMIT ${pageSize(limit)}`,
      );
      return rows.map((row) => row.observer_thread_id);
    }),
    record: Effect.fn("OverseerStore.record")(function* (input) {
      return yield* transaction(
        "record",
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly observer_thread_id: string;
          }>`INSERT INTO overseer_occurrences
          (link_id, kind, source_id, observer_thread_id, source_sequence, ready)
          SELECT link_id, ${input.kind}, ${input.sourceId}, observer_thread_id, ${input.sourceSequence}, ${input.ready ? 1 : 0}
          FROM overseer_links WHERE link_id = ${input.linkId} AND state = 'active'
          ON CONFLICT(link_id, kind, source_id) DO NOTHING RETURNING observer_thread_id`;
          if (!rows[0]) return false;
          yield* dirty(rows[0].observer_thread_id);
          return true;
        }),
      );
    }),
    settleCompletion: Effect.fn("OverseerStore.settleCompletion")(function* (linkId, sourceId) {
      return yield* transaction(
        "settleCompletion",
        Effect.gen(function* () {
          const rows = yield* sql<OccurrenceRow>`UPDATE overseer_occurrences SET ready = 1
          WHERE link_id = ${linkId} AND kind = 'completion' AND source_id = ${sourceId}
            AND ready = 0 AND disposition = 'pending' RETURNING *`;
          if (!rows[0]) return false;
          yield* dirty(rows[0].observer_thread_id);
          return true;
        }),
      );
    }),
    withdraw: Effect.fn("OverseerStore.withdraw")(function* (key) {
      return yield* transaction(
        "withdraw",
        Effect.gen(function* () {
          const rows = yield* sql<OccurrenceRow>`SELECT * FROM overseer_occurrences
          WHERE link_id = ${key.linkId} AND kind = ${key.kind} AND source_id = ${key.sourceId} AND disposition = 'pending'`;
          if (!rows[0]) return false;
          yield* invalidateOccurrence(rows[0]);
          yield* sql`UPDATE overseer_occurrences SET disposition = 'withdrawn', batch_number = NULL
            WHERE link_id = ${key.linkId} AND kind = ${key.kind} AND source_id = ${key.sourceId}`;
          yield* dirty(rows[0].observer_thread_id);
          return true;
        }),
      );
    }),
    pendingCandidates: Effect.fn("OverseerStore.pendingCandidates")(
      function* (observer, after, limit) {
        return yield* execute(
          "pendingCandidates",
          sql<OccurrenceRow>`SELECT * FROM overseer_occurrences
        WHERE observer_thread_id = ${observer} AND disposition = 'pending' AND ready = 0
          AND (source_sequence, source_id, link_id) > (${after?.sequence ?? -1}, ${after?.sourceId ?? ""}, ${after?.linkId ?? ""})
        ORDER BY source_sequence, source_id, link_id LIMIT ${pageSize(limit)}`,
        );
      },
    ),
    batchEntries: Effect.fn("OverseerStore.batchEntries")(function* (observer, batchNumber) {
      return yield* execute(
        "batchEntries",
        sql<OccurrenceRow>`SELECT * FROM overseer_occurrences
        WHERE observer_thread_id = ${observer} AND batch_number = ${batchNumber}
        ORDER BY source_sequence, kind, source_id, link_id LIMIT ${batchSize}`,
      );
    }),
    stageBatch: Effect.fn("OverseerStore.stageBatch")(function* (fence) {
      return yield* transaction(
        "stageBatch",
        Effect.gen(function* () {
          const box = yield* requireFence(fence);
          if (box.hold_reason !== null) return { state: "held" as const, reason: box.hold_reason };
          const hub = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads
            WHERE thread_id = ${fence.observerThreadId} AND archived_at IS NULL AND deleted_at IS NULL`;
          if (hub.length === 0) return { state: "blocked" as const };
          const existing = (yield* sql<BatchRow>`SELECT * FROM overseer_batches
          WHERE observer_thread_id = ${fence.observerThreadId} AND state IN ('queued', 'frozen')`)[0];
          if (existing?.state === "frozen") {
            const attempt = yield* pendingAttempt(fence.observerThreadId);
            if (attempt) return { state: "busy" as const, attempt };
            const members = yield* sql<OccurrenceRow>`SELECT * FROM overseer_occurrences
              WHERE observer_thread_id = ${fence.observerThreadId} AND batch_number = ${existing.batch_number} LIMIT ${batchSize}`;
            if ((yield* reconcileQuestions(fence.observerThreadId, members)) > 0)
              return { state: "reconciled" as const };
            if (!(yield* batchEligible(fence.observerThreadId, existing.batch_number))) {
              yield* disposeBatch(
                fence.observerThreadId,
                existing.batch_number,
                "lifecycle",
                false,
              );
              yield* dirty(fence.observerThreadId);
              return { state: "reconciled" as const };
            }
            return { state: "ready" as const, batch: existing };
          }
          const entries = yield* eligible(fence.observerThreadId);
          if ((yield* reconcileQuestions(fence.observerThreadId, entries)) > 0)
            return { state: "reconciled" as const };
          if (entries.length === 0) {
            if (existing)
              yield* disposeBatch(
                fence.observerThreadId,
                existing.batch_number,
                "lifecycle",
                false,
              );
            return (yield* hasUnfinished(fence.observerThreadId))
              ? { state: "blocked" as const }
              : { state: "idle" as const };
          }
          const number = existing?.batch_number ?? box.next_batch_number;
          if (!existing) {
            yield* sql`INSERT INTO overseer_batches (observer_thread_id, batch_number, epoch, revision, state) VALUES (${fence.observerThreadId}, ${number}, ${fence.epoch}, ${fence.revision}, 'queued')`;
            yield* sql`UPDATE overseer_mailboxes SET next_batch_number = next_batch_number + 1 WHERE observer_thread_id = ${fence.observerThreadId}`;
          } else {
            yield* sql`UPDATE overseer_occurrences SET batch_number = NULL
            WHERE observer_thread_id = ${fence.observerThreadId} AND batch_number = ${number}`;
            yield* sql`UPDATE overseer_batches SET revision = ${fence.revision}, epoch = ${fence.epoch}
            WHERE observer_thread_id = ${fence.observerThreadId} AND batch_number = ${number}`;
          }
          for (const entry of entries)
            yield* sql`UPDATE overseer_occurrences SET batch_number = ${number}
          WHERE link_id = ${entry.link_id} AND kind = ${entry.kind} AND source_id = ${entry.source_id}`;
          return {
            state: "ready" as const,
            batch: {
              observer_thread_id: fence.observerThreadId,
              batch_number: number,
              epoch: fence.epoch,
              revision: fence.revision,
              state: "queued" as const,
            },
          };
        }),
      );
    }),
    freezeAttempt: Effect.fn("OverseerStore.freezeAttempt")(function* (input) {
      return yield* transaction(
        "freezeAttempt",
        Effect.gen(function* () {
          const box = yield* mailbox(input.observerThreadId);
          const batch = (yield* sql<BatchRow>`SELECT * FROM overseer_batches
          WHERE observer_thread_id = ${input.observerThreadId} AND batch_number = ${input.batchNumber}`)[0];
          if (
            !box ||
            box.epoch !== input.epoch ||
            box.hold_mask !== 0 ||
            box.revision < input.revision ||
            !batch ||
            batch.epoch !== input.epoch ||
            batch.revision !== input.revision ||
            !["queued", "frozen"].includes(batch.state)
          ) {
            return yield* new OverseerConflict({ reason: "stale_mailbox" });
          }
          if (!(yield* batchEligible(input.observerThreadId, input.batchNumber)))
            return yield* new OverseerConflict({ reason: "stale_mailbox" });
          const previous = (yield* sql<AttemptRow>`SELECT * FROM overseer_attempts
            WHERE message_id = ${input.messageId} OR command_id = ${input.commandId}`)[0];
          if (previous) {
            if (
              previous.message_id !== input.messageId ||
              previous.command_id !== input.commandId ||
              previous.observer_thread_id !== input.observerThreadId ||
              previous.batch_number !== input.batchNumber ||
              previous.state !== "pending"
            )
              return yield* new OverseerConflict({ reason: "attempt_identity" });
            return previous;
          }
          if (yield* pendingAttempt(input.observerThreadId))
            return yield* new OverseerConflict({ reason: "attempt_identity" });
          const rows = yield* sql<AttemptRow>`INSERT INTO overseer_attempts
          (observer_thread_id, batch_number, attempt_number, command_id, message_id, state)
          SELECT ${input.observerThreadId}, ${input.batchNumber}, COALESCE(MAX(attempt_number), 0) + 1, ${input.commandId}, ${input.messageId}, 'pending'
          FROM overseer_attempts WHERE observer_thread_id = ${input.observerThreadId} AND batch_number = ${input.batchNumber} RETURNING *`;
          yield* sql`UPDATE overseer_batches SET state = 'frozen' WHERE observer_thread_id = ${input.observerThreadId} AND batch_number = ${input.batchNumber}`;
          return rows[0]!;
        }),
      );
    }),
    pendingAttempt: (observer) => execute("pendingAttempt", pendingAttempt(observer)),
    bindAttempt: Effect.fn("OverseerStore.bindAttempt")(function* (messageId, runId, runAttemptId) {
      return yield* transaction(
        "bindAttempt",
        sql`UPDATE overseer_attempts SET run_id = ${runId}, run_attempt_id = ${runAttemptId}
        WHERE message_id = ${messageId} AND state <> 'disposed' AND (run_id IS NULL OR run_id = ${runId})
          AND (run_attempt_id IS NULL OR run_attempt_id = ${runAttemptId}) RETURNING message_id`.pipe(
          Effect.map((rows) => rows.length > 0),
        ),
      );
    }),
    attemptForMessage: (id) => execute("attemptForMessage", attemptForMessage(id)),
    attemptsForRun: Effect.fn("OverseerStore.attemptsForRun")(function* (id, after = "", limit) {
      return yield* execute(
        "attemptsForRun",
        sql<AttemptRow>`SELECT * FROM overseer_attempts
        WHERE run_id = ${id} AND message_id > ${after} ORDER BY message_id LIMIT ${pageSize(limit)}`,
      );
    }),
    acceptAttempt: Effect.fn("OverseerStore.acceptAttempt")(function* (messageId, epoch) {
      return yield* transaction(
        "acceptAttempt",
        Effect.gen(function* () {
          const attempt = yield* attemptForMessage(messageId);
          if (!attempt || attempt.state !== "pending") return false;
          const box = yield* mailbox(attempt.observer_thread_id);
          const batch =
            (yield* sql<BatchRow>`SELECT * FROM overseer_batches WHERE observer_thread_id = ${attempt.observer_thread_id} AND batch_number = ${attempt.batch_number}`)[0];
          if (
            !box ||
            box.epoch !== epoch ||
            box.hold_reason !== null ||
            batch?.epoch !== epoch ||
            batch.state !== "frozen"
          )
            return false;
          if (!(yield* batchEligible(attempt.observer_thread_id, attempt.batch_number))) {
            yield* disposeBatch(
              attempt.observer_thread_id,
              attempt.batch_number,
              "lifecycle",
              false,
            );
            yield* dirty(attempt.observer_thread_id);
            return false;
          }
          yield* sql`UPDATE overseer_occurrences SET disposition = 'accepted'
          WHERE observer_thread_id = ${attempt.observer_thread_id} AND batch_number = ${attempt.batch_number} AND disposition = 'pending'`;
          yield* sql`UPDATE overseer_attempts SET state = 'accepted' WHERE message_id = ${messageId}`;
          yield* sql`UPDATE overseer_batches SET state = 'accepted' WHERE observer_thread_id = ${attempt.observer_thread_id} AND batch_number = ${attempt.batch_number}`;
          return true;
        }),
      );
    }),
    disposeAttempt: Effect.fn("OverseerStore.disposeAttempt")(function* (messageId, reason) {
      return yield* transaction(
        "disposeAttempt",
        Effect.gen(function* () {
          const attempt = yield* attemptForMessage(messageId);
          if (!attempt || attempt.state !== "pending") return false;
          if (reason === "process_loss") {
            yield* sql`UPDATE overseer_attempts SET state = 'disposed', disposal_reason = 'process_loss' WHERE message_id = ${messageId}`;
            // Frozen membership belongs to the logical batch, not one process-bound attempt.
          } else
            yield* disposeBatch(attempt.observer_thread_id, attempt.batch_number, "cancel", true);
          yield* dirty(attempt.observer_thread_id);
          return true;
        }),
      );
    }),
    // A checkpoint cannot make the latest revision clean while any source is unfinished.
    // Older progress can be acknowledged because a newer dirty revision remains discoverable.
    checkpoint: Effect.fn("OverseerStore.checkpoint")(function* (fence) {
      return yield* transaction(
        "checkpoint",
        sql`UPDATE overseer_mailboxes SET processed_revision = ${fence.revision}
        WHERE observer_thread_id = ${fence.observerThreadId} AND epoch = ${fence.epoch}
          AND processed_revision <= ${fence.revision} AND revision >= ${fence.revision}
          AND (revision > ${fence.revision} OR (
            NOT EXISTS (SELECT 1 FROM overseer_occurrences WHERE observer_thread_id = ${fence.observerThreadId} AND disposition = 'pending' AND ready = 1)
            AND NOT EXISTS (SELECT 1 FROM overseer_occurrences WHERE observer_thread_id = ${fence.observerThreadId} AND disposition = 'pending' AND ready = 0)
          )) RETURNING observer_thread_id`.pipe(Effect.map((rows) => rows.length > 0)),
      );
    }),
    stop: Effect.fn("OverseerStore.stop")(function* (observer) {
      yield* transaction(
        "stop",
        Effect.gen(function* () {
          const batches =
            yield* sql<BatchRow>`SELECT * FROM overseer_batches WHERE observer_thread_id = ${observer} AND state IN ('queued', 'frozen')`;
          for (const batch of batches)
            yield* disposeBatch(observer, batch.batch_number, "stop", true);
          yield* sql`UPDATE overseer_mailboxes SET epoch = epoch + 1, revision = revision + 1, hold_mask = hold_mask | ${holdBits.stop} WHERE observer_thread_id = ${observer}`;
        }),
      );
    }),
    hold: Effect.fn("OverseerStore.hold")(function* (observer, reason) {
      yield* transaction(
        "hold",
        Effect.gen(function* () {
          const batches =
            yield* sql<BatchRow>`SELECT * FROM overseer_batches WHERE observer_thread_id = ${observer} AND state IN ('queued', 'frozen')`;
          for (const batch of batches)
            yield* disposeBatch(observer, batch.batch_number, "lifecycle", false);
          yield* sql`UPDATE overseer_mailboxes SET epoch = epoch + 1, revision = revision + 1,
          hold_mask = hold_mask | ${holdBits[reason]} WHERE observer_thread_id = ${observer}`;
        }),
      );
    }),
    resume: Effect.fn("OverseerStore.resume")(function* (observer, reason) {
      yield* transaction(
        "resume",
        sql`UPDATE overseer_mailboxes SET hold_mask = hold_mask & ${31 ^ holdBits[reason]},
        epoch = epoch + 1, revision = revision + 1 WHERE observer_thread_id = ${observer} AND (hold_mask & ${holdBits[reason]}) <> 0`,
      ).pipe(Effect.asVoid);
    }),
    resumeExplicitly: Effect.fn("OverseerStore.resumeExplicitly")(function* (observer) {
      yield* transaction(
        "resumeExplicitly",
        sql`UPDATE overseer_mailboxes SET hold_mask = hold_mask & 30,
        epoch = epoch + 1, revision = revision + 1 WHERE observer_thread_id = ${observer} AND (hold_mask & 1) <> 0`,
      ).pipe(Effect.asVoid);
    }),
    unlink: Effect.fn("OverseerStore.unlink")((linkId) =>
      transaction("unlink", closeLink(linkId, "unlink")),
    ),
    endpointArchived: Effect.fn("OverseerStore.endpointArchived")(function* (threadId) {
      yield* transaction(
        "endpointArchived",
        Effect.gen(function* () {
          const links = yield* endpointLinks(threadId);
          for (const link of links) yield* invalidateLink(link.link_id);
          for (const observer of new Set(links.map((link) => link.observer_thread_id)))
            yield* dirty(observer);
        }),
      );
    }),
    endpointUnarchived: Effect.fn("OverseerStore.endpointUnarchived")(function* (threadId) {
      yield* transaction(
        "endpointUnarchived",
        Effect.gen(function* () {
          const observers = new Set(
            (yield* endpointLinks(threadId)).map((row) => row.observer_thread_id),
          );
          for (const observer of observers) {
            yield* dirty(observer);
          }
        }),
      );
    }),
    endpointDeleted: Effect.fn("OverseerStore.endpointDeleted")(function* (threadId) {
      yield* transaction(
        "endpointDeleted",
        Effect.gen(function* () {
          for (const link of yield* endpointLinks(threadId))
            yield* closeLink(link.link_id, "endpoint_deleted");
          yield* retireMailbox(threadId);
        }),
      );
    }),
    projectDeleted: Effect.fn("OverseerStore.projectDeleted")(function* (projectId) {
      yield* transaction(
        "projectDeleted",
        Effect.gen(function* () {
          const links = yield* sql<{ readonly link_id: string }>`
            SELECT link_id FROM overseer_links WHERE state = 'active' AND observer_project_id = ${projectId}
            UNION SELECT link_id FROM overseer_links WHERE state = 'active' AND target_project_id = ${projectId}`;
          for (const link of links) yield* closeLink(link.link_id, "project_deleted");
          const observers = yield* sql<{ readonly observer_thread_id: string }>`
            SELECT DISTINCT observer_thread_id FROM overseer_links WHERE observer_project_id = ${projectId}`;
          for (const row of observers) yield* retireMailbox(row.observer_thread_id);
        }),
      );
    }),
    // Keep live-link dispositions and transport provenance, even after core outbox pruning.
    // Closed link tombstones still reject old keys/sources after their occurrence rows are pruned.
    pruneClosed: transaction(
      "pruneClosed",
      sql`DELETE FROM overseer_occurrences
      WHERE link_id IN (SELECT link_id FROM overseer_links WHERE state = 'closed')`,
    ).pipe(Effect.asVoid),
    clearFeatureData: transaction(
      "clearFeatureData",
      Effect.gen(function* () {
        yield* sql`DELETE FROM overseer_occurrences`;
        yield* sql`DELETE FROM overseer_attempts`;
        yield* sql`DELETE FROM overseer_batches`;
        yield* sql`DELETE FROM overseer_links`;
        yield* sql`DELETE FROM overseer_mailboxes`;
      }),
    ),
  });
});

export const layer = Layer.effect(OverseerStore, make);

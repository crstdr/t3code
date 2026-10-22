import {
  CommandId,
  MessageId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Policy from "./OverseerWakePolicy.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as OverseerFeature from "./OverseerFeature.ts";
import * as OverseerStore from "./OverseerStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as OverseerGate from "./OverseerGate.ts";
import * as Option from "effect/Option";
import { OverseerNoticeGrant } from "./OverseerNoticeGrant.ts";

const Member = Schema.Struct({
  linkId: Schema.String,
  kind: Schema.Literals(["completion", "question", "approval"]),
  sourceId: Schema.String,
});
const Members = Schema.fromJsonString(Schema.Array(Member));
const decodeMembers = Schema.decodeUnknownEffect(Members);
const encodeMembers = Schema.encodeSync(Members);
export type TransportRow = {
  readonly message_id: string;
  readonly observer_thread_id: string;
  readonly batch_number: number;
  readonly command_id: string;
  readonly epoch: number;
  readonly revision: number;
  readonly text: string;
  readonly members_json: string;
  readonly phase: "prepared" | "queued" | "frozen" | "submitted" | "received" | "retired";
  readonly run_id: string | null;
  readonly run_attempt_id: string | null;
  readonly native_uuid: string | null;
  readonly provider_session_id: string | null;
  readonly receipt_kind: string | null;
  readonly submitted_at: string | null;
  readonly unconfirmed_logged_at: string | null;
};
export class TransportError extends Schema.TaggedError<TransportError>()("OverseerTransportError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

type Prepare = {
  readonly observerThreadId: ThreadId;
  readonly batchNumber: number;
  readonly epoch: number;
  readonly revision: number;
  readonly text: string;
};
type Submission = {
  readonly messageId: MessageId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerSessionId: string;
  readonly nativeUuid?: string;
};
type Receipt = Submission & { readonly kind: "codex_response" | "claude_echo" | "claude_started" };

export class OverseerTransport extends Context.Service<
  OverseerTransport,
  {
    readonly open: (observer: ThreadId) => Effect.Effect<TransportRow | undefined, TransportError>;
    readonly forMessage: (
      message: MessageId,
    ) => Effect.Effect<TransportRow | undefined, TransportError>;
    readonly prepare: (input: Prepare) => Effect.Effect<TransportRow, TransportError>;
    readonly observe: (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      commandType?: string,
    ) => Effect.Effect<void, TransportError>;
    readonly submit: (input: Submission) => Effect.Effect<boolean, TransportError>;
    readonly handoff: <A, E, R>(
      input: Submission,
      send: (enqueued: Effect.Effect<void>) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<Option.Option<A>, E | TransportError, R>;
    readonly receipt: (input: Receipt) => Effect.Effect<boolean, TransportError>;
    readonly retire: (message: MessageId) => Effect.Effect<void, TransportError>;
    readonly reconcileUnsent: (message: MessageId) => Effect.Effect<boolean, TransportError>;
    readonly abandonUnsent: (message: MessageId) => Effect.Effect<void, TransportError>;
    readonly afterProcessLoss: (
      observer: ThreadId,
      disabled?: boolean,
    ) => Effect.Effect<void, TransportError>;
    readonly restoreRetry: (observer: ThreadId) => Effect.Effect<boolean, TransportError>;
    readonly warnUnconfirmed: (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      commandType?: string,
    ) => Effect.Effect<void, TransportError>;
  }
>()("t3/overseer/OverseerTransport") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* OverseerStore.OverseerStore;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const gate = yield* OverseerGate.OverseerGate;
  const transaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    gate.commit(sql.withTransaction(effect));
  const execute = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError((cause) => new TransportError({ operation, cause })));
  const forMessage = Effect.fn("OverseerTransport.forMessage")(function* (message: string) {
    return (yield* sql<TransportRow>`SELECT * FROM overseer_transports WHERE message_id = ${message}`)[0];
  });
  const open = Effect.fn("OverseerTransport.open")(function* (observer: string) {
    return (yield* sql<TransportRow>`SELECT * FROM overseer_transports WHERE observer_thread_id = ${observer}
      AND phase IN ('prepared', 'queued', 'frozen', 'submitted')`)[0];
  });
  const retire = Effect.fn("OverseerTransport.retire")(function* (message: string) {
    const row = yield* forMessage(message);
    yield* sql`UPDATE overseer_transports SET phase = 'retired' WHERE message_id = ${message} AND phase IN ('prepared', 'queued', 'frozen')`;
    if (row?.phase === "frozen")
      yield* store.releaseUnsentBatch(ThreadId.make(row.observer_thread_id), row.batch_number);
    if (row && ["prepared", "queued", "frozen"].includes(row.phase))
      yield* Policy.releaseIdleBatch(row.observer_thread_id, row.batch_number).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
  });
  const reconcileUnsent = Effect.fn("OverseerTransport.reconcileUnsent")(function* (
    message: string,
  ) {
    const row = yield* forMessage(message);
    if (!row || row.phase !== "frozen") return false;
    const core = (yield* sql<{
      readonly status: string;
      readonly attempt: string | null;
      readonly message: string | null;
      readonly session: string | null;
    }>`
      SELECT r.status, json_extract(r.payload_json, '$.activeAttemptId') AS attempt, json_extract(r.payload_json, '$.userMessageId') AS message,
        p.provider_session_id AS session FROM orchestration_v2_projection_runs r
      LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = r.provider_thread_id WHERE r.run_id = ${row.run_id}`)[0];
    if (
      core &&
      ["starting", "running", "waiting"].includes(core.status) &&
      (row.run_attempt_id === null || core.attempt === row.run_attempt_id) &&
      (row.provider_session_id === null || row.provider_session_id === core.session)
    )
      return true;
    yield* retire(message);
    const observer = ThreadId.make(row.observer_thread_id);
    if (core?.status === "failed" && core.message === message)
      yield* store.hold(observer, "provider");
    else yield* store.touch(observer);
    return false;
  });
  const valid = Effect.fn("OverseerTransport.valid")(function* (row: TransportRow) {
    const box = yield* store.mailbox(ThreadId.make(row.observer_thread_id));
    if (!box || box.epoch !== row.epoch || box.hold_mask !== 0) return false;
    const suspended =
      yield* sql`SELECT 1 FROM orchestration_v2_projection_threads t WHERE t.thread_id = ${row.observer_thread_id}
      AND (json_extract(t.payload_json, '$.snoozedUntil') IS NOT NULL OR EXISTS (
        SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id = ${row.observer_thread_id}
          AND r.status = 'queued' AND json_extract(r.payload_json, '$.queueHeld') = 1))`;
    if (suspended.length > 0) return false;
    const batch = (yield* sql<{
      readonly state: string;
      readonly revision: number;
      readonly epoch: number;
    }>`SELECT state, revision, epoch FROM overseer_batches
      WHERE observer_thread_id = ${row.observer_thread_id} AND batch_number = ${row.batch_number}`)[0];
    if (
      !batch ||
      !["queued", "frozen"].includes(batch.state) ||
      batch.epoch !== row.epoch ||
      batch.revision !== row.revision
    )
      return false;
    if (!(yield* store.batchDispatchable(ThreadId.make(row.observer_thread_id), row.batch_number)))
      return false;
    const members = (yield* store.batchEntries(
      ThreadId.make(row.observer_thread_id),
      row.batch_number,
    )).map((entry) => ({ linkId: entry.link_id, kind: entry.kind, sourceId: entry.source_id }));
    return encodeMembers(members) === row.members_json;
  });
  const clearRetry = Effect.fn("OverseerTransport.clearRetry")(function* (row: TransportRow) {
    yield* sql`UPDATE overseer_mailboxes SET retry_message_id = NULL WHERE observer_thread_id = ${row.observer_thread_id}
      AND EXISTS (SELECT 1 FROM overseer_transports t WHERE t.message_id = retry_message_id AND t.batch_number = ${row.batch_number})`;
  });
  const service: OverseerTransport["Service"] = OverseerTransport.of({
    handoff: (input, send) => gate.handoff(service.submit(input), send),
    open: (observer) => execute("open", open(observer)),
    forMessage: (message) => execute("forMessage", forMessage(message)),
    retire: (message) => execute("retire", transaction(retire(message))),
    reconcileUnsent: (message) => execute("reconcileUnsent", transaction(reconcileUnsent(message))),
    abandonUnsent: Effect.fn("OverseerTransport.abandonUnsent")(function* (message) {
      if (!message.startsWith("message:overseer:") || !(yield* OverseerFeature.Enabled)) return;
      yield* execute(
        "abandonUnsent",
        transaction(
          Effect.gen(function* () {
            const row = yield* forMessage(message);
            if (row?.phase !== "frozen") return;
            yield* retire(message);
            yield* store.hold(ThreadId.make(row.observer_thread_id), "provider");
          }),
        ),
      );
    }),
    afterProcessLoss: Effect.fn("OverseerTransport.afterProcessLoss")(function* (
      observer,
      disabled = false,
    ) {
      yield* execute(
        "process-loss",
        transaction(
          Effect.gen(function* () {
            const row = yield* open(observer);
            if (row && (disabled || row.phase === "frozen" || row.phase === "submitted")) {
              yield* sql`UPDATE overseer_mailboxes SET retry_message_id = COALESCE(retry_message_id, ${row.message_id})
            WHERE observer_thread_id = ${observer} AND (hold_mask & 1) = 0`;
              yield* store.disposeAttempt(MessageId.make(row.message_id), "process_loss");
              yield* sql`UPDATE overseer_transports SET phase = 'retired' WHERE message_id = ${row.message_id}`;
              yield* store.touch(observer);
            }
            const orphan = yield* store.pendingAttempt(observer);
            if (orphan && (!row || row.message_id !== orphan.message_id))
              yield* store.disposeAttempt(MessageId.make(orphan.message_id), "process_loss");
          }),
        ),
      );
    }),
    restoreRetry: Effect.fn("OverseerTransport.restoreRetry")(function* (observer) {
      if (!(yield* OverseerFeature.Enabled)) return false;
      return yield* execute(
        "restore-retry",
        transaction(
          Effect.gen(function* () {
            const box = yield* store.mailbox(observer);
            if (!box?.retry_message_id) return true;
            if (box.hold_mask !== 0) return false;
            const origin = yield* forMessage(box.retry_message_id);
            if (!origin)
              return yield* new OverseerStore.OverseerConflict({ reason: "attempt_identity" });
            const originBatch = (yield* sql<{
              readonly purpose: string;
              readonly idle_token: string | null;
            }>`SELECT purpose, idle_token FROM overseer_batches
          WHERE observer_thread_id = ${observer} AND batch_number = ${origin.batch_number}`)[0];
            if (originBatch?.purpose === "idle") {
              if (
                !(yield* Policy.idleBatchEligible(observer, origin.batch_number, true).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                ))
              ) {
                yield* clearRetry(origin);
                yield* store.releaseUnsentBatch(observer, origin.batch_number);
                yield* Policy.syncIdleState(observer).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                );
                return true;
              }
              yield* sql`UPDATE overseer_batches SET state = 'frozen', epoch = ${box.epoch}, revision = ${box.revision}
            WHERE observer_thread_id = ${observer} AND batch_number = ${origin.batch_number} AND state IN ('queued', 'frozen', 'disposed')`;
              return true;
            }
            const policy = yield* Policy.admission(observer).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            const survivors: Array<typeof Member.Type> = [];
            for (const member of yield* decodeMembers(origin.members_json)) {
              if (member.kind === "completion" && !policy.allowRoutine) continue;
              const row = (yield* sql<{
                readonly ready: number;
                readonly state: string;
                readonly available: number;
              }>`
            SELECT o.ready, l.state, (t.thread_id IS NOT NULL AND t.archived_at IS NULL AND t.deleted_at IS NULL) AS available
            FROM overseer_occurrences o JOIN overseer_links l ON l.link_id = o.link_id
            LEFT JOIN orchestration_v2_projection_threads t ON t.thread_id = l.target_thread_id
            WHERE o.link_id = ${member.linkId} AND o.kind = ${member.kind} AND o.source_id = ${member.sourceId}
              AND o.disposition = 'pending'`)[0];
              if (!row || row.state !== "active") continue;
              if (row.ready !== 1 || row.available !== 1) continue;
              survivors.push(member);
            }
            if (survivors.length === 0) {
              yield* clearRetry(origin);
              yield* store.releaseUnsentBatch(observer, origin.batch_number);
              return true;
            }
            const other = yield* sql<{
              readonly batch_number: number;
            }>`SELECT batch_number FROM overseer_batches
          WHERE observer_thread_id = ${observer} AND state IN ('queued', 'frozen') AND batch_number <> ${origin.batch_number}`;
            for (const batch of other)
              yield* store.releaseUnsentBatch(observer, batch.batch_number);
            yield* sql`UPDATE overseer_occurrences SET batch_number = NULL WHERE observer_thread_id = ${observer} AND batch_number = ${origin.batch_number}`;
            const latest = (yield* store.mailbox(observer))!;
            yield* sql`UPDATE overseer_batches SET state = 'frozen', epoch = ${latest.epoch}, revision = ${latest.revision}
          WHERE observer_thread_id = ${observer} AND batch_number = ${origin.batch_number} AND state IN ('queued', 'frozen', 'disposed')`;
            for (const member of survivors)
              yield* sql`UPDATE overseer_occurrences SET batch_number = ${origin.batch_number}
          WHERE link_id = ${member.linkId} AND kind = ${member.kind} AND source_id = ${member.sourceId} AND disposition = 'pending' AND ready = 1`;
            return true;
          }),
        ),
      );
    }),
    prepare: Effect.fn("OverseerTransport.prepare")(function* (input) {
      return yield* execute(
        "prepare",
        transaction(
          Effect.gen(function* () {
            const box = yield* store.mailbox(input.observerThreadId);
            if (
              !box ||
              box.epoch !== input.epoch ||
              box.revision !== input.revision ||
              box.hold_mask !== 0
            )
              return yield* new OverseerStore.OverseerConflict({ reason: "stale_mailbox" });
            let existing = yield* open(input.observerThreadId);
            if (existing?.phase === "prepared") {
              const rejected = yield* sql`SELECT 1 FROM orchestration_command_receipts
                WHERE command_id = ${existing.command_id} AND status = 'rejected'`;
              if (rejected.length > 0) {
                yield* retire(existing.message_id);
                existing = undefined;
              }
            }
            if (existing && !["prepared", "queued"].includes(existing.phase))
              return yield* new OverseerStore.OverseerConflict({ reason: "attempt_identity" });
            const members = (yield* store.batchEntries(
              input.observerThreadId,
              input.batchNumber,
            )).map((entry) => ({
              linkId: entry.link_id,
              kind: entry.kind,
              sourceId: entry.source_id,
            }));
            const id = (yield* sql<{
              readonly id: string;
            }>`SELECT lower(hex(randomblob(16))) AS id`)[0]!.id;
            const message = existing?.message_id ?? `message:overseer:${id}`;
            const rows = yield* sql<TransportRow>`INSERT INTO overseer_transports
          (message_id, observer_thread_id, batch_number, command_id, epoch, revision, text, members_json, phase)
          VALUES (${message}, ${input.observerThreadId}, ${input.batchNumber}, ${existing?.command_id ?? `command:overseer:${id}`},
            ${input.epoch}, ${input.revision}, ${input.text}, ${encodeMembers(members)}, 'prepared')
          ON CONFLICT(message_id) DO UPDATE SET batch_number = excluded.batch_number, epoch = excluded.epoch,
            revision = excluded.revision, text = excluded.text, members_json = excluded.members_json RETURNING *`;
            if (!(yield* valid(rows[0]!)))
              return yield* new OverseerStore.OverseerConflict({ reason: "stale_mailbox" });
            return rows[0]!;
          }),
        ),
      );
    }),
    // SQL-only, inside the original core commit. Publication and native input occur later.
    observe: Effect.fn("OverseerTransport.observe")(function* (events, commandType) {
      if (!(yield* OverseerFeature.Enabled)) return;
      yield* execute(
        "observe",
        Effect.gen(function* () {
          const messages = new Set<string>();
          for (const { event } of events) {
            if (
              event.type === "message.updated" &&
              event.payload.id.startsWith("message:overseer:")
            )
              messages.add(event.payload.id);
            if (event.type === "run.created" || event.type === "run.updated") {
              if (event.payload.userMessageId?.startsWith("message:overseer:"))
                messages.add(event.payload.userMessageId);
              const bound = yield* sql<{
                readonly message_id: string;
              }>`SELECT message_id FROM overseer_transports
                WHERE run_id = ${event.payload.id} AND phase IN ('frozen', 'submitted')`;
              for (const transport of bound) messages.add(transport.message_id);
            }
          }
          for (const message of messages) {
            const row = yield* forMessage(message);
            if (!row || row.phase === "received" || row.phase === "retired") continue;
            // Startup owns process-loss disposition; preserve the logical batch until
            // its bounded reconciliation runs after core runtime normalization.
            if (
              commandType === "provider-runtime.reconcile" &&
              ["frozen", "submitted"].includes(row.phase)
            )
              continue;
            const cancelled = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
              WHERE run_id = ${row.run_id} AND status IN ('cancelled', 'interrupted')`;
            const grant = yield* OverseerNoticeGrant;
            if (cancelled.length > 0 && grant?.messageId === message && grant.retiring) {
              yield* retire(message);
              yield* store.touch(ThreadId.make(row.observer_thread_id));
              continue;
            }
            if (
              (commandType === "queued-run.cancel" || commandType === "run.interrupt") &&
              cancelled.length > 0
            ) {
              for (const member of yield* decodeMembers(row.members_json))
                yield* sql`UPDATE overseer_occurrences SET disposition = 'suppressed', batch_number = NULL
              WHERE link_id = ${member.linkId} AND kind = ${member.kind} AND source_id = ${member.sourceId} AND disposition = 'pending'`;
              yield* store.disposeAttempt(MessageId.make(message), "cancel");
              yield* sql`UPDATE overseer_batches SET state = 'suppressed' WHERE observer_thread_id = ${row.observer_thread_id}
                AND batch_number = ${row.batch_number} AND state IN ('queued', 'frozen')`;
              yield* clearRetry(row);
              yield* sql`UPDATE overseer_transports SET phase = 'retired' WHERE message_id = ${message}`;
              yield* Policy.releaseIdleBatch(row.observer_thread_id, row.batch_number, true).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              );
              yield* store.touch(ThreadId.make(row.observer_thread_id));
              continue;
            }
            if (row.phase === "submitted") continue;
            if (row.phase === "frozen" && !(yield* reconcileUnsent(message))) continue;
            const core = (yield* sql<{
              readonly text: string;
              readonly run_id: string | null;
              readonly status: string | null;
              readonly active_attempt_id: string | null;
              readonly session: string | null;
            }>`
            SELECT json_extract(m.payload_json, '$.text') AS text, m.run_id, r.status,
              json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id, p.provider_session_id AS session FROM orchestration_v2_projection_messages m
            LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id
            LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = r.provider_thread_id WHERE m.message_id = ${message}`)[0];
            if (!core) continue;
            if (core.status === "queued") {
              // Queue maintenance (including re-hold) must never be vetoed by a stale
              // wake. Only the private publication/edit requires snapshot validation.
              if (!(yield* valid(row)) || core.text !== row.text) {
                if ((yield* OverseerNoticeGrant)?.messageId === message && core.text !== row.text)
                  return yield* new OverseerStore.OverseerConflict({ reason: "stale_mailbox" });
                yield* store.touch(ThreadId.make(row.observer_thread_id));
              }
              yield* sql`UPDATE overseer_transports SET phase = 'queued', run_id = ${core.run_id} WHERE message_id = ${message}`;
              continue;
            }
            if (
              !["starting", "running", "waiting"].includes(core.status ?? "") ||
              core.run_id === null
            )
              continue;
            if (!(yield* valid(row)) || core.text !== row.text) {
              yield* retire(message);
              yield* store.touch(ThreadId.make(row.observer_thread_id));
              continue;
            }
            yield* store.freezeAttempt({
              observerThreadId: ThreadId.make(row.observer_thread_id),
              batchNumber: row.batch_number,
              epoch: row.epoch,
              revision: row.revision,
              commandId: CommandId.make(row.command_id),
              messageId: MessageId.make(message),
            });
            if (
              core.active_attempt_id !== null &&
              !(yield* store.bindAttempt(
                MessageId.make(message),
                RunId.make(core.run_id),
                RunAttemptId.make(core.active_attempt_id),
              ))
            ) {
              yield* retire(message);
              yield* store.touch(ThreadId.make(row.observer_thread_id));
              continue;
            }
            yield* sql`UPDATE overseer_transports SET phase = 'frozen', run_id = ${core.run_id}, run_attempt_id = ${core.active_attempt_id}, provider_session_id = ${core.status === "starting" ? null : core.session} WHERE message_id = ${message}`;
          }
        }),
      );
    }),
    submit: Effect.fn("OverseerTransport.submit")(function* (input) {
      if (!input.messageId.startsWith("message:overseer:")) return true;
      if (!(yield* OverseerFeature.Enabled)) return false;
      return yield* execute(
        "submit",
        transaction(
          Effect.gen(function* () {
            const row = yield* forMessage(input.messageId);
            if (!row || row.phase !== "frozen") return false;
            if (row.run_id !== input.runId || row.run_attempt_id !== input.attemptId) return false;
            if (!(yield* reconcileUnsent(input.messageId))) return false;
            const current = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs r
                JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = r.provider_thread_id
                WHERE r.run_id = ${input.runId} AND r.thread_id = ${row.observer_thread_id} AND r.status = 'running'
                  AND json_extract(r.payload_json, '$.activeAttemptId') = ${input.attemptId}
                  AND p.provider_session_id = ${input.providerSessionId}`;
            if (current.length === 0) return false;
            if (
              row.run_id !== input.runId ||
              row.run_attempt_id !== input.attemptId ||
              !(yield* valid(row))
            ) {
              yield* retire(input.messageId);
              yield* store.touch(ThreadId.make(row.observer_thread_id));
              return false;
            }
            // Persist uncertainty before the external handoff. handoff() retains the
            // feature gate until outbound enqueue, but never through the native response.
            yield* sql`UPDATE overseer_transports SET phase = 'submitted', native_uuid = ${input.nativeUuid ?? null},
          provider_session_id = ${input.providerSessionId}, submitted_at = ${DateTime.formatIso(yield* DateTime.now)} WHERE message_id = ${input.messageId}`;
            return true;
          }),
        ),
      );
    }),
    warnUnconfirmed: Effect.fn("OverseerTransport.warnUnconfirmed")(
      function* (events, commandType) {
        if (!(yield* OverseerFeature.Enabled) || commandType === "provider-runtime.reconcile")
          return;
        const runIds = new Set(
          events.flatMap(({ event }) =>
            event.type === "run.updated" &&
            ["completed", "failed", "cancelled", "interrupted"].includes(event.payload.status)
              ? [event.payload.id]
              : [],
          ),
        );
        for (const runId of runIds) {
          const rows = yield* execute(
            "warn-unconfirmed",
            transaction(sql<TransportRow>`UPDATE overseer_transports
          SET unconfirmed_logged_at = ${DateTime.formatIso(yield* DateTime.now)}
          WHERE run_id = ${runId} AND phase = 'submitted' AND unconfirmed_logged_at IS NULL RETURNING *`),
          );
          for (const row of rows)
            yield* Effect.logWarning("Overseer input ended without a provider receipt", {
              messageId: row.message_id,
              observerThreadId: row.observer_thread_id,
              runId,
              submittedAt: row.submitted_at,
            });
        }
      },
    ),
    receipt: Effect.fn("OverseerTransport.receipt")(function* (input) {
      if (!(yield* OverseerFeature.Enabled)) return false;
      return yield* execute(
        "receipt",
        transaction(
          Effect.gen(function* () {
            const row = yield* forMessage(input.messageId);
            if (
              !row ||
              row.phase !== "submitted" ||
              row.run_id !== input.runId ||
              row.run_attempt_id !== input.attemptId ||
              row.provider_session_id !== input.providerSessionId ||
              row.native_uuid !== (input.nativeUuid ?? null) ||
              (row.native_uuid === null
                ? input.kind !== "codex_response"
                : input.kind === "codex_response")
            )
              return false;
            const members = yield* decodeMembers(row.members_json);
            // The receipt describes an earlier admitted snapshot, not today's batch. A
            // resolved question stays withdrawn while surviving delivered members settle.
            let routine = false;
            for (const member of members) {
              const accepted = yield* sql`UPDATE overseer_occurrences SET disposition = 'accepted'
          WHERE link_id = ${member.linkId} AND kind = ${member.kind} AND source_id = ${member.sourceId} AND disposition = 'pending' AND ready = 1 RETURNING 1`;
              if (member.kind === "completion" && accepted.length > 0) routine = true;
            }
            if (routine)
              yield* sql`INSERT INTO overseer_preferences(observer_thread_id, last_routine_accepted_at)
              VALUES (${row.observer_thread_id}, ${DateTime.formatIso(yield* DateTime.now)})
              ON CONFLICT(observer_thread_id) DO UPDATE SET last_routine_accepted_at = excluded.last_routine_accepted_at`;
            const batch = (yield* sql<{
              readonly purpose: string;
              readonly idle_token: string | null;
            }>`SELECT purpose, idle_token FROM overseer_batches
              WHERE observer_thread_id = ${row.observer_thread_id} AND batch_number = ${row.batch_number}`)[0];
            if (batch?.purpose === "idle")
              yield* sql`UPDATE overseer_preferences SET idle_pending_token = NULL, idle_due_at = NULL
              WHERE observer_thread_id = ${row.observer_thread_id} AND idle_pending_token = ${batch.idle_token}`;
            yield* sql`UPDATE overseer_attempts SET state = 'accepted' WHERE message_id = ${input.messageId} AND state = 'pending'`;
            yield* sql`UPDATE overseer_batches SET state = 'accepted' WHERE observer_thread_id = ${row.observer_thread_id}
          AND batch_number = ${row.batch_number} AND state = 'frozen'`;
            yield* sql`UPDATE overseer_transports SET phase = 'received', receipt_kind = ${input.kind} WHERE message_id = ${input.messageId}`;
            yield* clearRetry(row);
            yield* store.touch(ThreadId.make(row.observer_thread_id));
            yield* Policy.syncIdleState(row.observer_thread_id).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            return true;
          }),
        ),
      ).pipe(Effect.tap((accepted) => (accepted ? outbox.notifyAvailable() : Effect.void)));
    }),
  });
  return service;
});

export const layer = Layer.effect(OverseerTransport, make).pipe(
  Layer.provide(Layer.mergeAll(OverseerStore.layer, EffectOutbox.layer, OverseerGate.layer)),
);

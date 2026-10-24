import {
  CommandId,
  MessageId,
  RunId,
  type ThreadId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as OverseerFeature from "./OverseerFeature.ts";
import * as OverseerStore from "./OverseerStore.ts";
import * as OverseerTransport from "./OverseerTransport.ts";
import * as OverseerWakeFacts from "./OverseerWakeFacts.ts";
import { OverseerNoticeGrant } from "./OverseerNoticeGrant.ts";
import { renderWake, wakeText, type WakeEntry } from "./OverseerText.ts";

export class MailboxError extends Schema.TaggedError<MailboxError>()("OverseerMailboxError", {
  cause: Schema.Defect(),
}) {}
type Sync = { readonly observerThreadId: ThreadId; readonly generation: string };
export class OverseerMailbox extends Context.Service<
  OverseerMailbox,
  {
    readonly sync: <E, R>(
      input: Sync,
      dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E, R>,
    ) => Effect.Effect<void, MailboxError | E, R>;
  }
>()("t3/overseer/OverseerMailbox") {}

const isConflict = Schema.is(OverseerStore.OverseerConflict);
function staleMailbox(error: unknown): boolean {
  let current = error;
  for (let depth = 0; depth < 8; depth++) {
    if (isConflict(current)) return current.reason === "stale_mailbox";
    if (typeof current !== "object" || current === null || !("cause" in current)) return false;
    current = current.cause;
  }
  return false;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* OverseerStore.OverseerStore;
  const transport = yield* OverseerTransport.OverseerTransport;
  const facts = yield* OverseerWakeFacts.OverseerWakeFacts;
  const freshId = sql<{ readonly id: string }>`SELECT lower(hex(randomblob(16))) AS id`.pipe(
    Effect.map((rows) => rows[0]!.id),
  );
  const storage = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError((cause) => new MailboxError({ cause })));
  return OverseerMailbox.of({
    sync: Effect.fn("OverseerMailbox.sync")(
      function* (input, dispatch) {
        if (!(yield* OverseerFeature.Enabled)) return;
        const observer = input.observerThreadId;
        let box = yield* storage(store.mailbox(observer));
        if (!box || box.flush_id !== `effect:overseer-flush:${input.generation}`) return;
        const suspended = yield* storage(sql`SELECT 1 FROM orchestration_v2_projection_threads t
        WHERE t.thread_id = ${observer} AND (t.archived_at IS NOT NULL OR t.deleted_at IS NOT NULL
          OR json_extract(t.payload_json, '$.snoozedUntil') IS NOT NULL
          OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id = ${observer}
            AND r.status = 'queued' AND json_extract(r.payload_json, '$.queueHeld') = 1))`);
        let current = yield* storage(transport.open(observer));
        // Withdrawal and finished bindings can be cleaned without resuming a
        // held queue. Only private queued input is cancelled, never its followers.
        if (current && (current.phase === "prepared" || current.phase === "queued")) {
          const bound = (yield* storage(sql<{
            readonly run_id: string | null;
            readonly status: string | null;
          }>`
            SELECT r.run_id, r.status FROM orchestration_v2_projection_messages m
            LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id WHERE m.message_id = ${current.message_id}`))[0];
          if (
            (bound?.status &&
              ["completed", "failed", "cancelled", "interrupted"].includes(bound.status)) ||
            ((suspended.length > 0 || box.hold_mask !== 0) &&
              !(yield* storage(store.batchDispatchable(observer, current.batch_number))))
          ) {
            if (bound?.status === "queued" && bound.run_id)
              yield* dispatch({
                type: "queued-run.cancel",
                threadId: observer,
                runId: RunId.make(bound.run_id),
                commandId: CommandId.make(`command:overseer-retire:${yield* storage(freshId)}`),
              }).pipe(
                Effect.provideService(OverseerNoticeGrant, {
                  messageId: MessageId.make(current.message_id),
                  queued: true,
                  retiring: true,
                }),
              );
            yield* storage(transport.retire(MessageId.make(current.message_id)));
            yield* storage(store.releaseUnsentBatch(observer, current.batch_number));
            current = undefined;
            box = yield* storage(store.mailbox(observer));
            if (!box) return;
          }
        }
        if (suspended.length > 0 || box.hold_mask !== 0) return;
        // Submitted input is a process-bound receipt barrier even after a source
        // invalidation disposed its batch. Never loop or resend surviving members.
        if (current?.phase === "submitted") return;
        if (current?.phase === "frozen") {
          if (!(yield* storage(transport.reconcileUnsent(MessageId.make(current.message_id)))))
            return;
          if (yield* storage(store.batchDispatchable(observer, current.batch_number))) return;
          yield* storage(transport.retire(MessageId.make(current.message_id)));
          return;
        }
        if (!(yield* storage(transport.restoreRetry(observer)))) return;
        box = yield* storage(store.mailbox(observer));
        if (!box) return;
        const fence = { observerThreadId: observer, epoch: box.epoch, revision: box.revision };
        let staged = yield* storage(store.stageBatch(fence));
        if (staged.state === "idle" || staged.state === "blocked") {
          const idle = yield* storage(store.stageIdleBatch(fence));
          if (idle) staged = { state: "ready", batch: idle };
        }
        if (staged.state !== "ready") {
          if (staged.state !== "busy" && current) {
            const runs = yield* storage(sql<{
              readonly run_id: string;
            }>`SELECT run_id FROM orchestration_v2_projection_runs
            WHERE run_id = ${current.run_id} AND status = 'queued'`);
            if (runs[0])
              yield* dispatch({
                type: "queued-run.cancel",
                threadId: observer,
                runId: RunId.make(runs[0].run_id),
                commandId: CommandId.make(`command:overseer-retire:${yield* storage(freshId)}`),
              }).pipe(
                Effect.provideService(OverseerNoticeGrant, {
                  messageId: MessageId.make(current.message_id),
                  queued: true,
                  retiring: true,
                }),
              );
            yield* storage(transport.retire(MessageId.make(current.message_id)));
            yield* storage(store.touch(observer));
          }
          if (staged.state === "idle") yield* storage(store.checkpoint(fence));
          return;
        }
        const members = yield* storage(store.batchEntries(observer, staged.batch.batch_number));
        const entries: Array<WakeEntry> = [];
        for (const member of members) {
          const links = yield* storage(
            sql<{
              readonly target_thread_id: string;
            }>`SELECT target_thread_id FROM overseer_links WHERE link_id = ${member.link_id}`,
          );
          const threadId = links[0]?.target_thread_id;
          if (!threadId) return;
          if (member.kind !== "completion")
            entries.push({ kind: member.kind, threadId, requestId: member.source_id });
          else {
            const outcome = yield* storage(
              facts.completionOutcome(member.link_id, member.source_id),
            );
            if (!outcome) return;
            entries.push({ kind: "completion", threadId, runId: member.source_id, outcome });
          }
        }
        const batchId = `${observer}:${staged.batch.batch_number}`;
        const rendered =
          staged.batch.purpose === "idle"
            ? { text: wakeText.W3, count: 0 }
            : renderWake(batchId, entries);
        if (staged.batch.state === "frozen" && (!rendered || rendered.count !== members.length)) {
          yield* Effect.logWarning("Overseer frozen retry no longer fits its envelope", {
            observerThreadId: observer,
            batchId,
          });
          return;
        }
        if (!rendered) {
          yield* Effect.logWarning("Oversight wake identity exceeds envelope budget", {
            observer,
            batch: staged.batch.batch_number,
          });
          return;
        }
        if (
          rendered.count < members.length &&
          (yield* storage(store.stageBatch(fence, rendered.count))).state !== "ready"
        )
          return;
        const prepared = yield* storage(
          transport.prepare({
            ...fence,
            batchNumber: staged.batch.batch_number,
            text: rendered.text,
          }),
        );
        const queued = yield* storage(sql<{
          readonly run_id: string;
          readonly text: string;
          readonly status: string;
        }>`SELECT m.run_id,
        json_extract(m.payload_json, '$.text') AS text, r.status FROM orchestration_v2_projection_messages m
        JOIN orchestration_v2_projection_runs r ON r.run_id = m.run_id WHERE m.message_id = ${prepared.message_id}`);
        const messageId = MessageId.make(prepared.message_id);
        if (queued[0]) {
          if (queued[0].status !== "queued" || queued[0].text === rendered.text) return;
          yield* dispatch({
            type: "queued-run.edit",
            threadId: observer,
            runId: RunId.make(queued[0].run_id),
            commandId: CommandId.make(`command:overseer-edit:${yield* storage(freshId)}`),
            text: rendered.text,
          }).pipe(Effect.provideService(OverseerNoticeGrant, { messageId, queued: true }));
        } else {
          yield* dispatch({
            type: "message.dispatch",
            threadId: observer,
            messageId,
            commandId: CommandId.make(prepared.command_id),
            text: rendered.text,
            attachments: [],
            createdBy: "agent",
            creationSource: "server",
            dispatchMode: { type: "queue_after_active" },
            notification: {
              source: { kind: "background_task" },
              outcome: "updated",
              summary: wakeText.W2,
            },
          }).pipe(
            Effect.provideService(OverseerNoticeGrant, {
              messageId,
              queued: staged.batch.purpose === "idle",
            }),
          );
        }
      },
      Effect.catch((error) => (staleMailbox(error) ? Effect.void : Effect.fail(error))),
    ),
  });
});

export const layer = Layer.effect(OverseerMailbox, make).pipe(
  Layer.provide(
    Layer.mergeAll(OverseerStore.layer, OverseerTransport.layer, OverseerWakeFacts.layer),
  ),
);

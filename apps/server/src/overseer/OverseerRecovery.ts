import {
  CommandId,
  MessageId,
  RunId,
  ThreadId,
  resolveProjectProviderInstanceEnabled,
  type ProviderInstanceId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Settings from "../serverSettings.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Store from "./OverseerStore.ts";
import * as Facts from "./OverseerWakeFacts.ts";
import * as Transport from "./OverseerTransport.ts";
import * as Flush from "./OverseerFlush.ts";
import * as Gate from "./OverseerGate.ts";
import { OverseerNoticeGrant } from "./OverseerNoticeGrant.ts";

export class RecoveryError extends Schema.TaggedError<RecoveryError>()("OverseerRecoveryError", {
  cause: Schema.Defect(),
}) {}
export class OverseerRecovery extends Context.Service<
  OverseerRecovery,
  {
    // Startup only: core runtime normalization must precede this, workers follow it.
    readonly recover: Effect.Effect<void>;
  }
>()("t3/overseer/OverseerRecovery") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.OverseerStore;
  const transport = yield* Transport.OverseerTransport;
  const facts = yield* Facts.OverseerWakeFacts;
  const outbox = yield* Outbox.EffectOutboxV2;
  const gate = yield* Gate.OverseerGate;
  const settings = yield* Settings.ServerSettingsService;
  const orchestrator = yield* Effect.serviceOption(Orchestrator.OrchestratorV2);
  const adapters = yield* Effect.serviceOption(Adapters.ProviderAdapterRegistryV2);
  const startup = yield* Semaphore.make(1);
  let recovered = false;
  const onFailure = (handle: (cause: Cause.Cause<unknown>) => Effect.Effect<void>) =>
    Effect.catchCause((cause: Cause.Cause<unknown>) =>
      Cause.hasInterruptsOnly(cause) ? Effect.interrupt : handle(cause),
    );
  const hold = Effect.fn("OverseerRecovery.hold")(function* (observer: ThreadId) {
    const box = yield* store.mailbox(observer);
    if (box && (box.hold_mask & 16) === 0) yield* store.hold(observer, "authorization");
  });
  const park = Effect.fn("OverseerRecovery.park")(function* (observer: ThreadId, cause: unknown) {
    yield* hold(observer).pipe(
      onFailure((holdCause) =>
        Effect.logWarning("Overseer recovery could not persist its hold", {
          observerThreadId: observer,
          cause: holdCause,
        }),
      ),
    );
    yield* Effect.logWarning("Overseer recovery parked; explicit rearm required", {
      observerThreadId: observer,
      cause,
    });
  });
  const cancelEffects = Effect.fn("OverseerRecovery.cancelEffects")(function* (observer: ThreadId) {
    const cancelled = yield* outbox.cancelUnsettled({
      threadId: observer,
      effectTypes: ["overseer.mailbox.flush"],
      reason: "overseer recovery held",
    });
    yield* outbox.signalCancellations(cancelled);
  });

  const repairLink = Effect.fn("OverseerRecovery.repairLink")(function* (link: Store.LinkRow) {
    const observer = ThreadId.make(link.observer_thread_id);
    const projects = yield* sql<{
      readonly project_id: string;
      readonly deleted_at: string | null;
    }>`
      SELECT project_id, deleted_at FROM projection_projects WHERE project_id IN (${link.observer_project_id}, ${link.target_project_id})`;
    for (const id of new Set([link.observer_project_id, link.target_project_id])) {
      const project = projects.find((row) => row.project_id === id);
      if (!project) {
        yield* park(observer, { missingProjectId: id });
        return;
      }
      if (project.deleted_at !== null) {
        yield* store.closeObligation(link.link_id, "project_deleted");
        return;
      }
    }
    const endpoints = yield* sql<{
      readonly thread_id: string;
      readonly deleted_at: string | null;
    }>`
      SELECT thread_id, deleted_at FROM orchestration_v2_projection_threads WHERE thread_id IN (${link.observer_thread_id}, ${link.target_thread_id})`;
    for (const id of new Set([link.observer_thread_id, link.target_thread_id])) {
      const endpoint = endpoints.find((row) => row.thread_id === id);
      if (!endpoint) {
        if (id !== link.target_thread_id || link.launch_accepted === 1)
          yield* park(observer, { missingThreadId: id });
        return;
      }
      if (endpoint.deleted_at !== null) {
        yield* store.closeObligation(link.link_id, "endpoint_deleted");
        return;
      }
    }
  });
  const repairLinks = Effect.fn("OverseerRecovery.repairLinks")(function* () {
    let after = "";
    for (;;) {
      const links =
        yield* sql<Store.LinkRow>`SELECT * FROM overseer_links WHERE state = 'active' AND link_id > ${after} ORDER BY link_id LIMIT 100`;
      if (links.length === 0) break;
      for (const link of links)
        yield* repairLink(link).pipe(
          onFailure((cause) => park(ThreadId.make(link.observer_thread_id), cause)),
        );
      after = links[links.length - 1]!.link_id;
    }
  });
  const cleanQueued = Effect.fn("OverseerRecovery.cleanQueued")(function* (
    observer: ThreadId,
    disabled: boolean,
  ) {
    let after = "";
    for (;;) {
      const rows = yield* sql<{
        readonly run_id: string;
        readonly message_id: string;
        readonly phase: string;
      }>`
        SELECT r.run_id, t.message_id, t.phase FROM orchestration_v2_projection_runs r
        JOIN overseer_transports t ON t.message_id = json_extract(r.payload_json, '$.userMessageId')
        WHERE r.thread_id = ${observer} AND r.status = 'queued' AND r.run_id > ${after}
        ORDER BY r.run_id LIMIT 100`;
      if (rows.length === 0) break;
      for (const row of rows) {
        if (!disabled && row.phase !== "retired") continue;
        if (Option.isNone(orchestrator))
          return yield* new RecoveryError({ cause: "missing private queue cleanup dispatcher" });
        const nonce = (yield* sql<{
          readonly id: string;
        }>`SELECT lower(hex(randomblob(16))) AS id`)[0]!.id;
        yield* orchestrator.value
          .dispatch({
            type: "queued-run.cancel",
            threadId: observer,
            runId: RunId.make(row.run_id),
            commandId: CommandId.make(`command:overseer-recovery-cancel:${nonce}`),
          })
          .pipe(
            Effect.provideService(OverseerNoticeGrant, {
              messageId: MessageId.make(row.message_id),
              queued: true,
              retiring: true,
            }),
          );
      }
      after = rows[rows.length - 1]!.run_id;
    }
  });
  const recoverObserver = Effect.fn("OverseerRecovery.recoverObserver")(function* (
    observer: ThreadId,
    enabled: boolean,
  ) {
    const shell = (yield* sql<{
      readonly project_id: ProjectId;
      readonly provider_instance_id: ProviderInstanceId;
      readonly deleted_at: string | null;
    }>`
      SELECT project_id, provider_instance_id, deleted_at FROM orchestration_v2_projection_threads WHERE thread_id = ${observer}`)[0];
    const project = shell
      ? (yield* sql<{
          readonly deleted_at: string | null;
        }>`SELECT deleted_at FROM projection_projects WHERE project_id = ${shell.project_id}`)[0]
      : undefined;
    if (!shell || !project) {
      yield* park(
        observer,
        !shell ? { missingThreadId: observer } : { missingProjectId: shell?.project_id },
      );
      yield* cancelEffects(observer);
      return;
    }
    if (shell.deleted_at !== null || project.deleted_at !== null) {
      yield* store.endpointDeleted(observer);
      yield* cancelEffects(observer);
      return;
    }
    if (!enabled) {
      // Commit disarm before any retirement: a crash must never leave retry
      // provenance without the hold that disables automatic catchup.
      yield* hold(observer);
      yield* transport.afterProcessLoss(observer, true);
      yield* cancelEffects(observer);
      yield* cleanQueued(observer, true);
      return;
    }
    yield* facts.reconcileObserver(observer);
    yield* transport.afterProcessLoss(observer);
    yield* cleanQueued(observer, false);
    const currentSettings = yield* settings.getSettings;
    let providerEnabled = resolveProjectProviderInstanceEnabled(
      currentSettings,
      shell.project_id,
      shell.provider_instance_id,
    );
    if (providerEnabled && Option.isSome(adapters)) {
      const result = yield* adapters.value.get(shell.provider_instance_id).pipe(Effect.result);
      providerEnabled =
        result._tag === "Success" &&
        (result.success.driver === "codex" || result.success.driver === "claudeAgent");
    }
    const box = (yield* store.mailbox(observer))!;
    if (!providerEnabled && (box.hold_mask & 8) === 0) yield* store.hold(observer, "provider");
    else if (providerEnabled && (box.hold_mask & 8) !== 0)
      yield* store.resume(observer, "provider");
    const pending = (yield* store.mailbox(observer))!;
    if (pending.revision > pending.processed_revision)
      yield* gate.commit(
        sql.withTransaction(
          Flush.schedule(observer).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        ),
      );
  });
  const recover = Effect.gen(function* () {
    if (recovered) return;
    const enabled = yield* Feature.Enabled;
    yield* repairLinks();
    let after = "";
    for (;;) {
      const observers = yield* store.recoveryMailboxes(after, 100, true);
      if (observers.length === 0) break;
      for (const id of observers) {
        const observer = ThreadId.make(id);
        yield* recoverObserver(observer, enabled).pipe(
          onFailure((cause) =>
            park(observer, cause).pipe(
              Effect.andThen(cancelEffects(observer)),
              onFailure((cleanupCause) =>
                Effect.logWarning("Overseer recovery cleanup failed", {
                  observerThreadId: observer,
                  cause: cleanupCause,
                }),
              ),
            ),
          ),
        );
      }
      after = observers[observers.length - 1]!;
    }
    if (enabled) yield* outbox.notifyAvailable();
    recovered = true;
  }).pipe(
    startup.withPermits(1),
    onFailure((cause) =>
      Effect.logWarning("Overseer recovery unavailable; ordinary startup continues", { cause }),
    ),
  );
  return OverseerRecovery.of({ recover });
});

export const layer = Layer.effect(OverseerRecovery, make).pipe(
  Layer.provide(
    Layer.mergeAll(Store.layer, Transport.layer, Facts.layer, Outbox.layer, Gate.layer),
  ),
);

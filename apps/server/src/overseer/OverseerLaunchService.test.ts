import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ToolAccess from "../mcp/McpToolAccess.ts";
import { isLiveCallerShell } from "../mcp/OrchestratorMcpService.ts";
import * as Handlers from "../mcp/toolkits/project/handlers.ts";
import { ProjectToolkit, ProjectToolkitOverseer } from "../mcp/toolkits/project/tools.ts";
import { assert, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  CommandId,
  EventId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Worker from "../orchestration-v2/EffectWorker.ts";
import * as McpInvocation from "../mcp/McpInvocationContext.ts";
import * as Config from "../config.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Recovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Store from "./OverseerStore.ts";
import * as Saga from "./OverseerLaunchService.ts";
import { Tool } from "effect/ai";
const LaunchParameters = ProjectToolkit.tools.t3_thread_launch.parametersSchema;
const OverseerLaunchParameters = ProjectToolkitOverseer.tools.t3_thread_launch.parametersSchema;
import {
  enabled,
  harness,
  invocation,
  observer,
  projectB,
  request,
  setup,
} from "./OverseerLaunch.testkit.ts";
const decodeLaunch = Schema.decodeUnknownEffect(LaunchParameters, {
  onExcessProperty:
    Tool.getStrictMode(ProjectToolkit.tools.t3_thread_launch) === true ? "error" : "ignore",
});
const decodeStrictOverseer = Schema.decodeUnknownEffect(OverseerLaunchParameters, {
  onExcessProperty: "error",
});

it.effect(
  "reserves before core creation and returns identical receipts across concurrent retries",
  () =>
    Effect.gen(function* () {
      const saga = yield* setup();
      const [a, b] = yield* enabled(
        Effect.all([saga.launch(request), saga.launch(request)], { concurrency: "unbounded" }),
      );
      assert.equal(a.linkId, b.linkId);
      assert.equal(a.threadId, b.threadId);
      const store = yield* Store.OverseerStore;
      const row = yield* store.reservationFor(observer, request.clientRequestId);
      assert.equal(row?.launch_accepted, 1);
      assert.isNull(row?.launch_input_json);
      assert.equal(a.projection.messages.filter((m) => m.id === a.initialMessageId).length, 1);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM overseer_links`)[0]?.count,
        1,
      );
      const mismatch = yield* enabled(
        saga.launch({ ...request, message: "A different task" }),
      ).pipe(Effect.flip);
      assert.equal(mismatch.message, new Saga.OverseerLaunchError({ reason: "E4" }).message);
    }).pipe(Effect.provide(harness())),
);

it.effect(
  "rechecks residency inside the keyed lock even while background liveness remains true",
  () =>
    Effect.gen(function* () {
      const reads = yield* Ref.make(0);
      yield* Effect.gen(function* () {
        const saga = yield* setup();
        yield* Ref.set(reads, 0);
        const error = yield* enabled(saga.launch(request)).pipe(Effect.flip);
        assert.equal(error.message, new Saga.OverseerLaunchError({ reason: "E1" }).message);
        assert.equal(yield* Ref.get(reads), 3);
        assert.deepEqual(yield* (yield* Store.OverseerStore).incompleteLaunches(), []);
        const sql = yield* SqlClient.SqlClient;
        assert.equal(
          (yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM projection_threads WHERE thread_id <> ${observer}`)[0]
            ?.count,
          0,
        );
      }).pipe(
        Effect.provide(
          harness({
            readObserverShell: (shell) =>
              Ref.updateAndGet(reads, (n) => n + 1).pipe(
                Effect.map((read) => {
                  // Entry reads (readCaller and writingCaller) see residency; the locked re-read loses it.
                  if (read < 3) return shell;
                  const stopped = {
                    ...shell,
                    activeRunId: null,
                    pendingBackgroundTasks: [
                      {
                        taskId: "background",
                        kind: "subagent" as const,
                        providerInstanceId: invocation.thread!.providerInstanceId,
                      },
                    ],
                  };
                  assert.isTrue(isLiveCallerShell(stopped, invocation.thread!.providerInstanceId));
                  return stopped;
                }),
              ),
          }),
        ),
      );
    }),
);

it.effect("uses authenticated observer identity across projects", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const result = yield* enabled(saga.launch({ ...request, projectId: projectB }));
    const row = yield* (yield* Store.OverseerStore).reservationFor(
      observer,
      request.clientRequestId,
    );
    assert.equal(row?.observer_thread_id, observer);
    assert.equal(row?.target_project_id, projectB);
    assert.equal(result.projection.thread.projectId, projectB);
    assert.equal(result.projection.messages.length, 1);
    assert.equal(row?.launch_accepted, 1);
  }).pipe(Effect.provide(harness())),
);

it.effect(
  "fails closed for disabled feature, external or unauthorized caller, missing key, and spoofed input",
  () =>
    Effect.gen(function* () {
      const saga = yield* setup();
      const disabled = yield* saga
        .launch(request)
        .pipe(Effect.provideService(McpInvocation.McpInvocationContext, invocation), Effect.flip);
      assert.equal(disabled._tag, "OrchestratorMcpFailure");
      const external = yield* enabled(saga.launch(request), {
        ...invocation,
        thread: undefined,
        client: { sessionId: "external", label: "test", access: "full-access" },
      }).pipe(Effect.flip);
      assert.equal(external.message, new Saga.OverseerLaunchError({ reason: "E1" }).message);
      const denied = yield* enabled(saga.launch(request), {
        ...invocation,
        capabilities: new Set(),
      }).pipe(Effect.flip);
      assert.equal(denied._tag, "OrchestratorMcpFailure");
      const stale = yield* enabled(saga.launch(request), {
        ...invocation,
        thread: { ...invocation.thread!, providerInstanceId: ProviderInstanceId.make("other") },
      }).pipe(Effect.flip);
      assert.equal(stale._tag, "OrchestratorMcpFailure");
      const missing = yield* enabled(saga.launch({ ...request, clientRequestId: undefined })).pipe(
        Effect.flip,
      );
      assert.equal(missing.message, new Saga.OverseerLaunchError({ reason: "E2" }).message);
      for (const decode of [decodeStrictOverseer]) {
        assert.isTrue(
          (yield* decode({
            ...request,
            observerThreadId: "spoof",
          }).pipe(Effect.result))._tag === "Failure",
        );
      }
      assert.deepEqual(yield* decodeLaunch({ ...request, observerThreadId: "spoof" }), {
        title: request.title,
        message: request.message,
      });
      assert.deepEqual(yield* (yield* Store.OverseerStore).incompleteLaunches(), []);
    }).pipe(Effect.provide(harness())),
);

it.effect(
  "the ordinary toolkit ignores unknown arguments while the opt-in toolkit rejects key-only calls",
  () =>
    Effect.gen(function* () {
      yield* setup();
      const ordinary = yield* ProjectToolkit.pipe(
        Effect.provide(ToolAccess.HandlersLayer.layer(Handlers.layer)),
      );
      const withUnknown = { ...request, unknownField: "ignored" };
      const hidden = yield* enabled(
        ordinary
          .handle("t3_thread_launch", withUnknown, undefined, {
            onExcessProperty:
              Tool.getStrictMode(ProjectToolkit.tools.t3_thread_launch) === true
                ? "error"
                : "ignore",
          })
          .pipe(Stream.unwrap, Stream.runHead),
      );
      assert.isFalse(Option.getOrThrow(hidden).isFailure);
      assert.notProperty(Option.getOrThrow(hidden).encodedResult, "linkId");
      const toolkit = yield* ProjectToolkitOverseer.pipe(
        Effect.provide(ToolAccess.HandlersLayer.layer(Handlers.layerOverseer)),
      );
      const call = Effect.fn("test.overseer.callTool")(function* (
        input: typeof OverseerLaunchParameters.Encoded,
      ) {
        return yield* enabled(
          toolkit
            .handle("t3_thread_launch", input, undefined, { onExcessProperty: "error" })
            .pipe(Stream.unwrap, Stream.runHead),
        );
      });
      const keyOnly = Option.getOrThrow(
        yield* call({ title: "Idle", clientRequestId: "unsupported" }),
      );
      assert.isTrue(keyOnly.isFailure);
      assert.include(
        JSON.stringify(keyOnly.encodedResult),
        new Saga.OverseerLaunchError({ reason: "E3" }).message,
      );
      const idle = Option.getOrThrow(yield* call({ title: "Ordinary idle" }));
      assert.isFalse(idle.isFailure);
      assert.notProperty(idle.encodedResult, "linkId");
      const launched = Option.getOrThrow(yield* call(request));
      assert.isFalse(launched.isFailure);
      assert.property(launched.encodedResult, "linkId");
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER reject_accept BEFORE UPDATE OF launch_accepted ON overseer_links BEGIN SELECT RAISE(ABORT,'injected acceptance failure'); END`;
      const unfinished = Option.getOrThrow(
        yield* call({ ...request, clientRequestId: "lost-response" }),
      );
      assert.isTrue(unfinished.isFailure);
      assert.include(
        JSON.stringify(unfinished.encodedResult),
        new Saga.OverseerLaunchError({ reason: "E7" }).message,
      );
      for (const field of ["linkId", "threadId", "projectId"])
        assert.property(unfinished.encodedResult, field);
    }).pipe(Effect.provide(Layer.merge(harness(), NodeCrypto.layer))),
);

it.effect("rejects unsupported observer and target adapters before reserving", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const unsupported = yield* enabled(
      saga.launch({
        ...request,
        modelSelection: { instanceId: ProviderInstanceId.make("cursor"), model: "auto" },
      }),
    ).pipe(Effect.flip);
    assert.equal(unsupported.message, new Saga.OverseerLaunchError({ reason: "E5" }).message);
    assert.deepEqual(yield* (yield* Store.OverseerStore).incompleteLaunches(), []);
  }).pipe(Effect.provide(harness())),
);

it.effect.each(
  (["codex", "claudeAgent"] as const).flatMap((observer) =>
    (["codex", "claudeAgent"] as const).map((target) => ({ observer, target })),
  ),
)("certifies $observer → $target", (pair) =>
  Effect.gen(function* () {
    const model = (instance: string) => ({
      instanceId: ProviderInstanceId.make(instance),
      model: "test-model",
    });
    const saga = yield* setup(model(pair.observer));
    const result = yield* enabled(saga.launch({ ...request, modelSelection: model(pair.target) }), {
      ...invocation,
      thread: { ...invocation.thread!, providerInstanceId: ProviderInstanceId.make(pair.observer) },
    });
    assert.equal(
      result.projection.thread.modelSelection.instanceId,
      ProviderInstanceId.make(pair.target),
    );
    assert.isDefined(result.linkId);
  }).pipe(Effect.provide(harness())),
);

it.effect("rejects an uncertified observer adapter", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const error = yield* enabled(saga.launch(request)).pipe(Effect.flip);
    assert.equal(error.message, new Saga.OverseerLaunchError({ reason: "E5" }).message);
  }).pipe(Effect.provide(harness({ providerDriver: "cursor" }))),
);

it.effect("keeps the link when workspace preparation fails", () =>
  Effect.gen(function* () {
    const failed = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const saga = yield* setup();
      const result = yield* enabled(
        saga.launch({ ...request, workspaceStrategy: { type: "worktree", baseRef: "main" } }),
      );
      yield* Deferred.await(failed);
      const projection = yield* (yield* Threads.ThreadManagementService).getThreadProjection(
        result.threadId,
      );
      assert.equal(projection.runs[0]?.status, "failed");
      assert.equal(
        (yield* (yield* Store.OverseerStore).reservationFor(observer, request.clientRequestId))
          ?.state,
        "active",
      );
    }).pipe(
      Effect.provide(
        harness({
          failPreparation: true,
          milestone: (step) =>
            step === "prepared-run.fail"
              ? Deferred.succeed(failed, undefined).pipe(Effect.asVoid)
              : Effect.void,
        }),
      ),
    );
  }),
);

it.effect(
  "an instantly completing fake effect worker sees the link before the launch returns",
  () =>
    Effect.gen(function* () {
      const runWorker = yield* Deferred.make<Effect.Effect<void>>();
      const completed = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const saga = yield* setup();
        const store = yield* Store.OverseerStore;
        const threads = yield* Threads.ThreadManagementService;
        const sink = yield* EventSink.EventSinkV2;
        const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
          execute: (effect) =>
            Effect.gen(function* () {
              if (effect.threadId === observer || effect.request.type !== "provider-turn.start")
                return;
              const link = (yield* store.linksForEndpoint(effect.threadId))[0];
              assert.isDefined(link);
              assert.equal(link?.observer_thread_id, observer);
              const projection = yield* threads.getThreadProjection(effect.threadId);
              const runId = effect.request.runId;
              const run = projection.runs.find((run) => run.id === runId)!;
              const now = yield* DateTime.now;
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("fake:instant-completion"),
                    type: "run.updated",
                    threadId: effect.threadId,
                    occurredAt: now,
                    payload: { ...run, status: "completed", completedAt: now },
                  },
                ],
              });
              yield* Deferred.succeed(completed, undefined);
            }).pipe(Effect.orDie),
        });
        const worker = Worker.layerWithOptions({ workerId: "instant-fake" }).pipe(
          Layer.provide(executor),
          Layer.provide(Outbox.layer),
        );
        yield* Effect.gen(function* () {
          const service = yield* Worker.OrchestrationEffectWorkerV2;
          yield* Deferred.succeed(runWorker, service.drain().pipe(Effect.orDie, Effect.asVoid));
          const result = yield* enabled(saga.launch(request));
          yield* Deferred.await(completed);
          const projection = yield* threads.getThreadProjection(result.threadId);
          assert.equal(projection.runs[0]?.status, "completed");
        }).pipe(Effect.provide(worker));
      }).pipe(
        Effect.provide(
          harness({
            milestone: (step) =>
              step === "prepared-run.release"
                ? Deferred.await(runWorker).pipe(Effect.flatMap((effect) => effect))
                : step === "prepared-or-scheduled"
                  ? Deferred.await(completed)
                  : Effect.void,
          }),
        ),
      );
    }),
);

it.effect("requires a non-empty initial message only for overseen launches", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    for (const message of [undefined, "", "  "]) {
      const error = yield* enabled(saga.launch({ ...request, message })).pipe(Effect.flip);
      assert.equal(error.message, new Saga.OverseerLaunchError({ reason: "E9" }).message);
    }
    assert.deepEqual(yield* (yield* Store.OverseerStore).incompleteLaunches(), []);
  }).pipe(Effect.provide(harness())),
);

it.effect("unlink during launch leaves independent work but never resurrects observation", () =>
  Effect.gen(function* () {
    const created = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const saga = yield* setup();
      const fiber = yield* enabled(saga.launch(request)).pipe(Effect.forkScoped);
      yield* Deferred.await(created);
      const store = yield* Store.OverseerStore;
      const row = (yield* store.incompleteLaunches())[0]!;
      yield* store.unlink(row.link_id);
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(fiber);
      assert.isUndefined(result.linkId);
      assert.equal(result.projection.messages.length, 1);
      assert.equal(
        (yield* store.reservationFor(observer, request.clientRequestId))?.state,
        "closed",
      );
      assert.isFalse(
        yield* store.record({
          linkId: row.link_id,
          kind: "completion",
          sourceId: "late",
          sourceSequence: 1,
          ready: true,
        }),
      );
      assert.deepEqual(yield* store.linksForEndpoint(result.threadId), []);
      assert.deepEqual(yield* enabled(saga.recover), { resumed: 0, pending: 0 });
    }).pipe(
      Effect.provide(
        harness({
          milestone: (step) =>
            step === "thread.create"
              ? Deferred.succeed(created, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        }),
      ),
    );
  }).pipe(Effect.scoped),
);

it.effect("rechecks mutable target modes at dispatch on retry and recovery", () =>
  Effect.gen(function* () {
    const created = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const saga = yield* setup();
      const threads = yield* Threads.ThreadManagementService;
      yield* threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("owner:restrict"),
        threadId: observer,
        runtimeMode: "approval-required",
      });
      const fiber = yield* enabled(saga.launch(request)).pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(created);
      const store = yield* Store.OverseerStore;
      const row = (yield* store.incompleteLaunches())[0]!;
      yield* threads.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("target:raise"),
        threadId: ThreadId.make(row.target_thread_id),
        runtimeMode: "full-access",
      });
      yield* Deferred.succeed(release, undefined);
      const first = yield* Fiber.join(fiber);
      assert.equal(first._tag, "Failure");
      const retry = yield* enabled(saga.launch(request)).pipe(Effect.flip);
      assert.equal(retry._tag, "OrchestratorMcpFailure");
      assert.deepEqual(yield* enabled(saga.recover), { resumed: 0, pending: 1 });
      const receipt = yield* (yield* Receipts.CommandReceiptStoreV2).getByCommandId(
        CommandId.make(`${row.launch_command_id}:initial-message`),
      );
      assert.isTrue(Option.isNone(receipt));
      assert.equal(
        (yield* threads.getThreadProjection(ThreadId.make(row.target_thread_id))).messages.length,
        0,
      );
    }).pipe(
      Effect.provide(
        harness({
          milestone: (step) =>
            step === "thread.create"
              ? Deferred.succeed(created, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        }),
      ),
    );
  }).pipe(Effect.scoped),
);

it.effect("a reservation failure creates no executable target or initial-message receipt", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER reject_overseer BEFORE INSERT ON overseer_links BEGIN SELECT RAISE(ABORT,'injected reserve failure'); END`;
    const error = yield* enabled(saga.launch(request)).pipe(Effect.flip);
    assert.equal(error.message, new Saga.OverseerLaunchError({ reason: "E8" }).message);
    assert.equal(
      (yield* sql<{
        count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads`)[0]?.count,
      1,
    );
    assert.equal(
      (yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM overseer_links`)[0]?.count,
      0,
    );
  }).pipe(Effect.provide(harness())),
);

it.effect.each(["root", "worktree", "existing_worktree"] as const)(
  "normalizes %s workspace and preserves its binding on retry",
  (strategy) =>
    Effect.gen(function* () {
      const saga = yield* setup();
      const cfg = yield* Config.ServerConfig;
      const workspaceStrategy =
        strategy === "existing_worktree"
          ? { type: strategy, worktreePath: cfg.stateDir }
          : strategy === "worktree"
            ? { type: strategy, baseRef: "main" }
            : { type: strategy };
      const first = yield* enabled(saga.launch({ ...request, workspaceStrategy }));
      const threads = yield* Threads.ThreadManagementService;
      yield* threads.streamStoredEventsFrom({ threadId: first.threadId }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
        ),
        Stream.runHead,
      );
      const settled = yield* threads.getThreadProjection(first.threadId);
      const second = yield* enabled(saga.launch({ ...request, workspaceStrategy }));
      assert.equal(settled.thread.worktreePath, second.projection.thread.worktreePath);
      assert.equal(first.threadId, second.threadId);
    }).pipe(Effect.provide(harness())),
);

it.effect("retains claimed attachments after a lost response and expired pending upload", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const cfg = yield* Config.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const attachment = {
      type: "image" as const,
      id: ChatAttachmentId.make(createPendingAttachmentId()),
      name: "input.png",
      mimeType: "image/png" as const,
      sizeBytes: 4,
    };
    const pending = resolveAttachmentPath({ attachmentsDir: cfg.attachmentsDir, attachment })!;
    yield* fs.writeFile(pending, new Uint8Array([1, 2, 3, 4]));
    const args = { ...request, attachments: [attachment] };
    const a = yield* enabled(saga.launch(args));
    yield* fs.remove(pending);
    const b = yield* enabled(saga.launch(args));
    assert.equal(a.threadId, b.threadId);
    assert.deepEqual(a.projection.messages[0]?.attachments, b.projection.messages[0]?.attachments);
    const owned = b.projection.messages[0]!.attachments[0]!;
    assert.notEqual(owned.id, attachment.id);
    assert.deepEqual(
      yield* fs.readFile(
        resolveAttachmentPath({ attachmentsDir: cfg.attachmentsDir, attachment: owned })!,
      ),
      new Uint8Array([1, 2, 3, 4]),
    );
  }).pipe(Effect.provide(harness())),
);

it.effect.each(["thread.create", "message.dispatch"])(
  "recovers owned uploads at %s after pending expiry through fresh services",
  (step) =>
    Effect.gen(function* () {
      const cfg = yield* Replay.makeReplayServerConfig("reserved-upload");
      const fs = yield* FileSystem.FileSystem;
      yield* Effect.addFinalizer(() =>
        fs.remove(cfg.baseDir, { recursive: true }).pipe(Effect.orDie),
      );
      const snapshot = `${cfg.baseDir}/crash.sqlite`;
      const captured = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      const attachment = {
        type: "image" as const,
        id: ChatAttachmentId.make(createPendingAttachmentId()),
        name: "input.png",
        mimeType: "image/png" as const,
        sizeBytes: 4,
      };
      const pending = resolveAttachmentPath({ attachmentsDir: cfg.attachmentsDir, attachment })!;
      yield* fs.writeFile(pending, new Uint8Array([1, 2, 3, 4]));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const saga = yield* setup();
          const fiber = yield* enabled(saga.launch({ ...request, attachments: [attachment] })).pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(captured);
          yield* fs.remove(pending);
          yield* Deferred.succeed(hold, undefined);
          yield* Fiber.interrupt(fiber);
        }).pipe(
          Effect.provide(
            harness({
              config: cfg,
              milestone: (milestone) =>
                milestone !== step
                  ? Effect.void
                  : Effect.gen(function* () {
                      const sql = yield* SqlClient.SqlClient;
                      yield* sql.unsafe(`VACUUM INTO '${snapshot.replaceAll("'", "''")}'`).pipe(
                        Effect.orDie,
                        Effect.onError((cause) => Deferred.failCause(captured, cause)),
                      );
                      yield* Deferred.succeed(captured, undefined);
                      yield* Deferred.await(hold);
                    }),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const saga = yield* Saga.OverseerLaunchService;
        assert.deepEqual(yield* enabled(saga.recover), { resumed: 1, pending: 0 });
        const row = (yield* (yield* Store.OverseerStore).reservationFor(
          observer,
          request.clientRequestId,
        ))!;
        const projection = yield* (yield* Threads.ThreadManagementService).getThreadProjection(
          ThreadId.make(row.target_thread_id),
        );
        const owned = projection.messages[0]!.attachments[0]!;
        assert.deepEqual(
          yield* fs.readFile(
            resolveAttachmentPath({ attachmentsDir: cfg.attachmentsDir, attachment: owned })!,
          ),
          new Uint8Array([1, 2, 3, 4]),
        );
      }).pipe(Effect.provide(harness({ config: cfg, database: Sqlite.layerFromPath(snapshot) })));
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("invalid uploads do not consume the retry key", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const cfg = yield* Config.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const attachment = {
      type: "image" as const,
      id: ChatAttachmentId.make(createPendingAttachmentId()),
      name: "input.png",
      mimeType: "image/png" as const,
      sizeBytes: 4,
    };
    const pending = resolveAttachmentPath({ attachmentsDir: cfg.attachmentsDir, attachment })!;
    for (const attachments of [
      [attachment],
      [attachment, attachment],
      [{ ...attachment, sizeBytes: 5 }],
    ]) {
      if (attachments.length > 1) yield* fs.writeFile(pending, new Uint8Array([1, 2, 3, 4]));
      yield* enabled(saga.launch({ ...request, attachments })).pipe(Effect.flip);
      assert.deepEqual(yield* (yield* Store.OverseerStore).incompleteLaunches(), []);
    }
    const valid = yield* enabled(saga.launch({ ...request, attachments: [attachment] }));
    assert.isDefined(valid.linkId);
  }).pipe(Effect.provide(harness())),
);

it.effect("does not provision a recorded worktree again after a crash", () =>
  Effect.gen(function* () {
    const cfg = yield* Replay.makeReplayServerConfig("bound-worktree");
    const fs = yield* FileSystem.FileSystem;
    yield* Effect.addFinalizer(() =>
      fs.remove(cfg.baseDir, { recursive: true }).pipe(Effect.orDie),
    );
    const snapshot = `${cfg.baseDir}/crash.sqlite`;
    const captured = yield* Deferred.make<void>();
    const hold = yield* Deferred.make<void>();
    const creates = yield* Ref.make(0);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const saga = yield* setup();
        const fiber = yield* enabled(
          saga.launch({ ...request, workspaceStrategy: { type: "worktree", baseRef: "main" } }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(captured);
        yield* Deferred.succeed(hold, undefined);
        yield* Fiber.interrupt(fiber);
      }).pipe(
        Effect.provide(
          harness({
            config: cfg,
            worktreeCreated: Ref.update(creates, (n) => n + 1),
            milestone: (step) =>
              step === "prepared-or-scheduled"
                ? Deferred.await(hold)
                : step !== "thread.metadata.update"
                  ? Effect.void
                  : Effect.gen(function* () {
                      const sql = yield* SqlClient.SqlClient;
                      yield* sql.unsafe(`VACUUM INTO '${snapshot.replaceAll("'", "''")}'`).pipe(
                        Effect.orDie,
                        Effect.onError((cause) => Deferred.failCause(captured, cause)),
                      );
                      yield* Deferred.succeed(captured, undefined);
                      yield* Deferred.await(hold);
                    }),
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const saga = yield* Saga.OverseerLaunchService;
      assert.deepEqual(yield* enabled(saga.recover), { resumed: 1, pending: 0 });
      const row = (yield* (yield* Store.OverseerStore).reservationFor(
        observer,
        request.clientRequestId,
      ))!;
      const threads = yield* Threads.ThreadManagementService;
      const projection = yield* threads.getThreadProjection(ThreadId.make(row.target_thread_id));
      assert.equal(projection.thread.worktreePath, `${cfg.stateDir}/worktree`);
      assert.equal(yield* Ref.get(creates), 1);
    }).pipe(
      Effect.provide(
        harness({
          config: cfg,
          database: Sqlite.layerFromPath(snapshot),
          worktreeCreated: Ref.update(creates, (n) => n + 1),
        }),
      ),
    );
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

it.effect("freezes one scratch folder before creation and reuses it", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const a = yield* enabled(saga.launch({ ...request, scratch: true }));
    const b = yield* enabled(saga.launch({ ...request, scratch: true }));
    assert.equal(a.projection.thread.projectId, projectB);
    assert.equal(a.projection.thread.worktreePath, b.projection.thread.worktreePath);
    assert.isTrue(a.projection.thread.worktreePath?.endsWith(a.threadId));
    assert.isTrue(yield* (yield* FileSystem.FileSystem).exists(a.projection.thread.worktreePath!));
  }).pipe(Effect.provide(harness({ scratch: true }))),
);

it.effect.each([
  "before-reservation",
  "reservation",
  "thread.create",
  "message.dispatch",
  "prepared-or-scheduled",
  "prepared-run.release",
  "startup-after-message",
])("recovers a pre-teardown snapshot at %s using fresh services and exact receipts", (step) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const folder = yield* fs.makeTempDirectoryScoped({ prefix: "overseer-crash-" });
    const snapshot = `${folder}/snapshot.sqlite`;
    const captured = yield* Deferred.make<void>();
    const hold = yield* Deferred.make<void>();
    const layer = harness({
      milestone: (milestone) =>
        milestone !== (step === "startup-after-message" ? "message.dispatch" : step)
          ? Effect.void
          : Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql.unsafe(`VACUUM INTO '${snapshot.replaceAll("'", "''")}'`).pipe(
                Effect.orDie,
                Effect.onError((cause) => Deferred.failCause(captured, cause)),
              );
              yield* Deferred.succeed(captured, undefined);
              yield* Deferred.await(hold);
            }),
    });
    yield* Effect.scoped(
      Effect.gen(function* () {
        const saga = yield* setup();
        const fiber = yield* enabled(saga.launch(request)).pipe(Effect.forkScoped);
        yield* Deferred.await(captured);
        yield* Deferred.succeed(hold, undefined);
        yield* Fiber.interrupt(fiber);
      }).pipe(Effect.provide(layer)),
    );
    // The copy precedes interruption/finalizers; they cannot manufacture a recoverable state.
    yield* Effect.gen(function* () {
      const saga = yield* Saga.OverseerLaunchService;
      const store = yield* Store.OverseerStore;
      const before = yield* store.reservationFor(observer, request.clientRequestId);
      if (step === "startup-after-message") {
        const runtimeRecovery = yield* Recovery.ProviderRuntimeRecoveryService.pipe(
          Effect.provide(
            Recovery.layer.pipe(
              Layer.provide(Layer.mergeAll(Projections.layer, Outbox.layer, IdAllocator.layer)),
            ),
          ),
        );
        yield* runtimeRecovery.recover;
      }
      const recovered = yield* enabled(saga.recover);
      const row = yield* store.reservationFor(observer, request.clientRequestId);
      const sql = yield* SqlClient.SqlClient;
      if (step === "before-reservation") {
        assert.equal(recovered.resumed, 0);
        assert.isUndefined(row);
        assert.equal(
          (yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads`)[0]?.count,
          1,
        );
      } else {
        assert.equal(recovered.resumed, before?.launch_accepted === 1 ? 0 : 1);
        assert.equal(recovered.pending, 0);
        assert.equal(row?.launch_accepted, 1);
        const receipts = yield* Receipts.CommandReceiptStoreV2;
        for (const [id, type] of [
          [row!.launch_command_id, "thread.create"],
          [`${row!.launch_command_id}:initial-message`, "message.dispatch"],
        ]) {
          const receipt = yield* receipts.getByCommandId(CommandId.make(id!));
          assert.isTrue(Option.isSome(receipt));
          if (Option.isSome(receipt)) {
            assert.equal(receipt.value.status, "accepted");
            assert.equal(receipt.value.threadId, row!.target_thread_id);
            assert.equal(receipt.value.commandType, type);
          }
        }
        const projection = yield* (yield* Threads.ThreadManagementService).getThreadProjection(
          ThreadId.make(row!.target_thread_id),
        );
        assert.equal(projection.messages.length, 1);
        assert.equal(projection.messages[0]?.id, row!.launch_message_id);
        assert.equal(row?.state, "active");
        if (step === "startup-after-message") {
          assert.equal(projection.runs.length, 1);
          assert.equal(projection.runs[0]?.status, "cancelled");
          assert.equal(
            (yield* sql<{
              count: number;
            }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads`)[0]?.count,
            2,
          );
          assert.equal(
            (yield* store.linksForEndpoint(ThreadId.make(row!.target_thread_id))).length,
            1,
          );
        }
        assert.deepEqual(yield* enabled(saga.recover), { resumed: 0, pending: 0 });
      }
    }).pipe(Effect.provide(harness({ database: Sqlite.layerFromPath(snapshot) })));
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);

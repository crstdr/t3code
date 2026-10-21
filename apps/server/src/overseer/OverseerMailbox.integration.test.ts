import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  EventId,
  NodeId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderTurnId,
  RuntimeRequestId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Store from "./OverseerStore.ts";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Sessions from "../orchestration-v2/ProviderSessionManager.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Transport from "./OverseerTransport.ts";
import * as Native from "./OverseerNative.ts";
import * as TurnStart from "../orchestration-v2/ProviderTurnStartService.ts";
import { enabled, harness, observer, request, setup } from "./OverseerLaunch.testkit.ts";
import { wakeText } from "./OverseerText.ts";

it.effect.each([
  { driver: "codex", supported: true, interrupts: false, steer: true },
  { driver: "codex", supported: false, interrupts: false, steer: false },
  { driver: "codex", supported: true, interrupts: true, steer: false },
  { driver: "claudeAgent", supported: true, interrupts: false, steer: false },
])(
  "routes $driver / steering=$supported / interrupts=$interrupts without ever restarting a turn",
  (policy) =>
    Effect.gen(function* () {
      const { core, sink, question, sync } = yield* fixture();
      const projection = yield* core.getThreadRecords(observer, ["runs", "providerThreads"]);
      const active = projection.runs[0]!;
      const providerThread = projection.providerThreads.find(
        (row) => row.id === active.providerThreadId,
      )!;
      const at = yield* DateTime.now;
      const id = ProviderSessionId.make("live-observer");
      const driver = ProviderDriverKind.make(policy.driver);
      const providerSession = {
        id,
        driver,
        providerInstanceId: active.providerInstanceId,
        status: "running" as const,
        cwd: process.cwd(),
        model: active.modelSelection.model,
        createdAt: at,
        updatedAt: at,
        lastError: null,
        capabilities: {
          ...CodexProviderCapabilitiesV2,
          turns: {
            ...CodexProviderCapabilitiesV2.turns,
            supportsActiveSteering: policy.supported,
            activeSteeringInterruptsTools: policy.interrupts,
          },
        },
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("live-run"),
            type: "run.updated",
            threadId: observer,
            occurredAt: at,
            payload: { ...active, status: "running", startedAt: at },
          },
          {
            id: EventId.make("live-thread"),
            type: "provider-thread.updated",
            threadId: observer,
            occurredAt: at,
            payload: { ...providerThread, driver, providerSessionId: id, status: "active" },
          },
          {
            id: EventId.make("live-session"),
            type: "provider-session.attached",
            threadId: observer,
            occurredAt: at,
            payload: providerSession,
          },
          {
            id: EventId.make("live-turn"),
            type: "provider-turn.updated",
            threadId: observer,
            occurredAt: at,
            payload: {
              id: ProviderTurnId.make("live-turn"),
              providerThreadId: providerThread.id,
              nodeId: active.rootNodeId!,
              runAttemptId: active.activeAttemptId,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: at,
              completedAt: null,
            },
          },
        ],
      });
      const unused = () => Effect.die("The routing fixture must not execute provider operations");
      const runtime = {
        instanceId: active.providerInstanceId,
        driver,
        providerSessionId: id,
        providerSession,
        events: Stream.empty,
        ensureThread: unused,
        resumeThread: unused,
        startTurn: unused,
        steerTurn: unused,
        interruptTurn: unused,
        respondToRuntimeRequest: unused,
        readThreadSnapshot: unused,
        rollbackThread: unused,
        forkThread: unused,
      } satisfies ProviderAdapterV2SessionRuntime;
      const sessions = yield* Sessions.ProviderSessionManagerV2;
      const spy = vi.spyOn(sessions, "get").mockReturnValue(Effect.succeed(Option.some(runtime)));
      yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
      yield* question("routing-question");
      yield* sync();
      const result = yield* core.getThreadRecords(observer, ["runs", "messages"]);
      const wake = result.messages.find((row) => row.id.startsWith("message:overseer:"))!;
      assert.isDefined(wake);
      assert.equal(wake.notification?.summary, wakeText.W2);
      const carrying = result.runs.find((row) => row.id === wake.runId)!;
      assert.equal(carrying.id === active.id, policy.steer);
      assert.equal(carrying.status, policy.steer ? "running" : "queued");
      const transport = yield* Transport.OverseerTransport.pipe(Effect.provide(Transport.layer));
      const row = (yield* transport.forMessage(wake.id))!;
      const effects = yield* (yield* Outbox.EffectOutboxV2.pipe(
        Effect.provide(Outbox.layer),
      )).listByCommandId(CommandId.make(row.command_id));
      assert.equal(
        effects.some((effect) => effect.request.type === "provider-turn.steer"),
        policy.steer,
      );
      assert.isFalse(
        effects.some(
          (effect) =>
            effect.request.type === "provider-turn.restart" ||
            effect.request.type === "provider-turn.interrupt",
        ),
      );
      if (policy.steer) {
        yield* sink.write({
          events: [
            {
              id: EventId.make("steer-target-ended"),
              type: "run.updated",
              threadId: observer,
              occurredAt: at,
              payload: { ...active, status: "completed", completedAt: at },
            },
          ],
        });
        assert.isUndefined(yield* transport.open(observer));
        yield* question("after-target-ended");
        yield* sync();
        const replacement = (yield* transport.open(observer))!;
        assert.notEqual(replacement.message_id, row.message_id);
      }
    }).pipe(enabled, Effect.provide(harness())),
);

const fixture = Effect.fn("test.overseerMailbox.integration")(function* () {
  const saga = yield* setup();
  const launched = yield* saga.launch(request);
  const core = yield* Orchestrator.OrchestratorV2;
  const store = yield* Store.OverseerStore;
  const sink = yield* Sink.EventSinkV2;
  const work = launched.projection.runs[0]!;
  const question = Effect.fn("test.overseerMailbox.question")(function* (id: string) {
    const now = yield* DateTime.now;
    const payload = {
      id: RuntimeRequestId.make(id),
      nodeId: NodeId.make(`node:${id}`),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: now,
      resolvedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make(`question:${id}`),
          type: "runtime-request.updated",
          threadId: launched.threadId,
          runId: work.id,
          occurredAt: now,
          payload,
        },
      ],
    });
    return payload;
  });
  const sync = Effect.fn("test.overseerMailbox.privateSync")(function* () {
    const box = (yield* store.mailbox(observer))!;
    assert.isDefined(core.syncOverseer);
    yield* core.syncOverseer!({
      observerThreadId: observer,
      generation: box.flush_id!.slice("effect:overseer-flush:".length),
    });
  });
  return { core, store, sink, launched, work, question, sync };
});

it.effect(
  "privately refreshes queued text and presentation in place, including mixed-batch withdrawal",
  () =>
    Effect.gen(function* () {
      const { core, store, sink, launched, work, question, sync } = yield* fixture();
      const first = yield* question("first");
      yield* sync();
      const initial = yield* core.getThreadRecords(observer, ["runs", "messages"]);
      const wake = initial.messages.find((message) => message.id.startsWith("message:overseer:"))!;
      assert.isDefined(wake);
      const queued = initial.runs.find((run) => run.userMessageId === wake.id)!;
      assert.equal(queued.status, "queued");
      assert.equal(wake.notification?.source.kind, "background_task");
      assert.equal(wake.notification?.summary, wakeText.W2);
      assert.include(wake.text, "request first: user-input question pending.");
      const at = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("queued-presentation"),
            type: "turn-item.updated",
            threadId: observer,
            runId: queued.id,
            occurredAt: at,
            payload: {
              id: TurnItemId.make("queued-presentation"),
              type: "user_message",
              threadId: observer,
              runId: queued.id,
              nodeId: queued.rootNodeId,
              providerThreadId: queued.providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: queued.ordinal * 100,
              status: "completed",
              title: null,
              startedAt: at,
              completedAt: at,
              updatedAt: at,
              messageId: wake.id,
              inputIntent: "queued_turn",
              text: wake.text,
              attachments: [],
              createdBy: "agent",
              creationSource: "server",
            },
          },
        ],
      });
      yield* core
        .dispatch({
          type: "queued-run.edit",
          threadId: observer,
          runId: queued.id,
          commandId: CommandId.make("public-edit"),
          text: "Not allowed",
        })
        .pipe(Effect.flip);
      yield* question("second");
      yield* sync();
      let refreshed = yield* core.getThreadRecords(observer, ["runs", "messages", "turnItems"]);
      assert.equal(
        refreshed.messages.filter((row) => row.id.startsWith("message:overseer:")).length,
        1,
      );
      let text = refreshed.messages.find((row) => row.id === wake.id)!.text;
      assert.include(text, "request first:");
      assert.include(text, "request second:");
      assert.equal(
        refreshed.turnItems.find((row) => row.id === "queued-presentation")?.type,
        "user_message",
      );
      const presentation = refreshed.turnItems.find((row) => row.id === "queued-presentation");
      if (presentation?.type === "user_message") assert.equal(presentation.text, text);
      yield* sink.write({
        events: [
          {
            id: EventId.make("resolve-first"),
            type: "runtime-request.updated",
            threadId: launched.threadId,
            runId: work.id,
            occurredAt: at,
            payload: { ...first, status: "resolved", resolvedAt: at },
          },
        ],
      });
      yield* sync();
      refreshed = yield* core.getThreadRecords(observer, ["runs", "messages", "turnItems"]);
      text = refreshed.messages.find((row) => row.id === wake.id)!.text;
      assert.notInclude(text, "request first:");
      assert.include(text, "request second:");
      assert.equal(
        refreshed.runs.find((row) => row.id === queued.id)!.queuePosition,
        queued.queuePosition,
      );
      const paired = refreshed.turnItems.find((row) => row.id === "queued-presentation");
      if (paired?.type === "user_message") assert.equal(paired.text, text);
      else assert.fail("missing paired presentation");
      yield* sink.write({
        events: [
          {
            id: EventId.make("start-wake"),
            type: "run.updated",
            threadId: observer,
            occurredAt: at,
            payload: { ...queued, status: "starting" },
          },
        ],
      });
      const transport = yield* Transport.OverseerTransport.pipe(Effect.provide(Transport.layer));
      const frozen = (yield* transport.forMessage(wake.id))!;
      assert.equal(frozen.phase, "frozen");
      yield* question("third");
      yield* sync();
      assert.deepEqual(
        (yield* store.batchEntries(observer, frozen.batch_number)).map((row) => row.source_id),
        ["second"],
      );
      assert.equal(
        (yield* core.getThreadRecords(observer, ["messages"])).messages.find(
          (row) => row.id === wake.id,
        )!.text,
        text,
      );
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "the real start service publishes its live binding before native admission and receipt",
  () =>
    Effect.gen(function* () {
      const { core, sink, question, sync } = yield* fixture();
      const old = (yield* core.getThreadRecords(observer, ["runs"])).runs[0]!;
      const at = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("before-native-start"),
            type: "run.updated",
            threadId: observer,
            occurredAt: at,
            payload: { ...old, status: "completed", completedAt: at },
          },
        ],
      });
      yield* question("native-start");
      yield* sync();
      const projection = yield* core.getThreadRecords(observer, ["runs", "messages"]);
      const wake = projection.messages.find((message) =>
        message.id.startsWith("message:overseer:"),
      )!;
      const run = projection.runs.find((run) => run.id === wake.runId)!;
      const sessions = yield* Sessions.ProviderSessionManagerV2;
      let enqueues = 0;
      const unused = () => Effect.die("unexpected provider operation");
      const spy = vi.spyOn(sessions, "open").mockImplementation((input) =>
        Effect.succeed({
          instanceId: run.providerInstanceId,
          driver: ProviderDriverKind.make("codex"),
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver: ProviderDriverKind.make("codex"),
            providerInstanceId: run.providerInstanceId,
            status: "running",
            cwd: process.cwd(),
            model: run.modelSelection.model,
            createdAt: at,
            updatedAt: at,
            lastError: null,
            capabilities: CodexProviderCapabilitiesV2,
          },
          events: Stream.empty,
          ensureThread: (input) =>
            Effect.succeed({ ...input.existingProviderThread!, status: "active" }),
          resumeThread: unused,
          startTurn: () =>
            Effect.gen(function* () {
              const observation = yield* Native.Observer;
              assert.isDefined(observation);
              assert.isTrue(
                Option.isSome(
                  yield* observation!.handoff(undefined, (release) =>
                    Effect.sync(() => {
                      enqueues++;
                    }).pipe(Effect.andThen(release)),
                  ),
                ),
              );
              yield* observation!.received("codex_response");
            }).pipe(Effect.orDie),
          steerTurn: unused,
          interruptTurn: unused,
          respondToRuntimeRequest: unused,
          readThreadSnapshot: unused,
          rollbackThread: unused,
          forkThread: unused,
        } satisfies ProviderAdapterV2SessionRuntime),
      );
      yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
      yield* (yield* TurnStart.ProviderTurnStartServiceV2).start({
        threadId: observer,
        runId: run.id,
      });
      assert.equal(enqueues, 1);
      const transport = yield* Transport.OverseerTransport.pipe(Effect.provide(Transport.layer));
      assert.equal((yield* transport.forMessage(wake.id))!.phase, "received");
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "admits an idle observer without requiring an active run, and Stop with no events still fences it",
  () =>
    Effect.gen(function* () {
      const { core, sink, store, question, sync } = yield* fixture();
      const before = yield* core.getThreadRecords(observer, ["runs"]);
      const old = before.runs.find((run) => run.status === "starting")!;
      const at = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("observer-idle"),
            type: "run.updated",
            threadId: observer,
            occurredAt: at,
            payload: { ...old, status: "completed", completedAt: at },
          },
        ],
      });
      yield* question("idle-question");
      yield* sync();
      const projection = yield* core.getThreadRecords(observer, ["runs", "messages"]);
      const wake = projection.messages.find((row) => row.id.startsWith("message:overseer:"))!;
      assert.isDefined(wake);
      assert.equal(
        projection.runs.find((run) => run.userMessageId === wake.id)?.status,
        "starting",
      );
      const first = yield* sink.commitCommand({
        commandId: CommandId.make("eventless-stop"),
        threadId: observer,
        commandType: "thread.stop",
        acceptedAt: at,
        events: [],
        effects: [],
      });
      const epoch = (yield* store.mailbox(observer))!.epoch;
      const replay = yield* sink.commitCommand({
        commandId: CommandId.make("eventless-stop"),
        threadId: observer,
        commandType: "thread.stop",
        acceptedAt: at,
        events: [],
        effects: [],
      });
      assert.isTrue(first.committed);
      assert.isFalse(replay.committed);
      assert.equal((yield* store.mailbox(observer))!.hold_reason, "stop");
      assert.equal((yield* store.mailbox(observer))!.epoch, epoch);
    }).pipe(enabled, Effect.provide(harness())),
);

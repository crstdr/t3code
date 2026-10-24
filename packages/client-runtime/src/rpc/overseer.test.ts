import { it, expect } from "@effect/vitest";
import {
  EnvironmentId,
  OversightError,
  ThreadId,
  WS_METHODS,
  type ServerConfig,
  type OversightWatchFrame,
  type OversightWatchInput,
  type OversightUnlinkInput,
  type OversightConfigureInput,
  type OversightPreferences,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import type { WsRpcProtocolClient } from "./protocol.ts";
import type { RpcSession } from "./session.ts";
import { RpcPermissionGuard, getInitialServerConfig } from "./client.ts";
import * as Overseer from "./overseer.ts";

const PAGE: OversightWatchFrame = { revision: 1, links: [] };
const PREFERENCES: OversightPreferences = {
  autoWake: true,
  cadenceEnabled: false,
  cadenceMinutes: 5,
  idleEnabled: false,
  idleMinutes: 30,
};

const makeHarness = Effect.fn("TestOverseer.makeHarness")(function* (
  watchFailure?: OversightError,
) {
  const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none());
  const state = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(Option.none());
  const configReady = yield* Deferred.make<ServerConfig>();
  const calls: string[] = [];
  const requests: Array<{ readonly tag: string; readonly input: unknown }> = [];
  const configRequested = yield* Deferred.make<void>();
  const client = {
    [WS_METHODS.oversightWatch]: (input: OversightWatchInput) => {
      calls.push("watch");
      requests.push({ tag: WS_METHODS.oversightWatch, input });
      return watchFailure ? Stream.fail(watchFailure) : Stream.make(PAGE);
    },
    [WS_METHODS.oversightUnlink]: (input: OversightUnlinkInput) =>
      Effect.sync(() => {
        calls.push("unlink");
        requests.push({ tag: WS_METHODS.oversightUnlink, input });
        return { linkId: "link", closed: true };
      }),
    [WS_METHODS.oversightConfigure]: (input: OversightConfigureInput) =>
      Effect.sync(() => {
        calls.push("configure");
        requests.push({ tag: WS_METHODS.oversightConfigure, input });
        return { ...PREFERENCES, ...input.preferences };
      }),
  } as unknown as WsRpcProtocolClient;
  const initialConfig = Deferred.succeed(configRequested, undefined).pipe(
    Effect.andThen(Deferred.await(configReady)),
  );
  const session: RpcSession = {
    client,
    initialConfig,
    subscribeServerConfig: () =>
      Stream.fromEffect(initialConfig).pipe(
        Stream.map((config) => ({ version: 1 as const, type: "snapshot" as const, config })),
        Stream.concat(Stream.never),
      ),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  const supervisor = EnvironmentSupervisor.of({
    target: new PrimaryConnectionTarget({
      environmentId: EnvironmentId.make("environment-1"),
      label: "Test environment",
      httpBaseUrl: "https://environment.example.test",
      wsBaseUrl: "wss://environment.example.test",
    }),
    state,
    session: activeSession,
    prepared,
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.die("The facade must wait for readiness, not retry the connection."),
  });
  const publishSession = SubscriptionRef.set(activeSession, Option.some(session));
  const ready = (capability: boolean | undefined) =>
    Effect.gen(function* () {
      // Only the capability is consumed; the real RPC helpers do not decode this fixture.
      const config = {
        environment: {
          capabilities: capability === undefined ? {} : { threadOversight: capability },
        },
      } as ServerConfig;
      yield* Deferred.succeed(configReady, config);
    });
  const connect = (capability: boolean | undefined) =>
    ready(capability).pipe(Effect.andThen(publishSession));
  return { supervisor, calls, requests, connect, publishSession, ready, configRequested };
});

it.effect("establishes watch after cold-load session readiness without a manual retry", () =>
  Effect.gen(function* () {
    const { supervisor, calls, connect } = yield* makeHarness();
    const run = Effect.gen(function* () {
      expect(yield* getInitialServerConfig().pipe(Effect.flip)).toMatchObject({
        _tag: "EnvironmentRpcUnavailableError",
      });
      const watching = yield* Overseer.watch({ threadId: ThreadId.make("thread") }).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      expect(calls).toEqual([]);
      yield* connect(true);
      expect(yield* Fiber.join(watching)).toEqual([PAGE]);
      expect(calls).toEqual(["watch"]);
    });
    yield* run.pipe(Effect.provideService(EnvironmentSupervisor, supervisor));
  }),
);

it.effect("waits for the connected config snapshot before establishing watch", () =>
  Effect.gen(function* () {
    const { supervisor, calls, publishSession, ready, configRequested } = yield* makeHarness();
    yield* publishSession;
    yield* Effect.gen(function* () {
      const watching = yield* Overseer.watch({ threadId: ThreadId.make("thread") }).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(configRequested);
      expect(calls).toEqual([]);
      yield* ready(true);
      expect(yield* Fiber.join(watching)).toEqual([PAGE]);
      expect(calls).toEqual(["watch"]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, supervisor));
  }),
);

it.effect("still refuses an unguarded unlink after waiting for session readiness", () =>
  Effect.gen(function* () {
    const { supervisor, calls, connect } = yield* makeHarness();
    yield* Effect.gen(function* () {
      const unlinking = yield* Overseer.unlink({ linkId: "link" }).pipe(
        Effect.flip,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* connect(true);
      expect(yield* Fiber.join(unlinking)).toMatchObject({
        _tag: "EnvironmentAuthorizationError",
        requiredScope: "orchestration:operate",
      });
      expect(calls).toEqual([]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, supervisor));
  }),
);

it.effect.each([undefined, false])(
  "refuses watch RPCs when a replacement session has threadOversight=%s",
  (capability) =>
    Effect.gen(function* () {
      const first = yield* makeHarness();
      const next = yield* makeHarness(new OversightError({ reason: "unsupported" }));
      yield* first.connect(true);
      yield* next.connect(capability);
      const firstFrame = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const watching = yield* Overseer.watch({ threadId: ThreadId.make("thread") }).pipe(
          Stream.tap(() => Deferred.succeed(firstFrame, undefined)),
          Stream.runDrain,
          Effect.flip,
          Effect.forkChild,
        );
        yield* Deferred.await(firstFrame);
        yield* SubscriptionRef.set(
          first.supervisor.session,
          yield* SubscriptionRef.get(next.supervisor.session),
        );
        expect(yield* Fiber.join(watching)).toMatchObject({ reason: "unsupported" });
        expect(first.calls).toEqual(["watch"]);
        expect(next.requests).toEqual([]);
      }).pipe(Effect.provideService(EnvironmentSupervisor, first.supervisor));
    }),
);

it.effect("reestablishes watch on a supported replacement session", () =>
  Effect.gen(function* () {
    const first = yield* makeHarness();
    const next = yield* makeHarness();
    yield* first.connect(true);
    yield* next.connect(true);
    const firstFrame = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const watching = yield* Overseer.watch({ threadId: ThreadId.make("thread") }).pipe(
        Stream.tap(() => Deferred.succeed(firstFrame, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstFrame);
      yield* SubscriptionRef.set(first.supervisor.session, Option.none());
      yield* SubscriptionRef.set(
        first.supervisor.session,
        yield* SubscriptionRef.get(next.supervisor.session),
      );
      expect(yield* Fiber.join(watching)).toEqual([PAGE, PAGE]);
      expect(first.calls).toEqual(["watch"]);
      expect(next.calls).toEqual(["watch"]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, first.supervisor));
  }),
);

it.effect("an old server with no capability is never sent a feature RPC", () =>
  Effect.gen(function* () {
    const { supervisor, calls, connect } = yield* makeHarness();
    yield* connect(undefined);
    yield* Effect.gen(function* () {
      expect(
        yield* Stream.runDrain(Overseer.watch({ threadId: ThreadId.make("thread") })).pipe(
          Effect.flip,
        ),
      ).toMatchObject({ _tag: "OversightError", reason: "unsupported" });
      expect(yield* Overseer.unlink({ linkId: "link" }).pipe(Effect.flip)).toMatchObject({
        _tag: "OversightError",
        reason: "unsupported",
      });
      expect(
        yield* Overseer.configure({
          threadId: ThreadId.make("thread"),
          preferences: { idleEnabled: false },
        }).pipe(Effect.flip),
      ).toMatchObject({
        _tag: "OversightError",
        reason: "unsupported",
      });
      expect(calls).toEqual([]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, supervisor));
  }),
);

it.effect("waits for session readiness before guarded unlink", () =>
  Effect.gen(function* () {
    const { supervisor, calls, connect } = yield* makeHarness();
    yield* Effect.gen(function* () {
      const unlinking = yield* Overseer.unlink({ linkId: "link" }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      expect(calls).toEqual([]);
      yield* connect(true);
      expect(yield* Fiber.join(unlinking)).toEqual({ linkId: "link", closed: true });
      expect(calls).toEqual(["unlink"]);
    }).pipe(
      Effect.provideService(EnvironmentSupervisor, supervisor),
      Effect.provideService(RpcPermissionGuard, { authorize: () => Effect.void }),
    );
  }),
);

it.effect("supported servers get the exact feature tags and payloads after readiness", () =>
  Effect.gen(function* () {
    const { supervisor, requests, connect } = yield* makeHarness();
    yield* connect(true);
    const threadId = ThreadId.make("thread");
    const preferences = { idleEnabled: false };
    yield* Effect.gen(function* () {
      yield* Overseer.watch({ threadId }).pipe(Stream.take(1), Stream.runDrain);
      yield* Overseer.unlink({ linkId: "link" });
      expect(yield* Overseer.configure({ threadId, preferences })).toEqual(PREFERENCES);
      expect(requests).toEqual([
        { tag: WS_METHODS.oversightWatch, input: { threadId } },
        { tag: WS_METHODS.oversightUnlink, input: { linkId: "link" } },
        { tag: WS_METHODS.oversightConfigure, input: { threadId, preferences } },
      ]);
    }).pipe(
      Effect.provideService(EnvironmentSupervisor, supervisor),
      Effect.provideService(RpcPermissionGuard, { authorize: () => Effect.void }),
    );
  }),
);

it.effect("configure waits for the session and its first config snapshot", () =>
  Effect.gen(function* () {
    const { supervisor, calls, publishSession, ready, configRequested } = yield* makeHarness();
    yield* Effect.gen(function* () {
      const configuring = yield* Overseer.configure({
        threadId: ThreadId.make("thread"),
        preferences: { idleEnabled: true },
      }).pipe(Effect.forkChild({ startImmediately: true }));
      expect(calls).toEqual([]);
      yield* publishSession;
      yield* Deferred.await(configRequested);
      expect(calls).toEqual([]);
      yield* ready(true);
      expect(yield* Fiber.join(configuring)).toEqual({ ...PREFERENCES, idleEnabled: true });
      expect(calls).toEqual(["configure"]);
    }).pipe(
      Effect.provideService(EnvironmentSupervisor, supervisor),
      Effect.provideService(RpcPermissionGuard, { authorize: () => Effect.void }),
    );
  }),
);

it.effect("still refuses an unguarded configure after waiting for session readiness", () =>
  Effect.gen(function* () {
    const { supervisor, calls, connect } = yield* makeHarness();
    yield* Effect.gen(function* () {
      const configuring = yield* Overseer.configure({
        threadId: ThreadId.make("thread"),
        preferences: { idleEnabled: true },
      }).pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      expect(calls).toEqual([]);
      yield* connect(true);
      expect(yield* Fiber.join(configuring)).toMatchObject({
        _tag: "EnvironmentAuthorizationError",
        requiredScope: "orchestration:operate",
      });
      expect(calls).toEqual([]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, supervisor));
  }),
);

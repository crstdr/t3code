import { it, expect } from "@effect/vitest";
import {
  EnvironmentId,
  ThreadId,
  WS_METHODS,
  type ServerConfig,
  type OversightWatchFrame,
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

const makeHarness = Effect.fn("TestOverseer.makeHarness")(function* () {
  const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none());
  const state = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
  const prepared = yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(Option.none());
  const configReady = yield* Deferred.make<ServerConfig>();
  const calls: string[] = [];
  const configSubscribed = yield* Deferred.make<void>();
  const client = {
    [WS_METHODS.oversightWatch]: () => {
      calls.push("watch");
      return Stream.make(PAGE);
    },
    [WS_METHODS.oversightUnlink]: () =>
      Effect.sync(() => {
        calls.push("unlink");
        return { linkId: "link", closed: true };
      }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession = {
    client,
    initialConfig: Deferred.await(configReady),
    subscribeServerConfig: () =>
      Stream.fromEffect(
        Deferred.succeed(configSubscribed, undefined).pipe(
          Effect.andThen(Deferred.await(configReady)),
        ),
      ).pipe(
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
  return { supervisor, calls, connect, publishSession, ready, configSubscribed };
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
    const { supervisor, calls, publishSession, ready, configSubscribed } = yield* makeHarness();
    yield* publishSession;
    yield* Effect.gen(function* () {
      const watching = yield* Overseer.watch({ threadId: ThreadId.make("thread") }).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(configSubscribed);
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

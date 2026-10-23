import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, ThreadId, EnvironmentId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Statement from "effect/sql/Statement";
import * as McpInvocation from "../mcp/McpInvocationContext.ts";
import { request, invocation } from "./OverseerLaunch.testkit.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Service from "./OverseerService.ts";
import * as Store from "./OverseerStore.ts";
import * as Gate from "./OverseerGate.ts";
import * as Feature from "./OverseerFeature.ts";
import { harness, setup, enabled, observer } from "./OverseerLaunch.testkit.ts";

const reserve = Effect.fn("test.reserve")(function* (id = "private-link") {
  const store = yield* Store.OverseerStore;
  return yield* store.reserve({
    linkId: id,
    observerThreadId: observer,
    observerProjectId: ProjectId.make("project:a"),
    targetThreadId: ThreadId.make(`target:${id}`),
    targetProjectId: ProjectId.make("project:b"),
    clientRequestId: id,
    fingerprint: id,
    launchCommandId: CommandId.make(`launch:${id}`),
    launchMessageId: MessageId.make(`message:${id}`),
    launchInputJson: JSON.stringify({
      title: "Reserved title",
      initialMessage: { text: "private-task" },
    }),
  });
});

it.effect(
  "pages are bounded, cursors are endpoint scoped, and repeated reads do not write or acknowledge",
  () =>
    Effect.gen(function* () {
      yield* setup();
      const store = yield* Store.OverseerStore;
      const service = yield* Service.OverseerService;
      const sql = yield* SqlClient.SqlClient;
      for (let index = 0; index < 25; index++)
        yield* reserve(`link:${String(index).padStart(2, "0")}`);
      yield* store.record({
        linkId: "link:00",
        kind: "completion",
        sourceId: "root",
        sourceSequence: 1,
        ready: true,
      });
      const before = yield* sql`SELECT total_changes() AS changes`;
      const queries: Array<readonly [string, ReadonlyArray<unknown>]> = [];
      const capture: Statement.Transformer = (statement) => {
        queries.push(statement.compile());
        return Effect.succeed(statement);
      };
      const first = yield* service
        .list({ threadId: observer })
        .pipe(Effect.provideService(Statement.CurrentTransformer, capture));
      assert.equal(first.links.length, 20);
      assert.isDefined(first.nextCursor);
      const second = yield* service.list({ threadId: observer, cursor: first.nextCursor! });
      assert.equal(second.links.length, 5);
      assert.isUndefined(second.nextCursor);
      assert.equal(new Set([...first.links, ...second.links].map((link) => link.linkId)).size, 25);
      assert.notEqual(first.links[0]!.linkId, "link:00");
      assert.equal(first.links[0]!.workerStatus, "reserved");
      assert.equal(first.links[0]!.title, "Reserved title");
      assert.notInclude(JSON.stringify(first), "private-task");
      yield* service.list({ threadId: observer });
      assert.deepEqual(yield* sql`SELECT total_changes() AS changes`, before);
      assert.equal(queries.filter(([query]) => query.trimStart().startsWith("SELECT")).length, 22);
      for (const [query, parameters] of queries) {
        if (!query.trimStart().startsWith("SELECT")) continue;
        assert.notMatch(query, /SELECT\s+\*/);
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${query}`,
          parameters,
        );
        const detail = plan.map((row) => row.detail).join("\n");
        assert.notMatch(detail, /SCAN (?:o|l|w|h|c|r|q|t|p)(?:\n|$)/);
        if (query.includes("AS accepted"))
          assert.include(detail, "overseer_occurrences_delivery_idx");
      }
      const incoming = ThreadId.make("target:link:00");
      assert.equal(
        (yield* service.list({ threadId: incoming, cursor: first.nextCursor! }).pipe(Effect.flip))
          .reason,
        "invalid_cursor",
      );
      assert.equal(
        (yield* service.list({ threadId: observer, cursor: "bad" }).pipe(Effect.flip)).reason,
        "invalid_cursor",
      );
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect("page one establishes both roles even with 25 outgoing links", () =>
  Effect.gen(function* () {
    yield* setup();
    for (let index = 0; index < 25; index++) yield* reserve(`link:${index}`);
    const sql = yield* SqlClient.SqlClient;
    // The link is reserved before target creation in production; this is its final stored shape.
    yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id) VALUES ('parent')`;
    yield* sql`INSERT INTO overseer_links (link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id,
      client_request_id, fingerprint, launch_command_id, launch_message_id, launch_input_json)
      VALUES ('zzz-incoming', 'parent', 'project:b', ${observer}, 'project:a', 'incoming', 'incoming', 'incoming', 'incoming', '{}')`;
    const service = yield* Service.OverseerService;
    const first = yield* service.list({ threadId: observer });
    assert.equal(first.links.length, 20);
    assert.equal(first.links[0]!.targetThreadId, observer);
    assert.equal(first.links[1]!.observerThreadId, observer);
    const second = yield* service.list({ threadId: observer, cursor: first.nextCursor! });
    assert.equal(second.links.length, 6);
    assert.equal(new Set([...first.links, ...second.links].map((link) => link.linkId)).size, 26);
    const oldCursor = Buffer.from(
      JSON.stringify([1, invocation.environmentId, observer, "link:0"]),
    ).toString("base64url");
    assert.equal(
      (yield* service.list({ threadId: observer, cursor: oldCursor }).pipe(Effect.flip)).reason,
      "invalid_cursor",
    );
  }).pipe(enabled, Effect.provide(harness())),
);

it.effect("preferences watch is read-only and updates round-trip with revision invalidation", () =>
  Effect.gen(function* () {
    yield* setup();
    yield* reserve();
    const sql = yield* SqlClient.SqlClient;
    const service = yield* Service.OverseerService;
    const before = yield* sql`SELECT total_changes() AS changes`;
    const pull = yield* Stream.toPull(service.watch({ threadId: observer }));
    const initial = (yield* pull)[0]!;
    assert.deepEqual(initial.preferences, {
      autoWake: true,
      cadenceEnabled: false,
      cadenceMinutes: 5,
      idleEnabled: false,
      idleMinutes: 30,
    });
    assert.deepEqual(yield* sql`SELECT total_changes() AS changes`, before);
    const preferences = {
      autoWake: false,
      cadenceEnabled: true,
      cadenceMinutes: 15 as const,
      idleEnabled: true,
      idleMinutes: 120 as const,
    };
    assert.deepEqual(yield* service.configure({ threadId: observer, preferences }), preferences);
    const updated = (yield* pull)[0]!;
    assert.isAbove(updated.revision, initial.revision);
    assert.deepEqual(updated.preferences, preferences);
    assert.equal(
      (yield* service
        .configure({ threadId: ThreadId.make("missing"), preferences })
        .pipe(Effect.flip)).reason,
      "not_found",
    );
    assert.equal(
      (yield* service
        .configure({ threadId: observer, preferences })
        .pipe(Effect.provideService(Feature.Enabled, false), Effect.flip)).reason,
      "unsupported",
    );
  }).pipe(enabled, Effect.provide(harness())),
);

it.effect("unlink is endpoint isolated and stale-idempotent", () =>
  Effect.gen(function* () {
    yield* setup();
    const row = yield* reserve();
    const service = yield* Service.OverseerService;
    const linkId = (yield* service.list({ threadId: observer })).links[0]!.linkId;
    assert.equal(
      (yield* service.unlink({ linkId }, ThreadId.make("outsider")).pipe(Effect.flip)).reason,
      "not_found",
    );
    assert.equal(
      (yield* service.unlink({ linkId: row.link_id }, observer).pipe(Effect.flip)).reason,
      "not_found",
    );
    assert.deepEqual(yield* service.unlink({ linkId }, ThreadId.make(row.target_thread_id)), {
      linkId,
      closed: true,
    });
    assert.deepEqual(yield* service.unlink({ linkId }, observer), { linkId, closed: true });
    assert.deepEqual((yield* service.list({ threadId: observer })).links, []);
    const sql = yield* SqlClient.SqlClient;
    assert.equal(
      (yield* sql`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${observer}`)
        .length,
      1,
    );
  }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "two isolated environments with identical private IDs cannot unlink each other's link",
  () =>
    Effect.gen(function* () {
      const first = yield* Effect.gen(function* () {
        yield* setup();
        yield* reserve();
        return (yield* (yield* Service.OverseerService).list({ threadId: observer })).links[0]!
          .linkId;
      }).pipe(
        enabled,
        Effect.provide(harness({ environmentId: EnvironmentId.make("first") })),
        Effect.scoped,
      );
      yield* Effect.gen(function* () {
        yield* setup();
        yield* reserve();
        const service = yield* Service.OverseerService;
        const own = (yield* service.list({ threadId: observer })).links[0]!.linkId;
        assert.notEqual(first, own);
        assert.equal(
          (yield* service.unlink({ linkId: first }, observer).pipe(Effect.flip)).reason,
          "not_found",
        );
        assert.equal((yield* service.list({ threadId: observer })).links.length, 1);
        yield* service.unlink({ linkId: own }, observer);
      }).pipe(
        enabled,
        Effect.provide(harness({ environmentId: EnvironmentId.make("second") })),
        Effect.scoped,
      );
    }),
);

it.effect(
  "watch captures its high-water under the gate and coalesces replacements without writes",
  () =>
    Effect.gen(function* () {
      yield* setup();
      yield* reserve();
      const service = yield* Service.OverseerService;
      const store = yield* Store.OverseerStore;
      const pull = yield* Stream.toPull(service.watch({ threadId: observer }));
      const initial = (yield* pull)[0]!;
      yield* store.record({
        linkId: "private-link",
        kind: "completion",
        sourceId: "root",
        sourceSequence: 1,
        ready: true,
      });
      yield* store.hold(observer, "provider");
      yield* store.hold(observer, "authorization");
      const replacement = (yield* pull)[0]!;
      assert.isAbove(replacement.revision, initial.revision);
      assert.equal(replacement.links[0]!.deliveryState, "error");
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT total_changes() AS changes`;
      const another = yield* Stream.toPull(service.watch({ threadId: observer }));
      assert.equal((yield* another)[0]!.revision, replacement.revision);
      assert.deepEqual(yield* sql`SELECT total_changes() AS changes`, before);
      yield* service.unlink({ linkId: initial.links[0]!.linkId });
      assert.deepEqual((yield* pull)[0]!.links, []);
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "an update racing the initial snapshot cannot fall between high-water and subscription",
  () =>
    Effect.gen(function* () {
      yield* setup();
      yield* reserve();
      const service = yield* Service.OverseerService;
      const store = yield* Store.OverseerStore;
      const reading = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const writing = yield* Deferred.make<void>();
      let blocked = false;
      const intercept: Statement.Transformer = (statement) =>
        Effect.gen(function* () {
          const [query] = statement.compile();
          if (!blocked && query.includes("WHERE state = 'active' AND observer_thread_id")) {
            blocked = true;
            yield* Deferred.succeed(reading, undefined);
            yield* Deferred.await(release);
          }
          return statement;
        });
      const pull = yield* Stream.toPull(
        service
          .watch({ threadId: observer })
          .pipe(Stream.provideService(Statement.CurrentTransformer, intercept)),
      );
      const first = yield* pull.pipe(Effect.forkChild);
      yield* Deferred.await(reading);
      const writer = yield* Deferred.succeed(writing, undefined).pipe(
        Effect.andThen(store.hold(observer, "provider")),
        Effect.forkChild,
      );
      yield* Deferred.await(writing);
      yield* Deferred.succeed(release, undefined);
      const initial = (yield* Fiber.join(first))[0]!;
      yield* Fiber.join(writer);
      const updated = (yield* pull)[0]!;
      assert.isAbove(updated.revision, initial.revision);
    }).pipe(enabled, Effect.provide(harness())),
);

it.effect("MCP list cannot select an endpoint and read-only credentials cannot unlink", () =>
  Effect.gen(function* () {
    const saga = yield* setup();
    const launched = yield* saga.launch({ ...request, title: "🦀".repeat(130) });
    const service = yield* Service.OverseerService;
    const page = yield* service.link({ op: "list" });
    assert.property(page, "links");
    const visible = (yield* service.list({ threadId: observer })).links[0]!;
    assert.equal(Array.from(visible.title).length, 120);
    assert.isTrue(visible.titleTruncated);
    const linkId = (yield* service.list({ threadId: observer })).links[0]!.linkId;
    const readonly = {
      ...invocation,
      client: { access: "read-only" as const, sessionId: "readonly", label: "readonly" },
    };
    const failure = yield* service
      .link({ op: "unlink", linkId })
      .pipe(Effect.provideService(McpInvocation.McpInvocationContext, readonly), Effect.flip);
    assert.equal(failure._tag, "OrchestratorMcpFailure");
    if (failure._tag === "OrchestratorMcpFailure") assert.equal(failure.code, "capability_denied");
    const endpoint = {
      ...invocation,
      thread: { ...invocation.thread!, threadId: launched.projection.thread.id },
    };
    const incoming = yield* service
      .link({ op: "list" })
      .pipe(Effect.provideService(McpInvocation.McpInvocationContext, endpoint));
    assert.deepEqual(incoming, {
      links: [{ ...(yield* service.list({ threadId: launched.projection.thread.id })).links[0]! }],
    });
    const unlinked = yield* service
      .link({ op: "unlink", linkId })
      .pipe(Effect.provideService(McpInvocation.McpInvocationContext, endpoint));
    assert.deepEqual(unlinked, { linkId, closed: true });
    assert.equal(
      (yield* (yield* SqlClient.SqlClient)`SELECT 1 FROM orchestration_v2_projection_threads WHERE thread_id = ${launched.projection.thread.id}`)
        .length,
      1,
    );
  }).pipe(enabled, Effect.provide(harness())),
);

it.effect(
  "gate publishes only successful commits, with indexed dependencies and a one-signal queue",
  () =>
    Effect.gen(function* () {
      const gate = yield* Gate.OverseerGate;
      const sub = yield* gate.commit(gate.listen(["viewed", "dependency"]));
      yield* gate.commit(gate.changed("unrelated"));
      assert.equal(yield* Queue.size(sub.queue), 0);
      yield* gate
        .commit(gate.changed("viewed").pipe(Effect.andThen(Effect.fail("rollback"))))
        .pipe(Effect.flip);
      assert.equal(yield* Queue.size(sub.queue), 0);
      for (let i = 0; i < 100; i++) yield* gate.commit(gate.changed("dependency"));
      assert.equal(yield* Queue.size(sub.queue), 1);
      assert.equal(yield* Queue.take(sub.queue), yield* gate.revision);
      sub.update(["replacement"]);
      yield* gate.commit(gate.changed("dependency"));
      assert.equal(yield* Queue.size(sub.queue), 0);
    }).pipe(Effect.provideService(Feature.Enabled, true), Effect.provide(Gate.layer)),
);

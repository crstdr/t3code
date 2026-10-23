import {
  OversightError,
  ThreadId,
  ProjectId,
  type OversightWatchInput,
  type OversightWatchFrame,
  type OversightPage,
  type OversightLink,
  type OversightUnlinkInput,
  type OversightConfigureInput,
  type OversightPreferences,
  OversightLinkInput,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import { readCaller, unavailable } from "../mcp/threadAccess.ts";
import { linkText } from "./OverseerText.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Gate from "./OverseerGate.ts";
import * as Store from "./OverseerStore.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Preferences from "./OverseerPreferences.ts";
import * as Launch from "./OverseerLaunchService.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";

export class OverseerService extends Context.Service<
  OverseerService,
  {
    readonly link: (
      input: typeof OversightLinkInput.Type,
    ) => Effect.Effect<
      OversightPage | { readonly linkId: string; readonly closed: true },
      OversightError | OrchestratorMcpFailure,
      McpInvocationContext.McpInvocationContext
    >;
    readonly launch: Launch.OverseerLaunchService["Service"]["launch"];
    readonly list: (input: OversightWatchInput) => Effect.Effect<OversightPage, OversightError>;
    readonly watch: (
      input: OversightWatchInput,
    ) => Stream.Stream<OversightWatchFrame, OversightError>;
    readonly configure: (
      input: OversightConfigureInput,
    ) => Effect.Effect<OversightPreferences, OversightError>;
    readonly unlink: (
      input: OversightUnlinkInput,
      caller?: ThreadId,
    ) => Effect.Effect<{ readonly linkId: string; readonly closed: true }, OversightError>;
  }
>()("t3/overseer/OverseerService") {}

const Handle = Schema.Tuple([Schema.String, Schema.String]);
const Cursor = Schema.Tuple([Schema.Literal(2), Schema.String, Schema.String, Schema.String]);
const decodeHandle = Schema.decodeUnknownOption(Handle);
const decodeCursor = Schema.decodeUnknownOption(Cursor);
const opaque = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
const unpack = (value: string): unknown => {
  if (value.length > 4096 || !/^[A-Za-z0-9_-]+$/.test(value)) return null;
  try {
    return JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    return null;
  }
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.OverseerStore;
  const preferences = yield* Preferences.OverseerPreferences;
  const gate = yield* Gate.OverseerGate;
  const launches = yield* Launch.OverseerLaunchService;
  const threads = yield* Threads.ThreadManagementService;
  const identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
  const environment = yield* identity.getEnvironmentId;
  const handle = (id: string) => opaque([environment, id]);
  const available = Effect.gen(function* () {
    if (!(yield* Feature.Enabled)) return yield* new OversightError({ reason: "unsupported" });
  });
  const page = Effect.fn("OverseerService.page")(
    function* (input: OversightWatchInput) {
      yield* available;
      let after = "";
      if (input.cursor !== undefined) {
        const cursor = decodeCursor(unpack(input.cursor));
        if (
          cursor._tag === "None" ||
          cursor.value[1] !== environment ||
          cursor.value[2] !== input.threadId ||
          opaque(cursor.value) !== input.cursor
        )
          return yield* new OversightError({ reason: "invalid_cursor" });
        after = cursor.value[3];
      }
      const exists =
        yield* sql`SELECT 1 FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.thread_id = ${input.threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL`;
      if (!exists.length) return yield* new OversightError({ reason: "not_found" });
      const rows = yield* sql<
        Pick<
          Store.LinkRow,
          | "link_id"
          | "observer_thread_id"
          | "observer_project_id"
          | "target_thread_id"
          | "target_project_id"
        >
      >`
        SELECT link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id, 1 AS direction FROM (
          SELECT link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id FROM overseer_links l
          WHERE state = 'active' AND observer_thread_id = ${input.threadId} AND link_id > ${after}
          ORDER BY link_id LIMIT 21
        )
        UNION ALL
        SELECT link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id, 0 AS direction FROM overseer_links l
        WHERE state = 'active' AND target_thread_id = ${input.threadId} AND ${after} = ''
        ORDER BY direction, link_id LIMIT 21`;
      const links: OversightLink[] = [];
      for (const row of rows.slice(0, 20)) {
        const counterpart =
          row.observer_thread_id === input.threadId ? row.target_thread_id : row.observer_thread_id;
        // Match the shell's latestUnheldRun selection without loading run histories.
        const details = (yield* sql<{
          readonly title: string | null;
          readonly worker_status: OversightLink["workerStatus"];
          readonly requests: number;
          readonly held: number;
          readonly suspended: number;
          readonly queued: number;
          readonly pending: number;
          readonly accepted: number;
        }>`SELECT substr(COALESCE(json_extract(c.payload_json, '$.title'), CASE WHEN ${counterpart} = l.target_thread_id THEN json_extract(l.launch_input_json, '$.title') END), 1, 121) AS title,
        CASE WHEN w.thread_id IS NULL THEN 'reserved' WHEN w.deleted_at IS NOT NULL THEN 'deleted' WHEN w.archived_at IS NOT NULL THEN 'archived'
          ELSE COALESCE((SELECT r.status FROM orchestration_v2_projection_runs r WHERE r.thread_id = l.target_thread_id
            AND NOT (r.status = 'queued' AND json_extract(r.payload_json, '$.queueHeld') IS 1)
            ORDER BY r.ordinal DESC, r.run_id DESC LIMIT 1), 'idle') END AS worker_status,
        (SELECT COUNT(*) FROM orchestration_v2_projection_runtime_requests q WHERE q.thread_id = l.target_thread_id AND q.status = 'pending' AND q.kind = 'user_input') AS requests,
        COALESCE(m.hold_mask, 0) AS held,
        (w.archived_at IS NOT NULL OR h.archived_at IS NOT NULL OR json_extract(h.payload_json, '$.snoozedUntil') IS NOT NULL
          OR EXISTS(SELECT 1 FROM orchestration_v2_projection_runs held WHERE held.thread_id = l.observer_thread_id AND held.status = 'queued' AND json_extract(held.payload_json, '$.queueHeld') = 1)) AS suspended,
        EXISTS(SELECT 1 FROM overseer_occurrences o WHERE o.link_id = l.link_id AND o.disposition = 'pending' AND o.batch_number IS NOT NULL) AS queued,
        EXISTS(SELECT 1 FROM overseer_occurrences o WHERE o.link_id = l.link_id AND o.disposition = 'pending') AS pending,
        EXISTS(SELECT 1 FROM overseer_occurrences o WHERE o.link_id = l.link_id AND o.disposition = 'accepted') AS accepted
        FROM overseer_links l LEFT JOIN orchestration_v2_projection_threads c ON c.thread_id = ${counterpart}
        LEFT JOIN orchestration_v2_projection_threads w ON w.thread_id = l.target_thread_id
        LEFT JOIN orchestration_v2_projection_threads h ON h.thread_id = l.observer_thread_id
        LEFT JOIN overseer_mailboxes m ON m.observer_thread_id = l.observer_thread_id WHERE l.link_id = ${row.link_id}`)[0]!;
        const title = Array.from(details.title ?? "");
        links.push({
          linkId: handle(row.link_id),
          observerThreadId: ThreadId.make(row.observer_thread_id),
          targetThreadId: ThreadId.make(row.target_thread_id),
          projectId: ProjectId.make(
            row.observer_thread_id === input.threadId
              ? row.target_project_id
              : row.observer_project_id,
          ),
          title: title.slice(0, 120).join(""),
          ...(title.length > 120 ? { titleTruncated: true as const } : {}),
          workerStatus: details.worker_status,
          pendingRequestCount: details.requests,
          deliveryState:
            details.pending && (details.held & 24) !== 0
              ? "error"
              : details.pending && (details.held !== 0 || details.suspended !== 0)
                ? "held"
                : details.queued
                  ? "queued"
                  : details.pending
                    ? "pending"
                    : details.accepted
                      ? "accepted"
                      : "pending",
        });
      }
      return {
        links,
        ...(rows.length > 20
          ? { nextCursor: opaque([2, environment, input.threadId, rows[19]!.link_id]) }
          : {}),
      };
    },
    Effect.mapError((error) =>
      error._tag === "OversightError" ? error : new OversightError({ reason: "unavailable" }),
    ),
  );
  const list = Effect.fn("OverseerService.list")((input: OversightWatchInput) =>
    gate
      .commit(sql.withTransaction(page(input)))
      .pipe(
        Effect.mapError((error) =>
          error._tag === "OversightError" ? error : new OversightError({ reason: "unavailable" }),
        ),
      ),
  );
  const watchPreferences = Effect.fn("OverseerService.watchPreferences")(
    function* (observer: ThreadId) {
      const exists =
        yield* sql`SELECT 1 FROM overseer_mailboxes WHERE observer_thread_id = ${observer}`;
      return exists.length ? { preferences: yield* preferences.get(observer) } : {};
    },
    Effect.mapError(() => new OversightError({ reason: "unavailable" })),
  );
  const dependencies = (input: OversightWatchInput, result: OversightPage) => [
    input.threadId,
    ...result.links.flatMap((link) => [link.observerThreadId, link.targetThreadId]),
  ];
  const unlink = Effect.fn("OverseerService.unlink")(function* (
    input: OversightUnlinkInput,
    caller?: ThreadId,
  ) {
    yield* available;
    const decoded = decodeHandle(unpack(input.linkId));
    if (
      decoded._tag === "None" ||
      decoded.value[0] !== environment ||
      opaque(decoded.value) !== input.linkId
    )
      return yield* new OversightError({ reason: "not_found" });
    const id = decoded.value[1];
    return yield* gate
      .commit(
        Effect.gen(function* () {
          const link = (yield* sql<
            Pick<Store.LinkRow, "observer_thread_id" | "target_thread_id">
          >`SELECT observer_thread_id, target_thread_id FROM overseer_links WHERE link_id = ${id}`)[0];
          if (
            !link ||
            (caller !== undefined &&
              caller !== link.observer_thread_id &&
              caller !== link.target_thread_id)
          )
            return yield* new OversightError({ reason: "not_found" });
          yield* store.unlink(id);
          return { linkId: input.linkId, closed: true as const };
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          error._tag === "OversightError" ? error : new OversightError({ reason: "unavailable" }),
        ),
      );
  });
  return OverseerService.of({
    link: Effect.fn("OverseerService.link")(
      function* (input) {
        yield* available;
        const caller = yield* readCaller();
        if (caller.scope.environmentId !== environment)
          return yield* new OversightError({ reason: "not_found" });
        if (!caller.caller)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: linkText.caller,
          });
        if (input.op === "list")
          return yield* list({
            threadId: caller.caller.id,
            ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
          });
        return yield* gate.commit(
          Effect.gen(function* () {
            const fresh = yield* McpToolAccess.writingCaller;
            return yield* unlink({ linkId: input.linkId ?? "" }, fresh.caller!.id);
          }),
        );
      },
      Effect.provideService(Threads.ThreadManagementService, threads),
    ),
    launch: Effect.fn("OverseerService.launch")(function* (input) {
      if (!(yield* Feature.Enabled)) return yield* unavailable();
      const caller = yield* McpInvocationContext.McpInvocationContext;
      if (caller.environmentId !== environment)
        return yield* new Launch.OverseerLaunchError({ reason: "E6" });
      const result = yield* launches
        .launch(input)
        .pipe(
          Effect.mapError((error) =>
            error._tag === "OverseerLaunchError" && error.linkId !== undefined
              ? new Launch.OverseerLaunchError({ ...error, linkId: handle(error.linkId) })
              : error,
          ),
        );
      return {
        ...result,
        ...(result.linkId === undefined ? {} : { linkId: handle(result.linkId) }),
      };
    }),
    list,
    configure: Effect.fn("OverseerService.configure")(function* (input) {
      yield* available;
      return yield* gate
        .commit(
          Effect.gen(function* () {
            const exists =
              yield* sql`SELECT 1 FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
          JOIN overseer_mailboxes m ON m.observer_thread_id = t.thread_id
          WHERE t.thread_id = ${input.threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL`;
            if (!exists.length) return yield* new OversightError({ reason: "not_found" });
            const previous = yield* preferences.get(input.threadId);
            return yield* preferences.configure(input.threadId, {
              ...previous,
              ...input.preferences,
            });
          }),
        )
        .pipe(
          Effect.mapError((error) =>
            error._tag === "OversightError" ? error : new OversightError({ reason: "unavailable" }),
          ),
        );
    }),
    watch: (input) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const initial = yield* gate.commit(
            Effect.gen(function* () {
              const result = yield* list(input);
              const project = (yield* sql<{
                readonly project_id: string;
              }>`SELECT project_id FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}`.pipe(
                Effect.mapError(() => new OversightError({ reason: "unavailable" })),
              ))[0]!;
              const fixed = ["*", `project:${project.project_id}`];
              const subscription = yield* gate.listen([...fixed, ...dependencies(input, result)]);
              const settings = yield* watchPreferences(input.threadId);
              return { result, subscription, fixed, settings };
            }),
          );
          let emitted = initial.subscription.revision;
          const updates = Stream.fromQueue(initial.subscription.queue).pipe(
            Stream.filter((revision) => revision > emitted),
            Stream.mapEffect(() =>
              gate.commit(
                Effect.gen(function* () {
                  const result = yield* list(input);
                  initial.subscription.update([...initial.fixed, ...dependencies(input, result)]);
                  emitted = yield* gate.revision;
                  const settings = yield* watchPreferences(input.threadId);
                  return { ...result, ...settings, revision: emitted };
                }),
              ),
            ),
          );
          return Stream.concat(
            Stream.succeed({
              ...initial.result,
              ...initial.settings,
              revision: initial.subscription.revision,
            }),
            updates,
          ).pipe(Stream.changesWith((a, b) => a.revision === b.revision));
        }),
      ),
    unlink,
  });
});
export const layer = Layer.effect(OverseerService, make).pipe(
  Layer.provide(Layer.merge(Gate.layer, Preferences.layer)),
);

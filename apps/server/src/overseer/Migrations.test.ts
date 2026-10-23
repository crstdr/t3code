import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Migrator from "effect/sql/Migrator";
import CanonicalState from "./Migration.ts";
import WakeFacts from "./WakeFactsMigration.ts";
import Mailbox from "./MailboxMigration.ts";
import Recovery from "./RecoveryMigration.ts";
import {
  migrationManifest,
  runMigrations as runCoreMigrations,
} from "../persistence/Migrations.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import { runMigrations } from "./Migrations.ts";

const featureTables = [
  "overseer_attempts",
  "overseer_background_owners",
  "overseer_batches",
  "overseer_links",
  "overseer_mailboxes",
  "overseer_migrations",
  "overseer_occurrences",
  "overseer_preferences",
  "overseer_transports",
  "overseer_work_runs",
];
const tables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    readonly name: string;
  }>`SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 'overseer_*' ORDER BY name`).map(
    (row) => row.name,
  );
});
const coreHistory = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    readonly migration_id: number;
    readonly name: string;
  }>`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`).map(
    (row) => [row.migration_id, row.name] as const,
  );
});

it.effect("fresh application persistence applies the feature track after core migrations", () =>
  Effect.gen(function* () {
    assert.deepEqual(yield* tables, featureTables);
    assert.deepEqual(yield* coreHistory, migrationManifest);
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM overseer_migrations`, [
      { migration_id: 1, name: "CanonicalState" },
      { migration_id: 2, name: "WakeFacts" },
      { migration_id: 3, name: "Mailbox" },
      { migration_id: 4, name: "Recovery" },
      { migration_id: 5, name: "WakeControls" },
      { migration_id: 6, name: "Api" },
    ]);
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_schema WHERE name='overseer_attempts_run_attempt_idx'`,
      [],
    );
    assert.isTrue(
      (yield* sql<{ readonly name: string }>`PRAGMA table_info(overseer_attempts)`).some(
        (column) => column.name === "run_attempt_id",
      ),
    );
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);

it.effect("upstream-only migration guard accepts a database with namespaced feature state", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runCoreMigrations();
    const before = yield* coreHistory;
    assert.deepEqual(yield* runMigrations(), [
      [1, "CanonicalState"],
      [2, "WakeFacts"],
      [3, "Mailbox"],
      [4, "Recovery"],
      [5, "WakeControls"],
      [6, "Api"],
    ]);
    yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id, hold_mask) VALUES ('observer', 1)`;
    assert.deepEqual(yield* runCoreMigrations(), []);
    assert.deepEqual(yield* coreHistory, before);
    assert.deepEqual(yield* sql`SELECT observer_thread_id, hold_mask FROM overseer_mailboxes`, [
      { observer_thread_id: "observer", hold_mask: 1 },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("upgrades the slice-1 schema without changing existing mailbox revisions", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runCoreMigrations();
    yield* Migrator.make({})({
      table: "overseer_migrations",
      loader: Migrator.fromRecord({ "1_CanonicalState": CanonicalState }),
    });
    yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id, revision) VALUES ('observer', 7)`;
    assert.deepEqual(yield* runMigrations(), [
      [2, "WakeFacts"],
      [3, "Mailbox"],
      [4, "Recovery"],
      [5, "WakeControls"],
      [6, "Api"],
    ]);
    assert.deepEqual(
      yield* sql`SELECT revision FROM overseer_mailboxes WHERE observer_thread_id = 'observer'`,
      [{ revision: 7 }],
    );
    assert.deepEqual(yield* tables, featureTables);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "upgrades frozen slice-3c state without changing occurrence membership or receipt history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runCoreMigrations();
      yield* Migrator.make({})({
        table: "overseer_migrations",
        loader: Migrator.fromRecord({
          "1_CanonicalState": CanonicalState,
          "2_WakeFacts": WakeFacts,
          "3_Mailbox": Mailbox,
          "4_Recovery": Recovery,
        }),
      });
      yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id, revision) VALUES ('observer', 7)`;
      yield* sql`INSERT INTO overseer_links(link_id, observer_thread_id, observer_project_id, target_thread_id, target_project_id,
      client_request_id, fingerprint, launch_command_id, launch_message_id, launch_accepted) VALUES ('link', 'observer', 'project', 'worker', 'project', 'request', 'input', 'launch', 'message', 1)`;
      yield* sql`INSERT INTO overseer_batches VALUES ('observer', 1, 0, 7, 'frozen')`;
      for (const disposition of ["pending", "accepted", "withdrawn", "suppressed"])
        yield* sql`INSERT INTO overseer_occurrences VALUES ('link', 'question', ${disposition}, 'observer', 1, 1, ${disposition}, 1)`;
      yield* sql`INSERT INTO overseer_attempts(observer_thread_id, batch_number, attempt_number, command_id, message_id, state)
      VALUES ('observer', 1, 1, 'delivery', 'notice', 'pending')`;
      const before = yield* sql`SELECT * FROM overseer_occurrences ORDER BY source_id`;
      const schema = yield* sql<{
        readonly name: string;
      }>`SELECT name FROM sqlite_schema WHERE tbl_name = 'overseer_occurrences'
        AND type IN ('index', 'trigger') AND name NOT LIKE 'sqlite_%'`;
      assert.deepEqual(yield* runMigrations(), [
        [5, "WakeControls"],
        [6, "Api"],
      ]);
      const migrated = yield* sql<{
        readonly name: string;
      }>`SELECT name FROM sqlite_schema WHERE tbl_name = 'overseer_occurrences'
        AND type IN ('index', 'trigger') AND name NOT LIKE 'sqlite_%'`;
      for (const object of schema) assert.isTrue(migrated.some((row) => row.name === object.name));
      assert.deepEqual(yield* sql`SELECT * FROM overseer_occurrences ORDER BY source_id`, before);
      assert.deepEqual(yield* sql`SELECT state, purpose, idle_token FROM overseer_batches`, [
        { state: "frozen", purpose: "updates", idle_token: null },
      ]);
      assert.deepEqual(yield* sql`SELECT state FROM overseer_attempts`, [{ state: "pending" }]);
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepEqual(yield* coreHistory, migrationManifest);
      assert.deepEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("restores the bounded source index when a core projection recreation removed it", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const schema = (yield* sql<{
      readonly sql: string;
    }>`SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'orchestration_v2_projection_subagents'`)[0]!;
    yield* sql`DROP TABLE orchestration_v2_projection_subagents`;
    yield* sql.unsafe(schema.sql);
    assert.deepEqual(yield* runMigrations(), []);
    const plans = yield* sql<{
      readonly detail: string;
    }>`EXPLAIN QUERY PLAN SELECT 1 FROM orchestration_v2_projection_subagents WHERE run_id = 'run' AND status = 'running'`;
    assert.isTrue(plans.some((row) => row.detail.includes("overseer_subagents_run_idx")));
    assert.deepEqual(yield* coreHistory, migrationManifest);
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);

it.effect("re-running the feature migrator is idempotent and preserves data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runCoreMigrations();
    yield* runMigrations();
    yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id, revision) VALUES ('observer', 12)`;
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* tables, featureTables);
    assert.deepEqual(yield* sql`SELECT observer_thread_id, revision FROM overseer_mailboxes`, [
      { observer_thread_id: "observer", revision: 12 },
    ]);
    assert.deepEqual(yield* sql`SELECT migration_id, name FROM overseer_migrations`, [
      { migration_id: 1, name: "CanonicalState" },
      { migration_id: 2, name: "WakeFacts" },
      { migration_id: 3, name: "Mailbox" },
      { migration_id: 4, name: "Recovery" },
      { migration_id: 5, name: "WakeControls" },
      { migration_id: 6, name: "Api" },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

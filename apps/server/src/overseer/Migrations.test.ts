import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  migrationManifest,
  runMigrations as runCoreMigrations,
} from "../persistence/Migrations.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import { runMigrations } from "./Migrations.ts";

const featureTables = [
  "overseer_attempts",
  "overseer_batches",
  "overseer_links",
  "overseer_mailboxes",
  "overseer_migrations",
  "overseer_occurrences",
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
    assert.deepEqual(yield* runMigrations(), [[1, "CanonicalState"]]);
    yield* sql`INSERT INTO overseer_mailboxes(observer_thread_id, hold_mask) VALUES ('observer', 1)`;
    assert.deepEqual(yield* runCoreMigrations(), []);
    assert.deepEqual(yield* coreHistory, before);
    assert.deepEqual(yield* sql`SELECT observer_thread_id, hold_mask FROM overseer_mailboxes`, [
      { observer_thread_id: "observer", hold_mask: 1 },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
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
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

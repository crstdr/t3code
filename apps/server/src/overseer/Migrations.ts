import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";
import CanonicalState from "./Migration.ts";
import WakeFacts from "./WakeFactsMigration.ts";

const run = Migrator.make({});
const loader = Migrator.fromRecord({
  "1_CanonicalState": CanonicalState,
  "2_WakeFacts": WakeFacts,
});

// A fork must not consume upstream's migration IDs or leave its manifest divergent.
export const runMigrations = Effect.fn("overseer.runMigrations")(function* () {
  const applied = yield* run({ table: "overseer_migrations", loader });
  const sql = yield* SqlClient.SqlClient;
  // Core migrations can recreate projections without rerunning the canonical feature track.
  yield* sql`CREATE INDEX IF NOT EXISTS overseer_subagents_run_idx ON orchestration_v2_projection_subagents(run_id, status)`;
  return applied;
});

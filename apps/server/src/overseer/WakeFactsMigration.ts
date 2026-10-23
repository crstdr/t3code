import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX overseer_occurrences_link_candidates_idx ON overseer_occurrences(link_id, kind, source_id)
    WHERE kind = 'completion' AND disposition = 'pending' AND ready = 0`;
  // Causal identities survive projection rebuilds; lifecycle and output stay in core projections.
  yield* sql`CREATE TABLE overseer_work_runs (
    run_id TEXT PRIMARY KEY,
    link_id TEXT NOT NULL REFERENCES overseer_links(link_id),
    root_run_id TEXT,
    restart_interrupted INTEGER NOT NULL DEFAULT 0 CHECK (restart_interrupted IN (0, 1))
  )`;
  yield* sql`CREATE INDEX overseer_work_runs_root_idx ON overseer_work_runs(link_id, root_run_id, run_id)`;
  yield* sql`CREATE TABLE overseer_background_owners (
    link_id TEXT NOT NULL REFERENCES overseer_links(link_id),
    provider_thread_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    root_run_id TEXT,
    PRIMARY KEY (link_id, provider_thread_id, task_id)
  )`;
  yield* sql`CREATE INDEX overseer_background_owners_root_idx ON overseer_background_owners(link_id, root_run_id, provider_thread_id, task_id)`;
});

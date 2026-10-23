import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE overseer_mailboxes ADD COLUMN flush_id TEXT`;
  yield* sql`CREATE TABLE overseer_transports (
    message_id TEXT PRIMARY KEY,
    observer_thread_id TEXT NOT NULL,
    batch_number INTEGER NOT NULL,
    command_id TEXT NOT NULL UNIQUE,
    epoch INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    text TEXT NOT NULL CHECK (length(CAST(text AS BLOB)) BETWEEN 1 AND 2048),
    members_json TEXT NOT NULL CHECK (json_valid(members_json)),
    phase TEXT NOT NULL CHECK (phase IN ('prepared', 'queued', 'frozen', 'submitted', 'received', 'retired')),
    run_id TEXT,
    run_attempt_id TEXT,
    native_uuid TEXT UNIQUE,
    provider_session_id TEXT,
    receipt_kind TEXT CHECK (receipt_kind IN ('codex_response', 'claude_echo', 'claude_started')),
    FOREIGN KEY(observer_thread_id, batch_number) REFERENCES overseer_batches(observer_thread_id, batch_number)
  )`;
  yield* sql`CREATE UNIQUE INDEX overseer_transports_open_idx ON overseer_transports(observer_thread_id)
    WHERE phase IN ('prepared', 'queued', 'frozen', 'submitted')`;
  yield* sql`CREATE INDEX overseer_transports_batch_idx ON overseer_transports(observer_thread_id, batch_number)`;
  yield* sql`CREATE INDEX overseer_transports_run_idx ON overseer_transports(run_id, message_id)`;
});

import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE overseer_preferences (
    observer_thread_id TEXT PRIMARY KEY REFERENCES overseer_mailboxes(observer_thread_id),
    auto_wake INTEGER NOT NULL DEFAULT 1 CHECK(auto_wake IN (0, 1)),
    cadence_enabled INTEGER NOT NULL DEFAULT 0 CHECK(cadence_enabled IN (0, 1)),
    cadence_minutes INTEGER NOT NULL DEFAULT 5 CHECK(cadence_minutes IN (1, 5, 10, 15, 30, 60, 180)),
    idle_enabled INTEGER NOT NULL DEFAULT 0 CHECK(idle_enabled IN (0, 1)),
    idle_minutes INTEGER NOT NULL DEFAULT 30 CHECK(idle_minutes IN (10, 30, 60, 120, 360)),
    last_routine_accepted_at TEXT,
    idle_due_at TEXT,
    idle_pending_token TEXT
  )`;
  yield* sql`CREATE INDEX overseer_preferences_idle_due_idx ON overseer_preferences(idle_due_at, observer_thread_id)
    WHERE idle_enabled = 1 AND idle_due_at IS NOT NULL AND idle_pending_token IS NULL`;
  yield* sql`CREATE INDEX overseer_preferences_idle_enabled_idx ON overseer_preferences(observer_thread_id) WHERE idle_enabled = 1`;
  yield* sql`ALTER TABLE overseer_batches ADD COLUMN purpose TEXT NOT NULL DEFAULT 'updates' CHECK(purpose IN ('updates', 'idle'))`;
  yield* sql`ALTER TABLE overseer_batches ADD COLUMN idle_token TEXT`;
  yield* sql`CREATE TABLE overseer_occurrences_next (
    link_id TEXT NOT NULL REFERENCES overseer_links(link_id),
    kind TEXT NOT NULL CHECK(kind IN ('completion', 'question', 'approval')),
    source_id TEXT NOT NULL,
    observer_thread_id TEXT NOT NULL REFERENCES overseer_mailboxes(observer_thread_id),
    source_sequence INTEGER NOT NULL CHECK(source_sequence >= 0),
    ready INTEGER NOT NULL CHECK(ready IN (0, 1)),
    disposition TEXT NOT NULL DEFAULT 'pending' CHECK(disposition IN ('pending', 'accepted', 'withdrawn', 'suppressed')),
    batch_number INTEGER,
    PRIMARY KEY(link_id, kind, source_id),
    FOREIGN KEY(observer_thread_id, batch_number) REFERENCES overseer_batches(observer_thread_id, batch_number),
    CHECK(kind = 'completion' OR ready = 1)
  )`;
  yield* sql`INSERT INTO overseer_occurrences_next SELECT * FROM overseer_occurrences`;
  yield* sql`DROP TABLE overseer_occurrences`;
  yield* sql`ALTER TABLE overseer_occurrences_next RENAME TO overseer_occurrences`;
  yield* sql`CREATE INDEX overseer_occurrences_pending_idx ON overseer_occurrences(observer_thread_id, source_sequence, kind, source_id, link_id) WHERE disposition = 'pending' AND ready = 1`;
  yield* sql`CREATE INDEX overseer_occurrences_candidates_idx ON overseer_occurrences(observer_thread_id, source_sequence, source_id, link_id) WHERE disposition = 'pending' AND ready = 0`;
  yield* sql`CREATE INDEX overseer_occurrences_link_pending_idx ON overseer_occurrences(link_id, batch_number) WHERE disposition = 'pending'`;
  yield* sql`CREATE INDEX overseer_occurrences_link_candidates_idx ON overseer_occurrences(link_id, kind, source_id)
    WHERE kind = 'completion' AND disposition = 'pending' AND ready = 0`;
  yield* sql`CREATE INDEX overseer_occurrences_batch_idx ON overseer_occurrences(observer_thread_id, batch_number)`;
  yield* sql`CREATE INDEX overseer_occurrences_attention_order_idx ON overseer_occurrences(observer_thread_id,
    (kind = 'completion'), source_sequence, kind, source_id, link_id) WHERE disposition = 'pending' AND ready = 1`;
  yield* sql`CREATE INDEX overseer_occurrences_attention_idx ON overseer_occurrences(observer_thread_id)
    WHERE disposition = 'pending' AND ready = 1 AND kind IN ('question', 'approval')`;
});

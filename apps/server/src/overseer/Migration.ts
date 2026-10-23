import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Canonical operational state: no foreign keys to rebuildable core projections.
// Full-database backups include these tables; event-log-only exports do not.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE overseer_mailboxes (
      observer_thread_id TEXT PRIMARY KEY,
      epoch INTEGER NOT NULL DEFAULT 0 CHECK (epoch >= 0),
      revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
      processed_revision INTEGER NOT NULL DEFAULT 0 CHECK (processed_revision <= revision),
      next_batch_number INTEGER NOT NULL DEFAULT 1,
      hold_mask INTEGER NOT NULL DEFAULT 0 CHECK (hold_mask BETWEEN 0 AND 31),
      hold_reason TEXT GENERATED ALWAYS AS (CASE
        WHEN hold_mask & 1 THEN 'stop' WHEN hold_mask & 2 THEN 'queue'
        WHEN hold_mask & 4 THEN 'snoozed' WHEN hold_mask & 8 THEN 'provider'
        WHEN hold_mask & 16 THEN 'authorization' END) VIRTUAL
    )
  `;
  yield* sql`CREATE INDEX overseer_mailboxes_dirty_idx ON overseer_mailboxes(observer_thread_id) WHERE revision > processed_revision`;
  yield* sql`CREATE INDEX overseer_mailboxes_held_idx ON overseer_mailboxes(observer_thread_id) WHERE hold_mask <> 0`;
  yield* sql`
    CREATE TABLE overseer_links (
      link_id TEXT PRIMARY KEY,
      observer_thread_id TEXT NOT NULL REFERENCES overseer_mailboxes(observer_thread_id),
      observer_project_id TEXT NOT NULL,
      target_thread_id TEXT NOT NULL UNIQUE,
      target_project_id TEXT NOT NULL,
      client_request_id TEXT NOT NULL,
      fingerprint TEXT NOT NULL,
      launch_command_id TEXT NOT NULL UNIQUE,
      launch_message_id TEXT NOT NULL UNIQUE,
      launch_input_json TEXT CHECK (launch_input_json IS NULL OR json_valid(launch_input_json)),
      launch_accepted INTEGER NOT NULL DEFAULT 0 CHECK (launch_accepted IN (0, 1)),
      state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'closed')),
      close_reason TEXT CHECK (close_reason IN ('unlink', 'endpoint_deleted', 'project_deleted')),
      UNIQUE (observer_thread_id, client_request_id),
      CHECK (observer_thread_id <> target_thread_id),
      CHECK ((state = 'active' AND close_reason IS NULL) OR (state = 'closed' AND close_reason IS NOT NULL)),
      CHECK ((state = 'closed' OR launch_accepted = 1) OR launch_input_json IS NOT NULL)
    )
  `;
  yield* sql`CREATE INDEX overseer_links_observer_idx ON overseer_links(observer_thread_id, link_id) WHERE state = 'active'`;
  yield* sql`CREATE INDEX overseer_links_observer_project_idx ON overseer_links(observer_project_id, state, observer_thread_id, link_id)`;
  yield* sql`CREATE INDEX overseer_links_target_project_idx ON overseer_links(target_project_id, link_id) WHERE state = 'active'`;
  yield* sql`CREATE INDEX overseer_links_closed_idx ON overseer_links(link_id) WHERE state = 'closed'`;
  yield* sql`CREATE INDEX overseer_links_launch_idx ON overseer_links(link_id) WHERE state = 'active' AND launch_accepted = 0`;
  yield* sql`
    CREATE TABLE overseer_batches (
      observer_thread_id TEXT NOT NULL REFERENCES overseer_mailboxes(observer_thread_id),
      batch_number INTEGER NOT NULL,
      epoch INTEGER NOT NULL,
      revision INTEGER NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('queued', 'frozen', 'accepted', 'suppressed', 'disposed')),
      PRIMARY KEY (observer_thread_id, batch_number)
    )
  `;
  yield* sql`CREATE UNIQUE INDEX overseer_batches_open_idx ON overseer_batches(observer_thread_id) WHERE state IN ('queued', 'frozen')`;
  yield* sql`
    CREATE TABLE overseer_occurrences (
      link_id TEXT NOT NULL REFERENCES overseer_links(link_id),
      kind TEXT NOT NULL CHECK (kind IN ('completion', 'question')),
      source_id TEXT NOT NULL,
      observer_thread_id TEXT NOT NULL REFERENCES overseer_mailboxes(observer_thread_id),
      source_sequence INTEGER NOT NULL CHECK (source_sequence >= 0),
      ready INTEGER NOT NULL CHECK (ready IN (0, 1)),
      disposition TEXT NOT NULL DEFAULT 'pending' CHECK (disposition IN ('pending', 'accepted', 'withdrawn', 'suppressed')),
      batch_number INTEGER,
      PRIMARY KEY (link_id, kind, source_id),
      FOREIGN KEY (observer_thread_id, batch_number) REFERENCES overseer_batches(observer_thread_id, batch_number),
      CHECK (kind = 'completion' OR ready = 1)
    )
  `;
  yield* sql`CREATE INDEX overseer_occurrences_pending_idx ON overseer_occurrences(observer_thread_id, source_sequence, kind, source_id, link_id) WHERE disposition = 'pending' AND ready = 1`;
  yield* sql`CREATE INDEX overseer_occurrences_candidates_idx ON overseer_occurrences(observer_thread_id, source_sequence, source_id, link_id) WHERE disposition = 'pending' AND ready = 0`;
  yield* sql`CREATE INDEX overseer_occurrences_link_pending_idx ON overseer_occurrences(link_id, batch_number) WHERE disposition = 'pending'`;
  yield* sql`CREATE INDEX overseer_occurrences_batch_idx ON overseer_occurrences(observer_thread_id, batch_number)`;
  yield* sql`
    CREATE TABLE overseer_attempts (
      observer_thread_id TEXT NOT NULL,
      batch_number INTEGER NOT NULL,
      attempt_number INTEGER NOT NULL,
      command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      run_id TEXT,
      run_attempt_id TEXT,
      state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'disposed')),
      disposal_reason TEXT CHECK (disposal_reason IN ('process_loss', 'cancel', 'stop', 'lifecycle')),
      PRIMARY KEY (observer_thread_id, batch_number, attempt_number),
      FOREIGN KEY (observer_thread_id, batch_number) REFERENCES overseer_batches(observer_thread_id, batch_number),
      CHECK ((state = 'disposed' AND disposal_reason IS NOT NULL) OR (state <> 'disposed' AND disposal_reason IS NULL))
    )
  `;
  yield* sql`CREATE INDEX overseer_attempts_run_idx ON overseer_attempts(run_id, message_id)`;
  yield* sql`CREATE UNIQUE INDEX overseer_attempts_pending_idx ON overseer_attempts(observer_thread_id) WHERE state = 'pending'`;
});

import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE overseer_transports ADD COLUMN submitted_at TEXT`;
  yield* sql`ALTER TABLE overseer_transports ADD COLUMN unconfirmed_logged_at TEXT`;
  yield* sql`ALTER TABLE overseer_mailboxes ADD COLUMN retry_message_id TEXT`;
  yield* sql`CREATE INDEX overseer_mailboxes_retry_idx ON overseer_mailboxes(observer_thread_id) WHERE retry_message_id IS NOT NULL`;
  yield* sql`CREATE INDEX overseer_links_active_idx ON overseer_links(link_id) WHERE state = 'active'`;
});

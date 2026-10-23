import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Public delivery status must not scan a link's accepted occurrence history.
  yield* sql`CREATE INDEX overseer_occurrences_delivery_idx ON overseer_occurrences(link_id, disposition, batch_number)`;
});

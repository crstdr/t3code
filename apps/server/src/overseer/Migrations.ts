import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import CanonicalState from "./Migration.ts";

const run = Migrator.make({});
const loader = Migrator.fromRecord({ "1_CanonicalState": CanonicalState });

// A fork must not consume upstream's migration IDs or leave its manifest divergent.
export const runMigrations = Effect.fn("overseer.runMigrations")(() =>
  run({ table: "overseer_migrations", loader }),
);

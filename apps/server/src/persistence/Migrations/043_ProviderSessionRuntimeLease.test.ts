import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("043_ProviderSessionRuntimeLease", (it) => {
  it.effect("adds a nullable session lease to provider runtime ownership", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 41 });
      yield* runMigrations({ toMigrationInclusive: 43 });

      const columns = yield* sql<{ readonly name: string; readonly notnull: number }>`
        PRAGMA table_info(provider_session_runtime)
      `;
      const sessionLease = columns.find((column) => column.name === "session_lease");

      assert.equal(sessionLease?.name, "session_lease");
      assert.equal(sessionLease?.notnull, 0);
    }),
  );
});

import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { WsRpcGroup } from "@t3tools/contracts";
import * as Feature from "./OverseerFeature.ts";
import rpcTags from "./fixtures/upstream-11adcb0e25-rpc-tags.json" with { type: "json" };
import manifest from "../../../../packages/contracts/src/fixtures/official-v0.1.2/provenance.json" with { type: "json" };

it("disabled RPC registration has exactly the upstream method surface", () => {
  assert.deepEqual([...WsRpcGroup.omit(...Feature.rpcTags).requests.keys()].sort(), rpcTags);
  assert.equal(WsRpcGroup.requests.size, rpcTags.length + 2);
});

it.effect("pins the actual official v0.1.2 decoder and its complete dependency closure", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const crypto = yield* Crypto.Crypto;
    assert.equal(manifest.commit, "044f07257c9684ae4543bd1e1ebf9c394e9e3830");
    for (const [file, digest] of Object.entries(manifest.sourceSha256)) {
      const bytes = yield* fs.readFile(
        new URL(
          `../../../../packages/contracts/src/fixtures/official-v0.1.2/${file}`,
          import.meta.url,
        ).pathname,
      );
      assert.equal(Hex.encode(yield* crypto.digest("SHA-256", bytes)), digest);
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

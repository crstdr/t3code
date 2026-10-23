import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as OverseerFeature from "./OverseerFeature.ts";

const enabledFrom = (env: Record<string, string>) =>
  OverseerFeature.Enabled.pipe(
    Effect.provide(
      OverseerFeature.layer.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
      ),
    ),
  );

it.effect("defaults to disabled without startup configuration", () =>
  Effect.gen(function* () {
    assert.isFalse(yield* OverseerFeature.Enabled);
    assert.isFalse(yield* enabledFrom({}));
  }),
);

it.effect("enables oversight when the startup switch is true", () =>
  Effect.gen(function* () {
    assert.isTrue(yield* enabledFrom({ T3CODE_OVERSEER_ENABLED: "true" }));
  }),
);

it.effect("keeps oversight disabled when the startup switch is false", () =>
  Effect.gen(function* () {
    assert.isFalse(yield* enabledFrom({ T3CODE_OVERSEER_ENABLED: "false" }));
  }),
);

it.effect("rejects an invalid startup switch rather than silently enabling", () =>
  Effect.gen(function* () {
    const error = yield* Effect.flip(enabledFrom({ T3CODE_OVERSEER_ENABLED: "invalid" }));
    assert.equal(error._tag, "ConfigError");
  }),
);

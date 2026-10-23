import {
  OversightError,
  WS_METHODS,
  type OversightWatchInput,
  type OversightUnlinkInput,
  type OversightConfigureInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { getInitialServerConfig, requestGuarded, subscribe } from "./client.ts";

const supported = getInitialServerConfig().pipe(
  Effect.map((config) => config.environment.capabilities.threadOversight === true),
  Effect.mapError(() => new OversightError({ reason: "unavailable" })),
);
export const watch = (input: OversightWatchInput) =>
  Stream.unwrap(
    supported.pipe(
      Effect.map((enabled) =>
        enabled
          ? subscribe(WS_METHODS.oversightWatch, input)
          : Stream.fail(new OversightError({ reason: "unsupported" })),
      ),
    ),
  );
export const configure = Effect.fn("Overseer.configure")(function* (
  input: OversightConfigureInput,
) {
  if (!(yield* supported)) return yield* new OversightError({ reason: "unsupported" });
  return yield* requestGuarded(WS_METHODS.oversightConfigure, input);
});
export const unlink = Effect.fn("Overseer.unlink")(function* (input: OversightUnlinkInput) {
  if (!(yield* supported)) return yield* new OversightError({ reason: "unsupported" });
  return yield* requestGuarded(WS_METHODS.oversightUnlink, input);
});

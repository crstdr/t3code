import {
  OversightError,
  WS_METHODS,
  type OversightWatchInput,
  type OversightUnlinkInput,
  type OversightConfigureInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { requestGuarded, subscribe, subscribeDynamic } from "./client.ts";
import type { RpcSession } from "./session.ts";

// Commands may precede the session; wait on the connected config stream.
const supported = subscribe(WS_METHODS.subscribeServerConfig, {}).pipe(
  Stream.filter((event) => event.type === "snapshot"),
  Stream.map((event) => event.config.environment.capabilities.threadOversight === true),
  Stream.runHead,
  Effect.map(Option.contains(true)),
  Effect.mapError(() => new OversightError({ reason: "unavailable" })),
);
const prepareSession = Effect.fn("Overseer.prepareSession")((session: RpcSession) =>
  session.initialConfig.pipe(
    Effect.mapError(() => new OversightError({ reason: "unavailable" })),
    Effect.flatMap((config) =>
      config.environment.capabilities.threadOversight === true
        ? Effect.void
        : Effect.fail(new OversightError({ reason: "unsupported" })),
    ),
  ),
);
export const watch = (input: OversightWatchInput) =>
  subscribeDynamic(WS_METHODS.oversightWatch, (session) =>
    prepareSession(session).pipe(Effect.as(input)),
  );
export const configure = Effect.fn("Overseer.configure")(function* (
  input: OversightConfigureInput,
) {
  if (!(yield* supported)) return yield* new OversightError({ reason: "unsupported" });
  return yield* requestGuarded(WS_METHODS.oversightConfigure, input, prepareSession);
});
export const unlink = Effect.fn("Overseer.unlink")(function* (input: OversightUnlinkInput) {
  if (!(yield* supported)) return yield* new OversightError({ reason: "unsupported" });
  return yield* requestGuarded(WS_METHODS.oversightUnlink, input, prepareSession);
});

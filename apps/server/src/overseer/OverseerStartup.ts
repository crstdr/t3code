import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Feature from "./OverseerFeature.ts";
import * as Launch from "./OverseerLaunchService.ts";

export const recoverLaunches = Effect.gen(function* () {
  if (!(yield* Feature.Enabled)) return;
  const service = yield* Effect.serviceOption(Launch.OverseerLaunchService);
  if (Option.isNone(service)) return yield* new Launch.OverseerLaunchError({ reason: "E8" });
  const summary = yield* service.value.recover;
  if (summary.pending > 0) yield* Effect.logWarning("Incomplete overseer launches remain", summary);
}).pipe(
  Effect.withSpan("server.startup.overseer.launches.recover", {
    attributes: { "startup.phase": "overseer.launches.recover" },
  }),
);

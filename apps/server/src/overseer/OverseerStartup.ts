import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import * as Feature from "./OverseerFeature.ts";
import * as Launch from "./OverseerLaunchService.ts";
import * as Recovery from "./OverseerRecovery.ts";

const recoverLaunches = Effect.gen(function* () {
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

export const recover = recoverLaunches.pipe(
  Effect.catchCauseIf(
    (cause) => !Cause.hasInterruptsOnly(cause),
    (cause) =>
      Effect.logWarning("Overseer launch recovery unavailable; ordinary startup continues", {
        cause,
      }),
  ),
  Effect.andThen(
    Effect.gen(function* () {
      const service = yield* Effect.serviceOption(Recovery.OverseerRecovery);
      if (Option.isSome(service)) yield* service.value.recover;
      else if (yield* Feature.Enabled)
        return yield* new Recovery.RecoveryError({ cause: "missing startup recovery service" });
    }),
  ),
  Effect.catchCauseIf(
    (cause) => !Cause.hasInterruptsOnly(cause),
    (cause) =>
      Effect.logWarning("Overseer mailbox recovery unavailable; ordinary startup continues", {
        cause,
      }),
  ),
  Effect.withSpan("server.startup.overseer.mailboxes.recover"),
);

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Feature from "./OverseerFeature.ts";

class Inside extends Context.Reference<boolean>("t3/overseer/InsideGate", {
  defaultValue: () => false,
}) {}
export class OverseerGate extends Context.Service<
  OverseerGate,
  {
    readonly commit: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    readonly handoff: <A, E, R, E2, R2>(
      check: Effect.Effect<boolean, E2, R2>,
      send: (enqueued: Effect.Effect<void>) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<Option.Option<A>, E | E2, R | R2>;
  }
>()("t3/overseer/OverseerGate") {}

export const layer = Layer.effect(
  OverseerGate,
  Effect.gen(function* () {
    const gate = yield* Semaphore.make(1);
    return OverseerGate.of({
      // Lock order: existing thread lock (if any), feature gate, SQL transaction.
      // No operation holding this gate may acquire a thread lock; send must not
      // perform gated work or await an ingestion fiber before outbound enqueue. Nested SQL-only
      // Store calls inherit Inside rather than re-entering the non-reentrant gate.
      // EventSink already spans commits, including high-frequency token chunks.
      commit: Effect.fnUntraced(function* (effect) {
        if (!(yield* Feature.Enabled) || (yield* Inside)) return yield* effect;
        return yield* gate.withPermits(1)(effect.pipe(Effect.provideService(Inside, true)));
      }),
      handoff: Effect.fn("OverseerGate.handoff")(function* (check, send) {
        if (!(yield* Feature.Enabled)) return Option.none();
        let released = false;
        const release = Effect.suspend(() => {
          if (released) return Effect.void;
          released = true;
          return gate.release(1).pipe(Effect.asVoid);
        }).pipe(Effect.uninterruptible);
        return yield* Effect.acquireUseRelease(
          gate.take(1),
          () =>
            Effect.gen(function* () {
              if (!(yield* check.pipe(Effect.provideService(Inside, true)))) return Option.none();
              return Option.some(yield* send(release));
            }),
          () => release,
        );
      }),
    });
  }),
);

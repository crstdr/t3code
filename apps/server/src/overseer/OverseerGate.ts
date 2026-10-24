import * as Context from "effect/Context";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Feature from "./OverseerFeature.ts";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";

class Changes extends Context.Reference<Set<string> | undefined>("t3/overseer/GateChanges", {
  defaultValue: () => undefined,
}) {}

class Inside extends Context.Reference<boolean>("t3/overseer/InsideGate", {
  defaultValue: () => false,
}) {}
export class OverseerGate extends Context.Service<
  OverseerGate,
  {
    readonly changed: (threadId: string) => Effect.Effect<void>;
    readonly listen: (threadIds: ReadonlyArray<string>) => Effect.Effect<
      {
        readonly queue: Queue.Queue<number>;
        readonly revision: number;
        readonly update: (ids: ReadonlyArray<string>) => void;
      },
      never,
      Scope.Scope
    >;
    readonly revision: Effect.Effect<number>;
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
    let revision = 0;
    const listeners = new Map<string, Set<Queue.Queue<number>>>();
    const publish = (ids: Set<string>) => {
      if (ids.size === 0) return;
      revision++;
      const queues = new Set<Queue.Queue<number>>();
      for (const id of ids) for (const queue of listeners.get(id) ?? []) queues.add(queue);
      for (const queue of queues) Queue.offerUnsafe(queue, revision);
    };
    return OverseerGate.of({
      revision: Effect.sync(() => revision),
      // Source invalidations are a hot path already covered by the surrounding commit span.
      changed: Effect.fnUntraced(function* (id) {
        if (!(yield* Feature.Enabled)) return;
        const changes = yield* Changes;
        if (changes) changes.add(id);
        else publish(new Set([id]));
      }),
      // The caller holds commit while installing dependencies and reading its initial page.
      listen: Effect.fn("OverseerGate.listen")(function* (ids) {
        const queue = yield* Effect.acquireRelease(Queue.sliding<number>(1), Queue.shutdown);
        let current = ids;
        const remove = () => {
          for (const id of current) {
            const set = listeners.get(id);
            set?.delete(queue);
            if (set?.size === 0) listeners.delete(id);
          }
        };
        const update = (next: ReadonlyArray<string>) => {
          remove();
          current = next;
          for (const id of next) {
            let set = listeners.get(id);
            if (!set) listeners.set(id, (set = new Set()));
            set.add(queue);
          }
        };
        yield* Effect.acquireRelease(
          Effect.sync(() => update(ids)),
          () => Effect.sync(remove),
        );
        return { queue, revision, update };
      }),
      // Lock order: thread lock (if any) → feature gate → SQL transaction → publication permit.
      // Commit releases SQL/gate before ordered publication; advisory persistence runs
      // after the publication permit is released. Never acquire the gate while holding
      // that permit, or a thread lock while holding the gate. Native send must not
      // perform gated work or await an ingestion fiber before outbound enqueue. Nested SQL-only
      // Store calls inherit Inside rather than re-entering the non-reentrant gate.
      // EventSink already spans commits, including high-frequency token chunks.
      commit: Effect.fnUntraced(function* (effect) {
        if (!(yield* Feature.Enabled) || (yield* Inside)) return yield* effect;
        const changes = new Set<string>();
        return yield* gate.withPermits(1)(
          effect.pipe(
            Effect.provideService(Inside, true),
            Effect.provideService(Changes, changes),
            // Cancellation can arrive just after SQL COMMIT. Invalidate on interruption too:
            // an occasional unchanged page after rollback is safer than a missed committed update.
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) || Cause.hasInterrupts(exit.cause)
                ? Effect.sync(() => publish(changes))
                : Effect.void,
            ),
          ),
        );
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

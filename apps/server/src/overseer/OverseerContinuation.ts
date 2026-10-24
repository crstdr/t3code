import type { MessageId, RunId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Feature from "./OverseerFeature.ts";

type Source = { readonly runId: RunId; readonly messageId?: MessageId };
type Disposition = "ordinary" | "mailbox_owned";
export class OwnershipError extends Schema.TaggedError<OwnershipError>()("OverseerOwnershipError", {
  cause: Schema.Defect(),
}) {}
export class OverseerContinuation extends Context.Service<
  OverseerContinuation,
  {
    readonly disposition: (source: Source) => Effect.Effect<Disposition, OwnershipError>;
  }
>()("t3/overseer/OverseerContinuation") {}

const unavailable = Effect.fn("OverseerContinuation.unavailable")(function* (source: Source) {
  if (source.messageId?.startsWith("message:overseer:")) return "mailbox_owned" as const;
  if (source.messageId !== undefined || !(yield* Feature.Enabled)) return "ordinary" as const;
  return yield* new OwnershipError({ cause: "missing ownership service" });
});

// Optional dependencies must be captured while their layer is being constructed,
// not looked up later from a worker's unrelated execution context.
export const capture = Effect.map(Effect.serviceOption(OverseerContinuation), (service) =>
  Option.isSome(service) ? service.value : OverseerContinuation.of({ disposition: unavailable }),
);

export const disposition = Effect.fn("OverseerContinuation.disposition")(function* (
  source: Source,
) {
  if (source.messageId !== undefined && !source.messageId.startsWith("message:overseer:"))
    return "ordinary" as const;
  const service = yield* Effect.serviceOption(OverseerContinuation);
  if (Option.isSome(service)) return yield* service.value.disposition(source);
  return yield* unavailable(source);
}, Effect.orDie);

export const layer = Layer.effect(
  OverseerContinuation,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return OverseerContinuation.of({
      disposition: Effect.fn("OverseerContinuation.lookup")(
        function* (source) {
          const message =
            source.messageId ??
            (yield* sql<{ readonly message: string | null }>`
        SELECT json_extract(payload_json, '$.userMessageId') AS message
        FROM orchestration_v2_projection_runs WHERE run_id = ${source.runId}`)[0]?.message;
          if (!message?.startsWith("message:overseer:")) return "ordinary" as const;
          const transport = (yield* sql<{
            readonly phase: string;
          }>`SELECT phase FROM overseer_transports
        WHERE message_id = ${message}`)[0];
          return transport?.phase === "received"
            ? ("ordinary" as const)
            : ("mailbox_owned" as const);
        },
        Effect.mapError((cause) => new OwnershipError({ cause })),
      ),
    });
  }),
);

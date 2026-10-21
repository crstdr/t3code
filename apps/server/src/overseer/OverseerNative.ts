import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import type * as Option from "effect/Option";
import type { TransportError, OverseerTransport } from "./OverseerTransport.ts";
import type { MessageId, RunId, RunAttemptId } from "@t3tools/contracts";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";

export type Observation = {
  readonly handoff: <A, E, R>(
    uuid: string | undefined,
    send: (enqueued: Effect.Effect<void>) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<Option.Option<A>, E | TransportError, R>;
  readonly received: (
    kind: "codex_response" | "claude_echo" | "claude_started",
    uuid?: string,
  ) => Effect.Effect<void>;
};
export function observation(
  transport: OverseerTransport["Service"],
  input: {
    readonly messageId: MessageId;
    readonly runId: RunId;
    readonly attemptId: RunAttemptId;
    readonly providerSessionId: string;
  },
): Observation | undefined {
  if (!input.messageId.startsWith("message:overseer:")) return undefined;
  return {
    handoff: (nativeUuid, send) =>
      transport.handoff({ ...input, ...(nativeUuid === undefined ? {} : { nativeUuid }) }, send),
    received: (kind, nativeUuid) =>
      transport
        .receipt({ ...input, kind, ...(nativeUuid === undefined ? {} : { nativeUuid }) })
        .pipe(
          // Retry only persistence of already-matched evidence, never native input.
          Effect.retry(Schedule.recurs(2)),
          Effect.asVoid,
          Effect.catchCause((cause) =>
            Effect.logError("Overseer native receipt remains pending", {
              messageId: input.messageId,
              cause,
            }),
          ),
        ),
  };
}

export class Observer extends Context.Reference<Observation | undefined>(
  "t3/overseer/OverseerNative",
  {
    defaultValue: (): Observation | undefined => undefined,
  },
) {}

// Private control flow: recognized before persisting a provider failure or agent text.
export class Fenced extends Schema.TaggedError<Fenced>()("OverseerDeliveryFenced", {}) {}
const isFencedError = Schema.is(Fenced);
export function fencedTerminal(
  cause: unknown,
  terminal: Extract<ProviderAdapterV2Event, { readonly type: "turn.terminal" }>,
) {
  if (!isFenced(cause)) return terminal;
  return {
    type: "turn.terminal" as const,
    driver: terminal.driver,
    providerThreadId: terminal.providerThreadId,
    providerTurnId: terminal.providerTurnId,
    runOrdinal: terminal.runOrdinal,
    status: "cancelled" as const,
    failure: null,
    threadDisposition: terminal.threadDisposition,
  };
}
export function isFenced(cause: unknown): boolean {
  let current = cause;
  for (let depth = 0; depth < 8; depth++) {
    if (isFencedError(current)) return true;
    if (typeof current !== "object" || current === null || !("cause" in current)) return false;
    current = current.cause;
  }
  return false;
}

import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import { WS_METHODS } from "@t3tools/contracts";

export const rpcTags = [WS_METHODS.oversightWatch, WS_METHODS.oversightUnlink] as const;

export class Enabled extends Context.Reference<boolean>("t3/overseer/Enabled", {
  defaultValue: () => false,
}) {}

// Startup-only: ServerSettings is a public RPC schema, not a private experimental gate.
export const layer = Layer.effect(
  Enabled,
  Config.Boolean("T3CODE_OVERSEER_ENABLED").pipe(Config.withDefault(false)),
);

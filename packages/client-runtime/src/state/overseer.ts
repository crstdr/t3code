import { WS_METHODS } from "@t3tools/contracts";
import type { Atom } from "effect/reactivity";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import * as Overseer from "../rpc/overseer.ts";
import { createEnvironmentSubscriptionAtomFamily, createEnvironmentRpcCommand } from "./runtime.ts";

export function createOverseerEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    watch: createEnvironmentSubscriptionAtomFamily(runtime, {
      label: "environment-data:oversight:watch",
      idleTtlMs: 0,
      subscribe: Overseer.watch,
    }),
    configure: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:oversight:configure",
      tag: WS_METHODS.oversightConfigure,
      execute: Overseer.configure,
    }),
    unlink: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:oversight:unlink",
      tag: WS_METHODS.oversightUnlink,
      execute: Overseer.unlink,
    }),
  };
}

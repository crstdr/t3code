import { createOverseerEnvironmentAtoms } from "@t3tools/client-runtime/state/overseer";
import { connectionAtomRuntime } from "../connection/runtime";

export const overseerEnvironment = createOverseerEnvironmentAtoms(connectionAtomRuntime);

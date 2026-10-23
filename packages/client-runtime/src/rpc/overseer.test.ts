import { it, expect } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import * as Stream from "effect/Stream";
import { ThreadId, WS_METHODS, type OversightConfigureInput } from "@t3tools/contracts";
import * as Overseer from "./overseer.ts";

const fixture = vi.hoisted(() => ({
  supported: undefined as boolean | undefined,
  watches: [] as Array<{ tag: string; input: unknown }>,
  writes: [] as Array<{ tag: string; input: unknown }>,
}));
vi.mock("./client.ts", () => ({
  getInitialServerConfig: () =>
    Effect.sync(() => ({
      environment: {
        capabilities: fixture.supported === undefined ? {} : { threadOversight: fixture.supported },
      },
    })),
  subscribe: (tag: string, input: unknown) => {
    fixture.watches.push({ tag, input });
    return Stream.empty;
  },
  requestGuarded: (tag: string, input: OversightConfigureInput) =>
    Effect.sync(() => {
      fixture.writes.push({ tag, input });
      return tag === WS_METHODS.oversightConfigure
        ? input.preferences
        : { linkId: "link", closed: true };
    }),
}));

it.effect(
  "an old server gets zero feature RPCs; supported servers get the exact tags and payloads",
  () =>
    Effect.gen(function* () {
      fixture.supported = undefined;
      fixture.watches = [];
      fixture.writes = [];
      const threadId = ThreadId.make("thread");
      const preferences = { idleEnabled: false };
      expect(yield* Stream.runDrain(Overseer.watch({ threadId })).pipe(Effect.flip)).toMatchObject({
        reason: "unsupported",
      });
      expect(yield* Overseer.unlink({ linkId: "link" }).pipe(Effect.flip)).toMatchObject({
        reason: "unsupported",
      });
      expect(yield* Overseer.configure({ threadId, preferences }).pipe(Effect.flip)).toMatchObject({
        reason: "unsupported",
      });
      expect(fixture.watches).toEqual([]);
      expect(fixture.writes).toEqual([]);
      fixture.supported = true;
      yield* Stream.runDrain(Overseer.watch({ threadId }));
      yield* Overseer.unlink({ linkId: "link" });
      yield* Overseer.configure({ threadId, preferences });
      expect(fixture.watches).toEqual([{ tag: WS_METHODS.oversightWatch, input: { threadId } }]);
      expect(fixture.writes).toEqual([
        { tag: WS_METHODS.oversightUnlink, input: { linkId: "link" } },
        { tag: WS_METHODS.oversightConfigure, input: { threadId, preferences } },
      ]);
    }).pipe(Effect.provideService(EnvironmentSupervisor, {} as EnvironmentSupervisor["Service"])),
);

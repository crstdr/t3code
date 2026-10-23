import { it, expect } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import * as Stream from "effect/Stream";
import { ThreadId } from "@t3tools/contracts";
import * as Overseer from "./overseer.ts";

const fixture = vi.hoisted(() => ({
  supported: undefined as boolean | undefined,
  watches: 0,
  unlinks: 0,
}));
vi.mock("./client.ts", () => ({
  getInitialServerConfig: () =>
    Effect.sync(() => ({
      environment: {
        capabilities: fixture.supported === undefined ? {} : { threadOversight: fixture.supported },
      },
    })),
  subscribe: () => {
    fixture.watches++;
    return Stream.empty;
  },
  requestGuarded: () =>
    Effect.sync(() => {
      fixture.unlinks++;
      return { linkId: "link", closed: true };
    }),
}));

it.effect("an old server with no capability is never sent a feature RPC", () =>
  Effect.gen(function* () {
    fixture.supported = undefined;
    fixture.watches = 0;
    fixture.unlinks = 0;
    expect(
      yield* Stream.runDrain(Overseer.watch({ threadId: ThreadId.make("thread") })).pipe(
        Effect.flip,
      ),
    ).toMatchObject({ _tag: "OversightError", reason: "unsupported" });
    expect(yield* Overseer.unlink({ linkId: "link" }).pipe(Effect.flip)).toMatchObject({
      _tag: "OversightError",
      reason: "unsupported",
    });
    expect(fixture.watches).toBe(0);
    expect(fixture.unlinks).toBe(0);
    fixture.supported = true;
    yield* Stream.runDrain(Overseer.watch({ threadId: ThreadId.make("thread") }));
    yield* Overseer.unlink({ linkId: "link" });
    expect(fixture.watches).toBe(1);
    expect(fixture.unlinks).toBe(1);
    // The RPC facade is mocked above; accessing this unused supervisor is a test defect.
  }).pipe(Effect.provideService(EnvironmentSupervisor, {} as EnvironmentSupervisor["Service"])),
);

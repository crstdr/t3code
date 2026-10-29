import { RegistryContext } from "@effect/atom-react";
import { useImperativeHandle, useRef, type ReactNode } from "react";
import { CommandPalette } from "../CommandPalette";
import type { ChatComposerHandle } from "../chat/ChatComposer";
import { useComposerHandleContext } from "../../composerHandleContext";
import { openCommandPalette } from "../../commandPaletteBus";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ThreadId,
  ProjectId,
  type OversightLink,
  type OversightWatchFrame,
  type OversightConfigureInput,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import "~/index.css";

type FrameAtom = ReturnType<typeof Atom.make<AsyncResult.AsyncResult<OversightWatchFrame, Error>>>;
const state = vi.hoisted(() => ({
  capabilities: new Map<string, boolean | undefined>(),
  frames: new Map<string, FrameAtom>(),
  queries: new Map<string, Atom.Atom<AsyncResult.AsyncResult<OversightWatchFrame, Error>>>(),
  permissions: new Map<string, ReturnType<typeof Atom.make<boolean>>>(),
  active: new Set<string>(),
  watch: vi.fn(),
  unlink: vi.fn(),
  configure: vi.fn(),
  unexpectedCommand: vi.fn(),
  restoredFocus: vi.fn(),
}));
const key = (environment: string, cursor?: string) => `${environment}:${cursor ?? "first"}`;
vi.mock("../../state/entities", () => ({
  useServerConfigs: () =>
    new Map(
      [...state.capabilities].map(([id, enabled]) => [
        id,
        {
          environment: { capabilities: enabled === undefined ? {} : { threadOversight: enabled } },
        },
      ]),
    ),
  useProjects: () => [],
  useThreadShells: () => [],
  waitForProject: state.unexpectedCommand,
  useProject: (ref: { environmentId: string; projectId: string }) => ({
    title: `${ref.environmentId} ${ref.projectId === "project:b" ? "Project B" : "Project A"}`,
  }),
}));
vi.mock("../../state/overseer", () => ({
  overseerEnvironment: {
    watch: (input: { environmentId: string; input: { threadId: string; cursor?: string } }) => {
      state.watch(input);
      return state.queries.get(`${input.environmentId}:${input.input.cursor ?? "first"}`)!;
    },
    unlink: {
      kind: "unlink",
      permissionAtom: (environment: string) => state.permissions.get(environment)!,
    },
    configure: {
      kind: "configure",
      permissionAtom: (environment: string) => state.permissions.get(environment)!,
    },
  },
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: { kind: string }) =>
    command.kind === "unlink"
      ? state.unlink
      : command.kind === "configure"
        ? state.configure
        : state.unexpectedCommand,
}));
vi.mock("../../hooks/useHandleNewThread", () => ({
  useHandleNewThread: () => ({
    activeThread: { environmentId, id: threadId, projectId: ProjectId.make("project:a") },
    activeDraftThread: null,
    defaultProjectRef: null,
    handleNewThread: state.unexpectedCommand,
  }),
}));
vi.mock("../../hooks/useThreadRouteTarget", () => ({
  useThreadRouteTarget: () => ({
    kind: "server",
    threadRef: scopeThreadRef(environmentId, threadId),
  }),
}));
vi.mock("../../hooks/useOpenPanelPullRequestUrl", () => ({
  useOpenPanelPullRequestUrl: () => null,
}));
vi.mock("../../hooks/useSettings", () => ({
  useClientSettings: () => ({ confirmThreadDelete: true, projectSortOrder: "name" }),
}));
vi.mock("../../hooks/useTheme", () => ({
  useTheme: () => ({ theme: "default", resolvedTheme: "light", appearanceMode: "light" }),
}));
vi.mock("../../hooks/useScratchProject", () => ({
  useScratchProject: () => ({ scratchEnvironmentId: () => null }),
}));
vi.mock("../../hooks/useNewProject", () => ({ useNewProject: () => state.unexpectedCommand }));
vi.mock("../../state/environments", () => ({
  useEnvironments: () => ({ environments: [] }),
  usePrimaryEnvironmentId: () => environmentId,
  useEnvironmentProviders: () => new Map(),
}));
vi.mock("../../connection/useDesktopLocalBootstraps", () => ({
  useDesktopLocalBootstraps: () => [],
}));
vi.mock("../../state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => state.unexpectedCommand,
}));
vi.mock("../settings/useAvailableSettingsSearchItems", () => ({
  useAvailableSettingsSearchItems: () => [],
}));
vi.mock("../CommandPaletteLookups", () => ({
  selectCommandPaletteClientSettings: () => ({}),
  useEnvironmentLookups: () => ({
    environmentLabelById: new Map(),
    projectEnvironmentLocationById: new Map(),
  }),
  useProjectLookups: () => ({
    projectCwdById: new Map(),
    projectByKey: new Map(),
    projectTitleById: new Map(),
  }),
  useSortedProjectGroups: () => [],
}));
vi.mock("../CommandPaletteThreadItems", () => ({
  useCommandPaletteThreadItems: () => ({ threadItems: [], isSearchPending: false }),
}));
vi.mock("../CommandPaletteThemeItems", () => ({
  APPEARANCE_OPTIONS: [],
  notifyThemeSaveFailure: state.unexpectedCommand,
  useCommandPaletteThemeItems: () => {
    const item = (title: string) => ({
      kind: "action",
      value: title,
      title,
      searchTerms: [title],
      icon: null,
      run: state.unexpectedCommand,
    });
    return {
      changeThemeItem: item("Theme"),
      changeAppearanceItem: item("Appearance"),
      themeEditorItem: item("Theme editor"),
    };
  },
}));
vi.mock("../files/ProjectFilePicker", () => ({ ProjectFilePicker: () => null }));
vi.mock("../search/ProjectContentSearchDialog", () => ({ ProjectContentSearchDialog: () => null }));
vi.mock("../pullRequest/LinkPullRequestDialog", () => ({ openLinkPullRequestDialog: () => {} }));
import { ThreadOversightLinks } from "./ThreadOversightLinks";
import { oversightPaletteItem } from "./paletteItem";

const environmentId = EnvironmentId.make("env-one");
const secondEnvironment = EnvironmentId.make("env-two");
const threadId = ThreadId.make("same-id");
const defaults = {
  autoWake: true,
  cadenceEnabled: false,
  cadenceMinutes: 5,
  idleEnabled: false,
  idleMinutes: 30,
} as const;
function link(overrides: Partial<OversightLink> = {}): OversightLink {
  return {
    linkId: "opaque",
    observerThreadId: threadId,
    targetThreadId: ThreadId.make("worker"),
    projectId: ProjectId.make("project:b"),
    title: "Worker B",
    workerStatus: "running",
    pendingRequestCount: 0,
    deliveryState: "pending",
    ...overrides,
  };
}
function setup(links: ReadonlyArray<OversightLink> = [link()]) {
  state.capabilities = new Map([
    [environmentId, true],
    [secondEnvironment, true],
  ]);
  state.frames = new Map();
  state.queries = new Map();
  state.permissions = new Map();
  state.active = new Set();
  state.watch.mockReset();
  state.unlink.mockReset();
  state.configure.mockReset();
  state.unexpectedCommand.mockReset();
  state.restoredFocus.mockReset();
  const registry = AtomRegistry.make();
  const addPage = (environment: string, cursor: string | undefined, frame: OversightWatchFrame) => {
    const id = key(environment, cursor);
    const atom = Atom.make<AsyncResult.AsyncResult<OversightWatchFrame, Error>>(
      AsyncResult.success(frame),
    );
    state.frames.set(id, atom);
    registry.mount(atom);
    state.queries.set(
      id,
      Atom.make((get) => {
        state.active.add(id);
        get.addFinalizer(() => state.active.delete(id));
        return get(atom);
      }).pipe(Atom.setIdleTTL(0)),
    );
  };
  for (const environment of [environmentId, secondEnvironment]) {
    addPage(environment, undefined, { revision: 0, links, preferences: defaults });
    const permission = Atom.make(true);
    registry.mount(permission);
    state.permissions.set(environment, permission);
  }
  const publish = (frame: OversightWatchFrame, environment = environmentId, cursor?: string) =>
    registry.set(state.frames.get(key(environment, cursor))!, AsyncResult.success(frame));
  state.unlink.mockResolvedValue(AsyncResult.success({ closed: true }));
  state.configure.mockImplementation(
    async ({
      environmentId: environment,
      input,
    }: {
      environmentId: EnvironmentId;
      input: OversightConfigureInput;
    }) => {
      const current = registry.get(state.frames.get(key(environment))!);
      if (current._tag !== "Success") throw new Error("Missing fixture frame");
      const preferences = { ...defaults, ...current.value.preferences, ...input.preferences };
      publish({ ...current.value, revision: current.value.revision + 1, preferences }, environment);
      return AsyncResult.success(preferences);
    },
  );
  return {
    registry,
    addPage,
    publish,
    ui: (environment = environmentId) => (
      <RegistryContext value={registry}>
        <ThreadOversightLinks environmentId={environment} threadId={threadId} />
      </RegistryContext>
    ),
    [Symbol.dispose]: () => registry.dispose(),
  };
}

it("capability absent renders nothing, sends zero feature requests, and contributes no palette entry", async () => {
  using fixture = setup();
  state.capabilities.set(environmentId, undefined);
  await using screen = await render(fixture.ui());
  expect(screen.container.childElementCount).toBe(0);
  expect(state.watch).not.toHaveBeenCalled();
  expect(state.unlink).not.toHaveBeenCalled();
  expect(state.configure).not.toHaveBeenCalled();
  expect(oversightPaletteItem(scopeThreadRef(environmentId, threadId), {})).toBeNull();
  expect(oversightPaletteItem(null, { threadOversight: true })).toBeNull();
});

it("shows outgoing, incoming, and both roles on live revision updates", async () => {
  using fixture = setup();
  await using screen = await render(fixture.ui());
  await expect.element(screen.getByRole("img", { name: "Overseer", exact: true })).toBeVisible();
  expect(screen.getByRole("img", { name: "Worker", exact: true }).query()).toBeNull();
  const incoming = link({
    linkId: "incoming",
    observerThreadId: ThreadId.make("parent"),
    targetThreadId: threadId,
    title: "Overseer A",
    projectId: ProjectId.make("project:a"),
  });
  await act(() => fixture.publish({ revision: 1, links: [incoming], preferences: defaults }));
  await expect.element(screen.getByRole("img", { name: "Worker", exact: true })).toBeVisible();
  expect(screen.getByRole("img", { name: "Overseer", exact: true }).query()).toBeNull();
  await act(() =>
    fixture.publish({ revision: 2, links: [incoming, link()], preferences: defaults }),
  );
  await expect.element(screen.getByRole("img", { name: "Overseer", exact: true })).toBeVisible();
  await expect.element(screen.getByRole("img", { name: "Worker", exact: true })).toBeVisible();
});

it("navigates A to B and the incoming backlink in the selected environment, including ID collisions", async () => {
  using fixture = setup();
  await using screen = await render(fixture.ui(), { router: "/env-one/same-id" });
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect.element(page.getByText("Worker · env-one Project B")).toBeVisible();
  await userEvent.click(page.getByRole("button", { name: "Worker B", exact: true }));
  await vi.waitFor(() => expect(screen.router?.state.location.pathname).toBe("/env-one/worker"));
  const incoming = link({
    observerThreadId: ThreadId.make("parent"),
    targetThreadId: threadId,
    title: "Overseer A",
    projectId: ProjectId.make("project:a"),
  });
  await act(() =>
    fixture.publish({ revision: 1, links: [incoming], preferences: defaults }, secondEnvironment),
  );
  await screen.rerender(fixture.ui(secondEnvironment));
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect.element(page.getByText("Overseen by · env-two Project A")).toBeVisible();
  await userEvent.click(page.getByRole("button", { name: "Overseer A", exact: true }));
  await vi.waitFor(() => expect(screen.router?.state.location.pathname).toBe("/env-two/parent"));
  expect(state.watch.mock.calls.at(-1)?.[0].environmentId).toBe(secondEnvironment);
});

it("palette opens an empty panel only for the matching scoped thread", async () => {
  using fixture = setup([]);
  await using screen = await render(fixture.ui());
  const wrong = oversightPaletteItem(scopeThreadRef(secondEnvironment, threadId), {
    threadOversight: true,
  })!;
  await act(() => wrong.run());
  expect(page.getByRole("dialog", { name: "Oversight links" }).query()).toBeNull();
  expect(screen.getByRole("button", { name: "Show oversight links" }).query()).toBeNull();
  expect(screen.container.childElementCount).toBe(0);
  const item = oversightPaletteItem(scopeThreadRef(environmentId, threadId), {
    threadOversight: true,
  })!;
  await act(() => item.run());
  await expect.element(page.getByText("No oversight links.")).toBeVisible();
  await userEvent.keyboard("{Escape}");
  await vi.waitFor(() => expect(page.getByText("No oversight links.").query()).toBeNull());
});

function PaletteComposer({ children }: { readonly children: ReactNode }) {
  const handle = useComposerHandleContext();
  const composer = useRef<HTMLInputElement>(null);
  useImperativeHandle(
    handle,
    () =>
      ({
        focusAtEnd: () => {
          composer.current?.focus();
          state.restoredFocus();
        },
        isModelPickerOpen: () => false,
      }) satisfies Pick<
        ChatComposerHandle,
        "focusAtEnd" | "isModelPickerOpen"
      > as ChatComposerHandle,
    [],
  );
  return (
    <>
      {children}
      <input aria-label="Composer" ref={composer} />
      <button onClick={() => openCommandPalette()}>Open palette</button>
    </>
  );
}

function PaletteFocusShell({ children }: { readonly children: ReactNode }) {
  return (
    <CommandPalette>
      <PaletteComposer>{children}</PaletteComposer>
    </CommandPalette>
  );
}

it("the palette modal can close and return composer focus without dismissing the new panel", async () => {
  using fixture = setup();
  await using screen = await render(<PaletteFocusShell>{fixture.ui()}</PaletteFocusShell>);
  await userEvent.click(screen.getByRole("button", { name: "Open palette", exact: true }));
  await expect
    .element(page.getByRole("dialog", { name: "Command palette", exact: true }))
    .toBeVisible();
  await userEvent.click(
    page
      .getByRole("dialog", { name: "Command palette", exact: true })
      .getByRole("option", { name: "Show oversight links" }),
  );
  await expect
    .element(page.getByRole("dialog", { name: "Oversight links", exact: true }))
    .toBeVisible();
  await userEvent.click(page.getByRole("switch", { name: "Auto-wake on all updates" }));
  await expect
    .element(page.getByRole("switch", { name: "Auto-wake on all updates" }))
    .not.toBeChecked();
  await userEvent.click(screen.getByRole("textbox", { name: "Composer" }));
  await expect
    .element(page.getByRole("dialog", { name: "Oversight links" }))
    .not.toBeInTheDocument();
});

it.each([
  ["pointer", "title"],
  ["pointer", "cleared"],
  ["pointer", "unfiltered"],
  ["Enter", "title"],
  ["Enter", "cleared"],
  ["Enter", "unfiltered"],
] as const)(
  "production palette opens a linkless thread with %s in the %s view and keeps preferences absent",
  async (selection, query) => {
    using fixture = setup([]);
    fixture.publish({ revision: 0, links: [] });
    await using screen = await render(<PaletteFocusShell>{fixture.ui()}</PaletteFocusShell>);
    expect(screen.getByRole("button", { name: "Show oversight links" }).query()).toBeNull();
    await userEvent.click(screen.getByRole("button", { name: "Open palette", exact: true }));
    const palette = page.getByRole("dialog", { name: "Command palette", exact: true });
    const input = palette.getByRole("combobox");
    if (query !== "unfiltered") await userEvent.fill(input, "Show oversight links");
    if (query === "cleared") await userEvent.fill(input, "");
    await expect.element(input).toHaveFocus();
    await expect
      .element(palette.getByRole("option", { name: "Show oversight links" }))
      .toBeVisible();
    if (selection === "pointer") {
      await userEvent.click(palette.getByRole("option", { name: "Show oversight links" }));
    } else {
      await userEvent.keyboard("{Enter}");
    }
    await vi.waitFor(() => expect(state.restoredFocus).toHaveBeenCalledTimes(1));
    await expect.element(palette).not.toBeInTheDocument();
    await expect.element(page.getByText("No oversight links.")).toBeVisible();
    expect(page.getByRole("switch").query()).toBeNull();
    expect(state.configure).not.toHaveBeenCalled();
    expect(state.unexpectedCommand).not.toHaveBeenCalled();
    expect([...state.active]).toEqual([key(environmentId)]);
    await expect
      .element(screen.getByRole("button", { name: "Show oversight links" }))
      .toBeVisible();
    await expect.element(page.getByText("No oversight links.")).toBeVisible();
    await userEvent.keyboard("{Escape}");
    await expect.element(page.getByText("No oversight links.")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show oversight links" }).query()).toBeNull();
  },
);

it("pages without accumulating history and releases the extra page watch on close", async () => {
  using fixture = setup();
  fixture.publish({ revision: 0, links: [link()], nextCursor: "page-2", preferences: defaults });
  fixture.addPage(environmentId, "page-2", {
    revision: 0,
    links: [link({ linkId: "other", title: "Worker C" })],
    preferences: defaults,
  });
  await using screen = await render(fixture.ui());
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await userEvent.click(page.getByRole("button", { name: "Next", exact: true }));
  await expect.element(page.getByRole("button", { name: "Worker C", exact: true })).toBeVisible();
  expect(page.getByRole("button", { name: "Worker B", exact: true }).query()).toBeNull();
  expect(state.active.has(key(environmentId, "page-2"))).toBe(true);
  await userEvent.click(page.getByRole("button", { name: "Previous", exact: true }));
  await expect.element(page.getByRole("button", { name: "Worker B", exact: true })).toBeVisible();
  await userEvent.click(page.getByRole("button", { name: "Next", exact: true }));
  await userEvent.keyboard("{Escape}");
  await vi.waitFor(() => expect(state.active.has(key(environmentId, "page-2"))).toBe(false));
  expect(state.active.has(key(environmentId))).toBe(true);
  await screen.unmount();
  await vi.waitFor(() => expect(state.active.size).toBe(0));
});

it("renders reserved/deleted and held/error states separately and makes unavailable targets non-navigable", async () => {
  using fixture = setup([
    link({ workerStatus: "reserved", deliveryState: "held", pendingRequestCount: 2 }),
    link({
      linkId: "deleted",
      title: "Deleted worker",
      workerStatus: "deleted",
      deliveryState: "error",
    }),
  ]);
  await using screen = await render(fixture.ui());
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect.element(page.getByRole("button", { name: "Worker B", exact: true })).toBeDisabled();
  await expect
    .element(page.getByRole("button", { name: "Deleted worker", exact: true }))
    .toBeDisabled();
  await expect.element(page.getByText("reserved · 2 pending questions")).toBeVisible();
  await expect.element(page.getByText("Delivery: held", { exact: true })).toBeVisible();
  await expect.element(page.getByText("Delivery: error", { exact: true })).toBeVisible();
});

it("read-only permission disables unlink and preferences; unlink uses only the scoped unlink command", async () => {
  using fixture = setup();
  fixture.registry.set(state.permissions.get(environmentId)!, false);
  await using screen = await render(fixture.ui());
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect.element(page.getByRole("button", { name: "Unlink Worker B" })).toBeDisabled();
  await expect
    .element(page.getByRole("switch", { name: "Auto-wake on all updates" }))
    .toBeDisabled();
  await act(() => fixture.registry.set(state.permissions.get(environmentId)!, true));
  await userEvent.click(page.getByRole("button", { name: "Unlink Worker B" }));
  expect(state.unlink).toHaveBeenCalledExactlyOnceWith({
    environmentId,
    input: { linkId: "opaque" },
  });
  expect(state.configure).not.toHaveBeenCalled();
  await expect.element(page.getByText("Unlink stops notifications, not the worker.")).toBeVisible();
});

it("preferences controls round-trip the approved defaults and all three settings", async () => {
  using fixture = setup();
  await using screen = await render(fixture.ui());
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect
    .element(page.getByRole("switch", { name: "Auto-wake on all updates" }))
    .toBeChecked();
  await expect
    .element(page.getByRole("switch", { name: "Routine wake-ups no more often than" }))
    .not.toBeChecked();
  await expect
    .element(page.getByRole("switch", { name: "Wake when idle for", exact: true }))
    .not.toBeChecked();
  await userEvent.click(page.getByRole("switch", { name: "Auto-wake on all updates" }));
  await expect
    .element(page.getByRole("switch", { name: "Auto-wake on all updates" }))
    .not.toBeChecked();
  await userEvent.click(page.getByRole("switch", { name: "Routine wake-ups no more often than" }));
  await userEvent.click(page.getByRole("combobox", { name: "Routine wake-up interval" }));
  await userEvent.click(page.getByRole("option", { name: "15 min", exact: true }));
  await userEvent.click(page.getByRole("switch", { name: "Wake when idle for", exact: true }));
  await userEvent.click(page.getByRole("combobox", { name: "Idle wake-up interval" }));
  await userEvent.click(page.getByRole("option", { name: "120 min", exact: true }));
  await expect
    .element(page.getByRole("combobox", { name: "Idle wake-up interval" }))
    .toHaveTextContent("120 min");
  expect(state.configure.mock.calls.at(-1)?.[0]).toEqual({
    environmentId,
    input: {
      threadId,
      preferences: { idleMinutes: 120 },
    },
  });
});

it("keeps idle controls reachable after the last unlink and captures the shared panel", async () => {
  using fixture = setup();
  await using screen = await render(fixture.ui());
  const wasDark = document.documentElement.classList.contains("dark");
  using _theme = {
    [Symbol.dispose]: () => document.documentElement.classList.toggle("dark", wasDark),
  };
  document.documentElement.classList.remove("dark");
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  await expect.element(page.getByRole("dialog", { name: "Oversight links" })).toBeVisible();
  await expect
    .element(page.getByRole("dialog", { name: "Oversight links" }))
    .toHaveStyle({ opacity: "1" });
  await page.screenshot({ path: "__screenshots__/slice5-panel-light.png" });
  document.documentElement.classList.add("dark");
  await page.screenshot({ path: "__screenshots__/slice5-panel-dark.png" });
  await act(() =>
    fixture.publish({ revision: 2, links: [], preferences: { ...defaults, idleEnabled: true } }),
  );
  await expect
    .element(page.getByRole("switch", { name: "Wake when idle for", exact: true }))
    .toBeChecked();
  await userEvent.click(page.getByRole("switch", { name: "Wake when idle for", exact: true }));
  expect(state.configure.mock.calls.at(-1)?.[0].input.preferences.idleEnabled).toBe(false);
});

it("shows load and mutation failures without hiding the worker state", async () => {
  using fixture = setup();
  await using screen = await render(fixture.ui());
  await userEvent.click(screen.getByRole("button", { name: "Show oversight links" }));
  state.unlink.mockResolvedValue(AsyncResult.failure(Cause.fail(new Error("denied"))));
  await userEvent.click(page.getByRole("button", { name: "Unlink Worker B" }));
  await expect.element(page.getByRole("alert")).toHaveTextContent("Could not unlink thread.");
  await expect.element(page.getByText("running", { exact: true })).toBeVisible();
  await act(() =>
    fixture.registry.set(
      state.frames.get(key(environmentId))!,
      AsyncResult.failure(Cause.fail(new Error("disconnected"))),
    ),
  );
  await expect
    .element(page.getByText("Could not load oversight links.", { exact: false }))
    .toBeVisible();
});

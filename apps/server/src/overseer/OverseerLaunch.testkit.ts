import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../config.ts";
import * as McpInvocation from "../mcp/McpInvocationContext.ts";
import * as Git from "../git/GitWorkflowService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Clone from "../project/ProjectCloneTracker.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as Tracker from "../project/WorktreeSetupTracker.ts";
import * as Terminal from "../terminal/Manager.ts";
import * as Text from "../textGeneration/TextGeneration.ts";
import * as Vcs from "../vcs/GitVcsDriver.ts";
import * as Settings from "../serverSettings.ts";
import * as RegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as AdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Launch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Store from "./OverseerStore.ts";
import * as Saga from "./OverseerLaunchService.ts";
import * as Feature from "./OverseerFeature.ts";

const projectA = ProjectId.make("project:a");
export const projectB = ProjectId.make("project:b");
export const observer = ThreadId.make("observer");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
export const invocation: McpInvocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("test"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "test",
  client: undefined,
  thread: {
    threadId: observer,
    providerInstanceId: selection.instanceId,
    providerSessionId: "resident",
  },
};
export const request = {
  oversee: true,
  clientRequestId: "retry",
  title: "Worker",
  message: "Work",
} as const;

interface Options {
  readonly database?: ReturnType<typeof Sqlite.layerFromPath>;
  readonly config?: ServerConfig.ServerConfig["Service"];
  readonly worktreeCreated?: Effect.Effect<void>;
  readonly milestone?: (step: string) => Effect.Effect<void, never, SqlClient.SqlClient>;
  readonly runSetup?: Setup.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly scratch?: boolean;
  readonly failPreparation?: boolean;
  readonly providerDriver?: string;
}

export function harness(options: Options = {}) {
  const database = (options.database ?? Sqlite.layerMemory).pipe(Layer.provide(NodeServices.layer));
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId: selection.instanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("No provider process may run in the launch harness"),
  };
  const registry = AdapterRegistry.layerFromAdapters([
    adapter,
    {
      ...adapter,
      instanceId: ProviderInstanceId.make("claudeAgent"),
      driver: ProviderDriverKind.make("claudeAgent"),
      getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
    },
  ]);
  const runtime = Harness.layerWithRegistry({ name: "overseer-launch" }, registry, {
    databaseLayer: database,
    runEffectWorker: false,
  });
  const settings = Settings.layerTest();
  const receipts = Receipts.layer.pipe(Layer.provide(database));
  const config = options.config
    ? Layer.succeed(ServerConfig.ServerConfig, options.config)
    : Layer.effect(
        ServerConfig.ServerConfig,
        Harness.makeReplayServerConfig("overseer-launch"),
      ).pipe(Layer.provide(NodeServices.layer));
  const external = Layer.unwrap(
    Effect.gen(function* () {
      const cfg = yield* ServerConfig.ServerConfig;
      const projects = [projectA, projectB].map((id) => ({
        id,
        title: id,
        workspaceRoot: cfg.stateDir,
        repositoryIdentity: null,
        faviconPath: null,
        defaultModelSelection: selection,
        defaultThreadEnvMode: null,
        scripts: [],
        createdAt: "2026-10-21T00:00:00.000Z",
        updatedAt: "2026-10-21T00:00:00.000Z",
        deletedAt: null,
      }));
      return Layer.mergeAll(
        Tracker.layer,
        Layer.mock(Clone.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
        Layer.mock(Terminal.TerminalManager)({ close: () => Effect.void }),
        Layer.mock(Projects.ProjectService)({
          getById: (id) =>
            Effect.succeed(Option.fromUndefinedOr(projects.find((p) => p.id === id))),
        }),
        Layer.mock(Folders.ManagedProjectFolders)({
          namedProjectsRoot: cfg.stateDir,
          isScratchProject: () => Effect.succeed(options.scratch === true),
          folderForThread: () => Effect.succeed(Option.none()),
          ensureScratchProject: Effect.succeed({
            projectId: projectB,
            workspaceRoot: cfg.stateDir,
          }),
          reservedThreadFolder: ({ threadId }) =>
            Effect.succeed(Option.some(`${cfg.stateDir}/${threadId}`)),
        }),
        Layer.mock(Git.GitWorkflowService)({
          createWorktree: (input) =>
            options.failPreparation
              ? Effect.die("Injected preparation failure")
              : Effect.succeed({
                  worktree: {
                    path: `${cfg.stateDir}/worktree`,
                    refName: input.newRefName ?? input.refName,
                    headSha: "abc",
                  },
                }).pipe(Effect.tap(() => options.worktreeCreated ?? Effect.void)),
          renameBranch: (input) => Effect.succeed({ branch: input.newBranch }),
          fetchWorktreeBaseCommit: () =>
            Effect.succeed({ commitSha: "abc", remoteRefName: "origin/main" }),
          hasCommit: () => Effect.succeed(true),
          resolveWorktreeBaseRemote: () =>
            Effect.succeed({ remoteName: "origin", branchName: "main" }),
          remoteExists: () => Effect.succeed(true),
          remoteBranchExists: () => Effect.succeed(true),
          removeWorktree: () => Effect.void,
        }),
        Layer.mock(Vcs.GitVcsDriver)({ listWorktreePaths: () => Effect.succeed([cfg.stateDir]) }),
        Layer.succeed(Setup.ProjectSetupScriptRunner, {
          runForThread: options.runSetup ?? (() => Effect.succeed({ status: "no-script" })),
        }),
        Layer.mock(Text.TextGeneration)({
          generateThreadTitle: () => Effect.succeed({ title: "Worker" }),
          generateBranchName: () => Effect.succeed({ branch: "worker" }),
        }),
        RegistryMock.layer([
          {
            instanceId: selection.instanceId,
            driver: ProviderDriverKind.make(options.providerDriver ?? "codex"),
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt: "2026-10-21T00:00:00.000Z",
            version: "test",
            models: [],
            slashCommands: [],
            skills: [],
          },
          {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            driver: ProviderDriverKind.make("claudeAgent"),
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt: "2026-10-21T00:00:00.000Z",
            version: "test",
            models: [],
            slashCommands: [],
            skills: [],
          },
        ]),
        settings,
      );
    }),
  ).pipe(Layer.provide(config));
  const rawThreads = Threads.layer.pipe(Layer.provide(Layer.mergeAll(runtime, settings, receipts)));
  const milestone = options.milestone ?? (() => Effect.void);
  const threads = Layer.effect(
    Threads.ThreadManagementService,
    Effect.gen(function* () {
      const service = yield* Threads.ThreadManagementService;
      const sql = yield* SqlClient.SqlClient;
      return {
        ...service,
        dispatch: (command: Parameters<typeof service.dispatch>[0]) =>
          service
            .dispatch(command)
            .pipe(
              Effect.tap(() =>
                !("threadId" in command) || command.threadId === observer
                  ? Effect.void
                  : milestone(command.type).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
              ),
            ),
      };
    }),
  ).pipe(Layer.provide(Layer.merge(rawThreads, database)));
  const launch = Launch.layer.pipe(
    Layer.provide(Layer.mergeAll(external, threads, receipts, IdAllocator.layer)),
  );
  const store = Layer.effect(
    Store.OverseerStore,
    Effect.gen(function* () {
      const service = yield* Store.OverseerStore;
      const sql = yield* SqlClient.SqlClient;
      return {
        ...service,
        reserve: (input: Parameters<typeof service.reserve>[0]) =>
          milestone("before-reservation").pipe(
            Effect.andThen(service.reserve(input)),
            Effect.tap(() => milestone("reservation")),
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
      };
    }),
  ).pipe(Layer.provide(Layer.merge(Store.layer.pipe(Layer.provide(database)), database)));
  const wrappedLaunch = Layer.effect(
    Launch.ThreadLaunchService,
    Effect.gen(function* () {
      const service = yield* Launch.ThreadLaunchService;
      const sql = yield* SqlClient.SqlClient;
      return {
        ...service,
        launch: (input: Launch.ThreadLaunchInput) =>
          service
            .launch(input)
            .pipe(
              Effect.tap(() =>
                input.threadId === observer
                  ? Effect.void
                  : milestone("prepared-or-scheduled").pipe(
                      Effect.provideService(SqlClient.SqlClient, sql),
                    ),
              ),
            ),
      };
    }),
  ).pipe(Layer.provide(Layer.merge(launch, database)));
  const dependencies = Layer.mergeAll(
    store,
    wrappedLaunch,
    threads,
    external,
    receipts,
    config,
    database,
  );
  const saga = Saga.layer.pipe(
    Layer.provide(dependencies),
    Layer.provide(NodeServices.layer),
    Layer.provide(NodeCrypto.layer),
  );
  return Layer.mergeAll(saga, dependencies, runtime).pipe(Layer.provideMerge(NodeServices.layer));
}

export const setup = Effect.fn("test.overseer.setup")(function* (modelSelection = selection) {
  const sql = yield* SqlClient.SqlClient;
  const cfg = yield* ServerConfig.ServerConfig;
  for (const id of [projectA, projectB])
    yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES (${id},${id},${cfg.stateDir},'[]','2026-10-21T00:00:00.000Z','2026-10-21T00:00:00.000Z')`;
  const launch = yield* Launch.ThreadLaunchService;
  yield* launch.launch({
    commandId: CommandId.make("owner:create"),
    threadId: observer,
    projectId: projectA,
    title: "Observer",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    workspaceStrategy: { type: "root" },
    initialMessage: {
      messageId: MessageId.make("owner:initial"),
      text: "Observe",
      attachments: [],
    },
    createdBy: "user",
    creationSource: "web",
  });
  return yield* Saga.OverseerLaunchService;
});

export const enabled = <A, E, R>(effect: Effect.Effect<A, E, R>, scope = invocation) =>
  effect.pipe(
    Effect.provideService(McpInvocation.McpInvocationContext, scope),
    Effect.provideService(Feature.Enabled, true),
  );

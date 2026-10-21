import {
  ChatAttachment,
  CommandId,
  MessageId,
  ModelSelection,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Exit from "effect/Exit";
import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../orchestration-v2/DispatchModeLimit.ts";
import * as Schema from "effect/Schema";
import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import {
  readCaller,
  resolveProjectId,
  assertTargetWithinLimits,
  unavailable,
} from "../mcp/threadAccess.ts";
import { resolveInteractionMode, resolveRuntimeMode } from "../mcp/OrchestratorMcpService.ts";
import * as Claims from "../orchestration-v2/AttachmentClaims.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Intake from "../orchestration-v2/ThreadMessageIntake.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Feature from "./OverseerFeature.ts";
import * as Store from "./OverseerStore.ts";
import { OverseerLaunchParameters } from "./OverseerLaunchInput.ts";
import { assertProjectWorktree } from "./LaunchValidation.ts";
import { launchText } from "./OverseerText.ts";

export class OverseerLaunchError extends Schema.TaggedError<OverseerLaunchError>()(
  "OverseerLaunchError",
  {
    reason: Schema.Literals(["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"]),
    linkId: Schema.optional(Schema.String),
    threadId: Schema.optional(ThreadId),
    projectId: Schema.optional(ProjectId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return launchText[this.reason];
  }
}

export function toMcpFailure(error: OverseerLaunchError) {
  return Object.assign(
    new OrchestratorMcpFailure({
      code:
        error.reason === "E7" || error.reason === "E8" ? "orchestration_error" : "invalid_request",
      message: launchText[error.reason],
    }),
    {
      ...(error.linkId === undefined ? {} : { linkId: error.linkId }),
      ...(error.threadId === undefined ? {} : { threadId: error.threadId }),
      ...(error.projectId === undefined ? {} : { projectId: error.projectId }),
    },
  );
}

type Request = typeof OverseerLaunchParameters.Type;
const DurableInput = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  projectId: ProjectId,
  title: Schema.String,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy,
  scratchFolder: Schema.optional(Schema.String),
  initialMessage: Schema.optional(
    Schema.Struct({
      messageId: MessageId,
      senderThreadId: ThreadId,
      text: Schema.String,
      attachments: Schema.Array(ChatAttachment),
    }),
  ),
});
const DurableJson = Schema.fromJsonString(DurableInput);
const decodeDurable = Schema.decodeEffect(DurableJson);
const encodeDurable = Schema.encodeEffect(DurableJson);
type DurableInput = typeof DurableInput.Type;
type LaunchResult = ThreadLaunch.ThreadLaunchResult & {
  readonly linkId?: string;
  readonly initialMessageId: MessageId;
};

export class OverseerLaunchService extends Context.Service<
  OverseerLaunchService,
  {
    readonly launch: (
      request: Request,
    ) => Effect.Effect<
      LaunchResult,
      OverseerLaunchError | OrchestratorMcpFailure,
      McpInvocationContext.McpInvocationContext
    >;
    readonly recover: Effect.Effect<
      { readonly resumed: number; readonly pending: number },
      OverseerLaunchError
    >;
  }
>()("t3/overseer/OverseerLaunchService") {}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (typeof value === "object" && value !== null)
    return (
      "{" +
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, item]) => JSON.stringify(key) + ":" + canonical(item))
        .join(",") +
      "}"
    );
  return JSON.stringify(value) ?? "null";
}

const make = Effect.gen(function* () {
  const store = yield* Store.OverseerStore;
  const launches = yield* ThreadLaunch.ThreadLaunchService;
  const threads = yield* Threads.ThreadManagementService;
  const projects = yield* Projects.ProjectService;
  const folders = yield* Folders.ManagedProjectFolders;
  const providers = yield* Providers.ProviderRegistry;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const fs = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const config = yield* ServerConfig.ServerConfig;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const locks = yield* KeyedLock.make<string>();
  const digest = Effect.fn("OverseerLaunchService.digest")(function* (value: unknown) {
    return Hex.encode(
      yield* crypto
        .digest("SHA-256", new TextEncoder().encode(canonical(value)))
        .pipe(Effect.orDie),
    );
  });
  const certified = Effect.fn("OverseerLaunchService.certified")(function* (
    instanceId: ModelSelection["instanceId"],
  ) {
    const provider = (yield* providers.getProviders).find(
      (entry) => entry.instanceId === instanceId,
    );
    if (provider?.driver !== "codex" && provider?.driver !== "claudeAgent")
      return yield* new OverseerLaunchError({ reason: "E5" });
  });
  const validateOwner = Effect.fn("OverseerLaunchService.validateOwner")(function* (
    row: Store.LinkRow,
    input?: DurableInput,
  ) {
    const owner = yield* threads
      .getThreadShell(ThreadId.make(row.observer_thread_id))
      .pipe(Effect.mapError(unavailable));
    if (
      !owner ||
      owner.deletedAt !== null ||
      owner.projectId !== row.observer_project_id ||
      owner.lineage.relationshipToParent === "subagent"
    ) {
      return yield* new OverseerLaunchError({ reason: "E6" });
    }
    if (input) {
      yield* assertTargetWithinLimits(owner, input);
      yield* certified(input.modelSelection.instanceId);
      yield* threads
        .assertProviderInstanceEnabled({
          projectId: input.projectId,
          commandId: input.commandId,
          instanceId: input.modelSelection.instanceId,
        })
        .pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
          ),
        );
    }
    return owner;
  });
  const assertReceipt = Effect.fn("OverseerLaunchService.assertReceipt")(function* (
    row: Store.LinkRow,
    commandId: CommandId,
    type: string,
  ) {
    const receipt = yield* receipts.getByCommandId(commandId).pipe(
      Effect.mapError(
        (cause) =>
          new OverseerLaunchError({
            reason: "E7",
            cause,
            linkId: row.link_id,
            threadId: ThreadId.make(row.target_thread_id),
            projectId: ProjectId.make(row.target_project_id),
          }),
      ),
    );
    if (
      Option.isNone(receipt) ||
      receipt.value.status !== "accepted" ||
      receipt.value.threadId !== row.target_thread_id ||
      receipt.value.commandType !== type
    ) {
      return yield* new OverseerLaunchError({
        reason: "E7",
        linkId: row.link_id,
        threadId: ThreadId.make(row.target_thread_id),
        projectId: ProjectId.make(row.target_project_id),
      });
    }
  });
  const resume = Effect.fn("OverseerLaunchService.resume")(function* (
    snapshot: Store.LinkRow,
  ): Effect.fn.Return<LaunchResult, OverseerLaunchError | OrchestratorMcpFailure> {
    const row = yield* store
      .reservationFor(ThreadId.make(snapshot.observer_thread_id), snapshot.client_request_id)
      .pipe(
        Effect.mapError(
          (cause) =>
            new OverseerLaunchError({
              reason: "E7",
              cause,
              linkId: snapshot.link_id,
              threadId: ThreadId.make(snapshot.target_thread_id),
              projectId: ProjectId.make(snapshot.target_project_id),
            }),
        ),
      );
    if (!row) return yield* new OverseerLaunchError({ reason: "E6" });
    if (row.state !== "active") return yield* new OverseerLaunchError({ reason: "E6" });
    yield* validateOwner(row);
    if (row.launch_accepted === 1) {
      yield* assertReceipt(row, CommandId.make(row.launch_command_id), "thread.create");
      const projection = yield* threads
        .getThreadProjection(ThreadId.make(row.target_thread_id))
        .pipe(Effect.mapError(unavailable));
      return {
        threadId: projection.thread.id,
        projection,
        resumed: true,
        linkId: row.link_id,
        initialMessageId: MessageId.make(row.launch_message_id),
      };
    }
    const input = yield* decodeDurable(row.launch_input_json ?? "").pipe(
      Effect.mapError(
        (cause) =>
          new OverseerLaunchError({
            reason: "E7",
            cause,
            linkId: row.link_id,
            threadId: ThreadId.make(row.target_thread_id),
            projectId: ProjectId.make(row.target_project_id),
          }),
      ),
    );
    // Caller-supplied worktrees are mutable filesystem bindings, unlike our reserved Scratch folder.
    if (input.workspaceStrategy.type === "existing_worktree" && !input.scratchFolder) {
      const project = yield* projects.getById(input.projectId).pipe(Effect.mapError(unavailable));
      if (Option.isNone(project)) return yield* new OverseerLaunchError({ reason: "E6" });
      yield* assertProjectWorktree(
        project.value.workspaceRoot,
        input.workspaceStrategy.worktreePath,
      ).pipe(
        Effect.provideService(GitVcsDriver.GitVcsDriver, git),
        Effect.provideService(FileSystem.FileSystem, fs),
      );
    }
    const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
    const { scratchFolder: _scratchFolder, initialMessage, ...base } = input;
    const launchInput: ThreadLaunch.ThreadLaunchInput = {
      ...base,
      ...(initialMessage ? { initialMessage } : {}),
      createdBy: "agent",
      creationSource: "mcp",
    };
    const result = yield* Effect.gen(function* () {
      if (input.scratchFolder) yield* fs.makeDirectory(input.scratchFolder, { recursive: true });
      const messageReceipt = input.initialMessage
        ? yield* receipts.getByCommandId(CommandId.make(`${input.commandId}:initial-message`))
        : Option.none();
      // A receipt means the original claimed attachments are already owned by the message;
      // never require the temporary upload to survive until the caller retries.
      if (Option.isSome(messageReceipt) && input.initialMessage) {
        yield* assertReceipt(
          row,
          CommandId.make(`${input.commandId}:initial-message`),
          "message.dispatch",
        );
        const projection = yield* threads.getThreadProjection(input.threadId);
        yield* assertReceipt(row, input.commandId, "thread.create");
        const run = projection.runs.find(
          (run) => run.userMessageId === input.initialMessage?.messageId,
        );
        if (!run) return yield* new OverseerLaunchError({ reason: "E7" });
        // The accepted run owns preparation; this path reuses any recorded worktree binding.
        yield* launches.prepareRun({
          commandId: input.commandId,
          threadId: input.threadId,
          runId: run.id,
        });
        return { threadId: input.threadId, projection, resumed: true };
      }
      const owner = yield* validateOwner(row, input);
      return yield* Intake.launchThread(launchInput, { recoverReservedClaims: true }).pipe(
        Effect.provideService(DispatchModeLimit, {
          runtimeMode: owner.runtimeMode,
          interactionMode: owner.interactionMode,
          refused,
        }),
        Effect.provideService(ThreadLaunch.ThreadLaunchService, launches),
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(ServerConfig.ServerConfig, config),
      );
    }).pipe(
      Effect.mapError(
        (cause) =>
          new OverseerLaunchError({
            reason: "E7",
            cause,
            linkId: row.link_id,
            threadId: input.threadId,
            projectId: input.projectId,
          }),
      ),
      Effect.catch((error) =>
        Ref.get(refused).pipe(
          Effect.flatMap((refusal) =>
            Effect.fail(refusal === undefined ? error : McpToolAccess.escalationDenied(refusal)),
          ),
        ),
      ),
    );
    yield* assertReceipt(row, input.commandId, "thread.create");
    if (input.initialMessage)
      yield* assertReceipt(
        row,
        CommandId.make(`${input.commandId}:initial-message`),
        "message.dispatch",
      );
    const accepted = yield* store.acceptLaunch(row.link_id).pipe(
      Effect.mapError(
        (cause) =>
          new OverseerLaunchError({
            reason: "E7",
            cause,
            linkId: row.link_id,
            threadId: input.threadId,
            projectId: input.projectId,
          }),
      ),
    );
    // Unlink ends observation, not the independent work already requested by the caller.
    return {
      ...result,
      ...(accepted ? { linkId: row.link_id } : {}),
      initialMessageId: MessageId.make(row.launch_message_id),
    };
  });

  const launch = Effect.fn("OverseerLaunchService.launch")(
    function* (request: Request) {
      if (!(yield* Feature.Enabled)) return yield* unavailable();
      yield* readCaller();
      const authenticated = yield* McpToolAccess.writingCaller;
      const owner = authenticated.caller;
      if (!owner || owner.lineage.relationshipToParent === "subagent" || owner.activeRunId === null)
        return yield* new OverseerLaunchError({ reason: "E1" });
      if (request.oversee !== true)
        return yield* new OverseerLaunchError({
          reason: request.clientRequestId === undefined ? "E2" : "E3",
        });
      if (request.clientRequestId === undefined)
        return yield* new OverseerLaunchError({ reason: "E2" });
      if (!request.message?.trim()) return yield* new OverseerLaunchError({ reason: "E9" });
      if (
        request.scratch &&
        (request.projectId !== undefined || request.workspaceStrategy !== undefined)
      )
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "scratch:true picks its own project and folder; omit projectId and workspaceStrategy.",
        });
      const key = request.clientRequestId;
      const identity = yield* digest([owner.id, key]);
      // Request defaults are normalized symbolically. Mutable caller model/mode defaults
      // are frozen in the recovery input, not re-resolved into a different task on retry.
      const fingerprint = yield* digest({
        ...request,
        oversee: undefined,
        clientRequestId: undefined,
        projectId: request.scratch ? undefined : (request.projectId ?? owner.projectId),
        scratch: request.scratch === true,
        workspaceStrategy: request.workspaceStrategy ?? { type: "root" },
        attachments: request.attachments ?? [],
      });
      return yield* locks.withLock(
        `overseer:link:${identity}`,
        Effect.gen(function* () {
          const context = yield* McpToolAccess.writingCaller;
          const owner = context.caller;
          if (!owner || owner.lineage.relationshipToParent === "subagent")
            return yield* new OverseerLaunchError({ reason: "E1" });
          const existing = yield* store
            .reservationFor(owner.id, key)
            .pipe(Effect.mapError(unavailable));
          if (existing) {
            if (existing.fingerprint !== fingerprint)
              return yield* new OverseerLaunchError({ reason: "E4" });
            return yield* resume(existing);
          }
          if (owner.activeRunId === null) return yield* new OverseerLaunchError({ reason: "E1" });
          yield* certified(context.scope.thread!.providerInstanceId);
          const attachments = request.attachments ?? [];
          if (attachments.some((item) => !Claims.attachmentIsPendingUpload(item)))
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "A new thread accepts only pending attachment uploads.",
            });
          yield* Claims.validateAttachmentLimits(attachments).pipe(
            Effect.mapError(
              (error) =>
                new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
            ),
          );
          if (
            request.scratch &&
            (request.projectId !== undefined || request.workspaceStrategy !== undefined)
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "scratch:true picks its own project and folder; omit projectId and workspaceStrategy.",
            });
          const projectId = request.scratch
            ? (yield* folders.ensureScratchProject.pipe(
                Effect.mapError(
                  (error) =>
                    new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message: error.message,
                    }),
                ),
              )).projectId
            : yield* resolveProjectId(context, request.projectId);
          const project = yield* projects.getById(projectId).pipe(Effect.mapError(unavailable));
          if (Option.isNone(project))
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The project was not found.",
            });
          const modelSelection = request.modelSelection ?? owner.modelSelection;
          yield* certified(modelSelection.instanceId);
          const runtimeMode = yield* resolveRuntimeMode(
            context.limits.runtimeMode,
            request.runtimeMode,
          );
          const interactionMode = yield* resolveInteractionMode(
            context.limits.interactionMode,
            request.interactionMode,
          );
          const commandId = CommandId.make(`overseer:launch:${identity}`);
          const threadId = ThreadId.make(`overseer:${identity}`);
          const messageId = MessageId.make(`overseer:initial:${identity}`);
          yield* Claims.validatePendingAttachments({ threadId, attachments }).pipe(
            Effect.provideService(FileSystem.FileSystem, fs),
            Effect.provideService(ServerConfig.ServerConfig, config),
            Effect.mapError(
              (error) =>
                new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
            ),
          );
          const isScratch = yield* folders.isScratchProject(projectId);
          let strategy =
            isScratch && request.workspaceStrategy?.type === "worktree"
              ? { type: "root" as const }
              : (request.workspaceStrategy ?? { type: "root" as const });
          if (strategy.type === "existing_worktree")
            yield* assertProjectWorktree(project.value.workspaceRoot, strategy.worktreePath).pipe(
              Effect.provideService(GitVcsDriver.GitVcsDriver, git),
              Effect.provideService(FileSystem.FileSystem, fs),
            );
          if (strategy.type === "worktree")
            yield* launches.checkWorktreeBase({ projectId, workspaceStrategy: strategy }).pipe(
              Effect.mapError(
                (error) =>
                  new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: `${error.message} No thread was created. Pass a branch, tag, or commit that exists there${strategy.type === "worktree" && strategy.startFromOrigin === true ? "; the project has no usable remote to fetch it from." : ", or set startFromOrigin:true if it exists only on a remote."}`,
                  }),
              ),
            );
          const scratch =
            isScratch && strategy.type === "root"
              ? yield* folders.reservedThreadFolder({
                  projectId,
                  threadId,
                  text: request.message ?? request.title,
                })
              : Option.none();
          if (Option.isSome(scratch))
            strategy = { type: "existing_worktree", worktreePath: scratch.value };
          const input: DurableInput = {
            commandId,
            threadId,
            projectId,
            title: request.title,
            modelSelection,
            runtimeMode,
            interactionMode,
            workspaceStrategy: strategy,
            ...(Option.isSome(scratch) ? { scratchFolder: scratch.value } : {}),
            ...(request.message === undefined && attachments.length === 0
              ? {}
              : {
                  initialMessage: {
                    messageId,
                    senderThreadId: owner.id,
                    text: request.message ?? "",
                    attachments,
                  },
                }),
          };
          yield* threads
            .assertProviderInstanceEnabled({
              projectId,
              commandId,
              instanceId: modelSelection.instanceId,
            })
            .pipe(
              Effect.mapError(
                (error) =>
                  new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
              ),
            );
          const launchInputJson = yield* encodeDurable(input).pipe(
            Effect.mapError((cause) => new OverseerLaunchError({ reason: "E8", cause })),
          );
          const row = yield* store
            .reserve({
              linkId: `overseer:link:${identity}`,
              observerThreadId: owner.id,
              observerProjectId: owner.projectId,
              targetThreadId: threadId,
              targetProjectId: projectId,
              clientRequestId: key,
              fingerprint,
              launchCommandId: commandId,
              launchMessageId: messageId,
              launchInputJson,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OverseerLaunchError({
                    reason:
                      cause._tag === "OverseerConflict"
                        ? cause.reason === "retry_input"
                          ? "E4"
                          : "E6"
                        : "E8",
                    cause,
                  }),
              ),
            );
          return yield* resume(row);
        }),
      );
    },
    Effect.provideService(Threads.ThreadManagementService, threads),
  );

  const recover = Effect.gen(function* () {
    if (!(yield* Feature.Enabled)) return { resumed: 0, pending: 0 };
    let after = "";
    let resumed = 0;
    let pending = 0;
    while (true) {
      const page = yield* store
        .incompleteLaunches(after, 100)
        .pipe(Effect.mapError((cause) => new OverseerLaunchError({ reason: "E7", cause })));
      for (const row of page) {
        const result = yield* locks.withLock(row.link_id, resume(row)).pipe(Effect.exit);
        if (Exit.isSuccess(result)) resumed++;
        else pending++;
        after = row.link_id;
      }
      if (page.length < 100) break;
    }
    return { resumed, pending };
  });
  return OverseerLaunchService.of({ launch, recover });
});

export const layer = Layer.effect(OverseerLaunchService, make);

import { ChatAttachmentId, type ChatAttachment } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import {
  createDeterministicAttachmentId,
  parseAttachmentFileExtension,
  planAttachmentClaim,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import {
  AttachmentClaimError,
  attachmentIsPendingUpload,
  releaseClaimedAttachments,
  validateAttachmentLimits,
} from "../orchestration-v2/AttachmentClaims.ts";

type Input = {
  readonly threadId: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
};

// Admission and reserved recovery share metadata checks, but never alter ordinary upload claims.
const prepare = Effect.fnUntraced(function* (input: Input, copy: boolean) {
  yield* validateAttachmentLimits(input.attachments);
  if (
    new Set(input.attachments.map((attachment) => attachment.id)).size !== input.attachments.length
  )
    return yield* new AttachmentClaimError({
      message: "Duplicate attachment ids are not allowed.",
    });
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const claimedPaths: string[] = [];
  const attachments = yield* Effect.forEach(
    input.attachments,
    (attachment) =>
      Effect.gen(function* () {
        if (!attachmentIsPendingUpload(attachment)) return attachment;
        const extension = parseAttachmentFileExtension(attachment.id);
        const stableId = createDeterministicAttachmentId(input.threadId, attachment.id);
        const finalId = stableId && `${stableId}${extension ? `-${extension}` : ""}`;
        const currentPath = resolveAttachmentPath({
          attachmentsDir: config.attachmentsDir,
          attachment,
        });
        const normalized: ChatAttachment = {
          ...attachment,
          id: ChatAttachmentId.make(finalId ?? ""),
          mimeType: attachment.mimeType.toLowerCase(),
        };
        const finalPath = resolveAttachmentPath({
          attachmentsDir: config.attachmentsDir,
          attachment: normalized,
        });
        if (!currentPath || !finalPath || !finalId)
          return yield* new AttachmentClaimError({
            message: `Attachment '${attachment.name}' cannot be sent: invalid attachment id.`,
          });
        if (!copy) {
          const plan = planAttachmentClaim({
            attachmentsDir: config.attachmentsDir,
            threadId: input.threadId,
            attachmentId: attachment.id,
          });
          if (!plan.ok)
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: ${plan.reason}.`,
            });
          if (
            resolveAttachmentPath({
              attachmentsDir: config.attachmentsDir,
              attachment: { ...normalized, id: ChatAttachmentId.make(plan.finalId) },
            }) !== plan.finalPath
          )
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: attachment type does not match the upload.`,
            });
        } else {
          // A durable reservation fixes both target ID and metadata. Its owned copy survives upload expiry.
          const existing = yield* fs.stat(finalPath).pipe(Effect.option);
          if (Option.isSome(existing) && Number(existing.value.size) === attachment.sizeBytes)
            return normalized;
        }
        const info = yield* fs.stat(currentPath).pipe(
          Effect.mapError(
            (cause) =>
              new AttachmentClaimError({
                message: `Attachment '${attachment.name}' cannot be sent: attachment not found.`,
                cause,
              }),
          ),
        );
        if (Number(info.size) !== attachment.sizeBytes)
          return yield* new AttachmentClaimError({
            message: `Attachment '${attachment.name}' cannot be sent: stored size does not match.`,
          });
        if (copy)
          yield* fs.copyFile(currentPath, finalPath).pipe(
            Effect.mapError(
              (cause) =>
                new AttachmentClaimError({
                  message: `Failed to claim attachment '${attachment.name}' for this thread.`,
                  cause,
                }),
            ),
            Effect.andThen(Effect.sync(() => claimedPaths.push(finalPath))),
            Effect.uninterruptible,
          );
        return normalized;
      }),
    { concurrency: 1 },
  ).pipe(Effect.onError(() => releaseClaimedAttachments(claimedPaths)));
  return attachments;
});

export const validate = Effect.fn("ReservedUploads.validate")((input: Input) =>
  prepare(input, false).pipe(Effect.asVoid),
);
export const claim = Effect.fn("ReservedUploads.claim")((input: Input) => prepare(input, true));

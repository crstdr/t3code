// Approved wording. Only the disabled-by-default oversight feature uses these.
export const linkText = {
  description:
    "List or unlink oversight links involving this calling thread. Unlink is permanent: it stops future notices, not the other thread. Links grant no additional authority. Use t3_thread_read and t3_thread_send for permitted reads and messages.",
  op: "list returns this thread's incoming and outgoing links; unlink closes linkId.",
  linkId: "Required for unlink. Use a linkId returned by t3_thread_launch or by list.",
  cursor: "For list, continue from the returned nextCursor. Omit for the first page.",
  caller: "This operation requires a calling T3 thread.",
  unsupported: "Thread oversight is not supported.",
  not_found: "The oversight link or thread was not found.",
  invalid_cursor: "The oversight cursor is not valid for this thread.",
  unavailable: "Oversight storage is unavailable.",
} as const;

export const launchText = {
  T1: "Without oversee:true, each call creates a new launch. With oversee:true, clientRequestId identifies a retry of the same launch. Retain threadId and use t3_thread_read/t3_thread_wait to follow preparation.",
  T2: "After errors or lost responses, reuse clientRequestId with identical arguments for an overseen launch; otherwise inspect t3_thread_list before retrying.",
  T3: "Set oversee:true to receive completion, user-input-question and approval notices about the new thread. The oversight link grants no additional access and no authority to answer or approve.",
  T4: "Create a durable link from this calling T3 thread to the new top-level thread. Requires clientRequestId and a non-empty message. Default false.",
  T5: "Required with oversee:true only. Reuse this key with identical launch arguments after a lost response; a different task needs a new key.",
  E1: "oversee:true requires a calling top-level T3 thread.",
  E2: "clientRequestId is required with oversee:true.",
  E3: "clientRequestId is supported only with oversee:true.",
  E4: "This clientRequestId was already used with different launch arguments. To retry that launch, repeat its original arguments; use a new key only for a different task.",
  E5: "Oversight requires Codex or Claude for both this thread and the new thread; otherwise launch without oversee:true.",
  E6: "The oversight link is not available to this calling thread.",
  E7: "The launch is reserved but not finished. Retry with the same clientRequestId and identical arguments; if this repeats, tell the user instead of starting another launch.",
  E8: "Oversight storage is unavailable. The overseen launch was not started.",
  E9: "oversee:true requires a non-empty message.",
} as const;

export const wakeText = {
  W1: "Oversight updates (batch {batchId}). Links grant no additional authority.\n{entries}\nRead permitted output with t3_thread_read; approvals appear in view='activity'. Read pending questions with t3_pending_request_read. A notice is not permission to answer or approve; surface needed user input or approvals to the user.",
  completion: "Thread {threadId}, run {runId}: {outcome}.",
  question: "Thread {threadId}, request {requestId}: user-input question pending.",
  approval: "Thread {threadId}, request {requestId}: user approval pending.",
  W3: "[Periodic wake-up. Perform only checks or follow-ups previously requested by the user. If none are due, end your turn without taking action.]",
  W2: "Oversight updates",
} as const;

export type WakeEntry = { readonly threadId: string } & (
  | {
      readonly kind: "completion";
      readonly runId: string;
      readonly outcome: "completed" | "failed" | "cancelled" | "interrupted";
    }
  | { readonly kind: "question" | "approval"; readonly requestId: string }
);

// IDs are data, never prose: oversized identities leave the entire occurrence pending.
export function renderWake(batchId: string, entries: ReadonlyArray<WakeEntry>) {
  const lines = entries.slice(0, 8).map((entry) =>
    entry.kind === "completion"
      ? wakeText.completion
          .replace("{threadId}", () => entry.threadId)
          .replace("{runId}", () => entry.runId)
          .replace("{outcome}", () => entry.outcome)
      : wakeText[entry.kind]
          .replace("{threadId}", () => entry.threadId)
          .replace("{requestId}", () => entry.requestId),
  );
  for (let count = lines.length; count > 0; count--) {
    const text = wakeText.W1.replace("{batchId}", () => batchId).replace("{entries}", () =>
      lines.slice(0, count).join("\n"),
    );
    if (Buffer.byteLength(text, "utf8") + Buffer.byteLength(wakeText.W2, "utf8") <= 2048)
      return { text, count };
  }
  return undefined;
}

export function oversightLaunchDescription(ordinary: string) {
  return ordinary
    .replace(
      "Each call creates a new launch with no retry key; retain threadId and use t3_thread_read/t3_thread_wait to follow preparation.",
      launchText.T1,
    )
    .replace(
      "After errors or lost responses, inspect t3_thread_list before retrying.",
      launchText.T2,
    )
    .concat(" ", launchText.T3);
}

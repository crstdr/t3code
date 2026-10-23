// Unapproved §7 drafts. Only the disabled-by-default oversight feature uses these.
export const launchText = {
  T1: "Without oversee:true, each call creates a new launch. With oversee:true, clientRequestId identifies a retry of the same launch. Retain threadId and use t3_thread_read/t3_thread_wait to follow preparation.",
  T2: "After errors or lost responses, reuse clientRequestId for an overseen launch; otherwise inspect t3_thread_list before retrying.",
  T3: "Set oversee:true to receive completion and user-input-question notices from the new thread. The link grants no additional access or answering authority.",
  T4: "Create a durable link from this calling T3 thread to the new top-level thread. Requires clientRequestId. Default false.",
  T5: "Required with oversee:true only. Reuse this key with identical launch arguments after a lost response; a different task needs a new key.",
  E1: "oversee:true requires a calling T3 thread.",
  E2: "clientRequestId is required with oversee:true.",
  E3: "clientRequestId is supported only with oversee:true.",
  E4: "This clientRequestId was already used with different launch arguments. Use the original arguments or a new key.",
  E5: "This provider is not supported for oversight in v1. Use Codex or Claude, or launch without oversee:true.",
  E6: "The oversight link is not available to this calling thread.",
  E7: "The launch is reserved but not finished. Retry with the same clientRequestId.",
  E8: "Oversight storage is unavailable. The overseen launch was not started.",
  E9: "oversee:true requires message.",
} as const;

export const wakeText = {
  W1: "Oversight updates (batch {batchId}). Links grant no additional authority.\n{entries}\nRead permitted output with t3_thread_read; approvals appear in view='activity'. Read pending questions with t3_pending_request_read. A notice is not permission to answer or approve; surface needed user input to the user.",
  completion: "Thread {threadId}, run {runId}: {outcome}.",
  question: "Thread {threadId}, request {requestId}: user-input question pending.",
  approval: "Thread {threadId}, request {requestId}: approval pending.",
  W3: "[Periodic wake-up. Perform only checks or follow-ups previously requested by the user. If none are due, take no action and return to idle.]",
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

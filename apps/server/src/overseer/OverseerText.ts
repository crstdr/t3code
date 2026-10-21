// Unapproved §7 drafts. Only the disabled-by-default oversight catalog uses these.
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

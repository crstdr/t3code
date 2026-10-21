import { renderWake, type WakeEntry } from "./OverseerText.ts";

const sampleObserver = "9c2c2e38-8138-4bf8-bfd0-92b187416efa";
export const sampleNativeUuid = "e6c1f8d3-7bab-4c15-81d7-688f00bc87a9";
export function wakeExample(count: number, questionOnly = false) {
  const entries: WakeEntry[] = Array.from({ length: count }, (_, index) => {
    const threadId = `4a62f3d9-5d14-44f0-94a7-9c835c52276${index}`;
    return questionOnly || index % 2 === 1
      ? { kind: "question", threadId, requestId: `15679e63-c80b-44b2-9c73-d4bb8dad937${index}` }
      : {
          kind: "completion",
          threadId,
          runId: `bfa651c0-2c59-4270-aac5-65c7713f7a0${index}`,
          outcome: "completed",
        };
  });
  return renderWake(`${sampleObserver}:12`, entries)!;
}

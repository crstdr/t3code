import type { ScopedThreadRef } from "@t3tools/contracts";

const OPEN = "t3code:show-oversight-links";
export function showOversightLinks(ref: ScopedThreadRef): void {
  window.dispatchEvent(new CustomEvent(OPEN, { detail: ref }));
}
export function onShowOversightLinks(listener: (ref: ScopedThreadRef) => void): () => void {
  const handler = (event: Event) => listener((event as CustomEvent<ScopedThreadRef>).detail);
  window.addEventListener(OPEN, handler);
  return () => window.removeEventListener(OPEN, handler);
}

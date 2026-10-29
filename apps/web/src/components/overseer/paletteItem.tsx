import type { ScopedThreadRef } from "@t3tools/contracts";
import { LinkIcon } from "lucide-react";
import type { CommandPaletteActionItem } from "../CommandPalette.logic";
import { showOversightLinks } from "./panelIntent";

export function oversightPaletteItem(
  ref: ScopedThreadRef | null,
  capabilities: { readonly threadOversight?: boolean } | undefined,
): CommandPaletteActionItem | null {
  if (ref === null || capabilities?.threadOversight !== true) return null;
  return {
    kind: "action",
    value: "action:show-oversight-links",
    title: "Show oversight links",
    searchTerms: ["oversight", "overseer", "worker", "links", "show oversight links"],
    icon: <LinkIcon className="size-4" />,
    run: async () => {
      showOversightLinks(ref);
    },
  };
}

import { useAtomValue } from "@effect/atom-react";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { useNavigate } from "@tanstack/react-router";
import type {
  EnvironmentId,
  ThreadId,
  OversightLink,
  OversightPreferences as Preferences,
} from "@t3tools/contracts";
import { LinkIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { useProject, useServerConfigs } from "../../state/entities";
import { useEnvironmentQuery } from "../../state/query";
import { overseerEnvironment } from "../../state/overseer";
import { useAtomCommand } from "../../state/use-atom-command";
import { buildThreadRouteParams } from "../../threadRoutes";
import { Button } from "../ui/button";
import { Popover, PopoverTrigger, PopoverPopup, PopoverTitle } from "../ui/popover";
import { onShowOversightLinks } from "./panelIntent";
import { OversightRoleIcon } from "./icons/OversightRoleIcon";
import { OversightPreferences } from "./OversightPreferences";

type Props = { readonly environmentId: EnvironmentId; readonly threadId: ThreadId };

export function ThreadOversightLinks(props: Props) {
  const enabled =
    useServerConfigs().get(props.environmentId)?.environment.capabilities.threadOversight === true;
  return enabled ? (
    <EnabledLinks key={`${props.environmentId}:${props.threadId}`} {...props} />
  ) : null;
}

function LinkRow({
  link,
  environmentId,
  threadId,
  disabled,
  unlink,
  close,
}: Props & {
  readonly link: OversightLink;
  readonly disabled: boolean;
  readonly unlink: (linkId: string) => void;
  readonly close: () => void;
}) {
  const project = useProject(scopeProjectRef(environmentId, link.projectId));
  const incoming = link.targetThreadId === threadId;
  const destination = incoming ? link.observerThreadId : link.targetThreadId;
  const navigate = useNavigate();
  const unavailable =
    !incoming && (link.workerStatus === "reserved" || link.workerStatus === "deleted");
  return (
    <li className="space-y-1 border-b border-border py-2 last:border-b-0">
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <p className="text-xs text-muted-foreground">
            {incoming ? "Overseen by" : "Worker"} · {project?.title ?? "Unavailable project"}
          </p>
          <Button
            variant="link"
            size="sm"
            disabled={unavailable}
            onClick={() => {
              close();
              void navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(scopeThreadRef(environmentId, destination)),
              });
            }}
          >
            {link.title || "Untitled thread"}
            {link.titleTruncated ? "…" : ""}
          </Button>
        </div>
        <Button
          variant="ghost"
          size="xs"
          disabled={disabled}
          aria-label={`Unlink ${link.title || "Untitled thread"}`}
          onClick={() => unlink(link.linkId)}
        >
          Unlink
        </Button>
      </div>
      {!incoming ? (
        <p className="text-xs text-muted-foreground">
          {link.workerStatus}
          {link.pendingRequestCount > 0
            ? ` · ${link.pendingRequestCount} pending question${link.pendingRequestCount === 1 ? "" : "s"}`
            : ""}
        </p>
      ) : null}
      <p className="text-xs text-muted-foreground">Delivery: {link.deliveryState}</p>
    </li>
  );
}

function EnabledLinks({ environmentId, threadId }: Props) {
  const [open, setOpen] = useState(false);
  const [cursors, setCursors] = useState<ReadonlyArray<string>>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // The visible header owns only page one. Additional page watches live only while the panel is open.
  const first = useEnvironmentQuery(
    overseerEnvironment.watch({ environmentId, input: { threadId } }),
  );
  const cursor = cursors.at(-1);
  const page = useEnvironmentQuery(
    open && cursor !== undefined
      ? overseerEnvironment.watch({ environmentId, input: { threadId, cursor } })
      : null,
  );
  const current = cursor === undefined ? first : page;
  const outgoing = first.data?.links.some((link) => link.observerThreadId === threadId) === true;
  const incoming = first.data?.links.some((link) => link.targetThreadId === threadId) === true;
  const canUnlink = useAtomValue(overseerEnvironment.unlink.permissionAtom(environmentId));
  const canConfigure = useAtomValue(overseerEnvironment.configure.permissionAtom(environmentId));
  const unlink = useAtomCommand(overseerEnvironment.unlink, { reportFailure: false });
  const configure = useAtomCommand(overseerEnvironment.configure, { reportFailure: false });
  useEffect(
    () =>
      onShowOversightLinks((ref) => {
        if (ref.environmentId === environmentId && ref.threadId === threadId) setOpen(true);
      }),
    [environmentId, threadId],
  );
  const changePreferences = async (preferences: Partial<Preferences>) => {
    if (busy) return;
    setBusy(true);
    setError(null);
    const result = await configure({ environmentId, input: { threadId, preferences } });
    setBusy(false);
    if (result._tag === "Failure") setError("Could not save wake preferences.");
  };
  const remove = async (linkId: string) => {
    if (busy) return;
    setBusy(true);
    setError(null);
    const result = await unlink({ environmentId, input: { linkId } });
    setBusy(false);
    if (result._tag === "Failure") setError("Could not unlink thread.");
  };
  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) {
          setCursors([]);
          setError(null);
        }
      }}
    >
      {outgoing || incoming || open || first.error !== null ? (
        <PopoverTrigger
          render={<Button variant="ghost" size="xs" aria-label="Show oversight links" />}
        >
          {outgoing ? <OversightRoleIcon role="overseer" /> : null}
          {incoming ? <OversightRoleIcon role="worker" /> : null}
          {!outgoing && !incoming ? <LinkIcon className="size-4" /> : null}
        </PopoverTrigger>
      ) : null}
      <PopoverPopup width="lg" align="start">
        <PopoverTitle>Oversight links</PopoverTitle>
        <p className="mt-1 text-xs text-muted-foreground">
          Unlink stops notifications, not the worker.
        </p>
        {error !== null ? (
          <p role="alert" className="mt-2 text-xs text-destructive">
            {error}
          </p>
        ) : null}
        {current.error !== null ? (
          <p role="alert" className="mt-2 text-xs text-destructive">
            Could not load oversight links.{" "}
            <Button variant="link" size="xs" onClick={current.refresh}>
              Retry
            </Button>
          </p>
        ) : current.data === null ? (
          <p role="status" className="py-3 text-xs text-muted-foreground">
            Loading oversight links…
          </p>
        ) : current.data.links.length === 0 ? (
          <p className="py-3 text-xs text-muted-foreground">No oversight links.</p>
        ) : (
          <ul className="my-2 max-h-72 overflow-y-auto">
            {current.data.links.map((link) => (
              <LinkRow
                key={link.linkId}
                link={link}
                environmentId={environmentId}
                threadId={threadId}
                disabled={!canUnlink || busy}
                unlink={(linkId) => void remove(linkId)}
                close={() => setOpen(false)}
              />
            ))}
          </ul>
        )}
        {cursor !== undefined || current.data?.nextCursor !== undefined ? (
          <div className="my-2 flex justify-between gap-2">
            <Button
              variant="ghost"
              size="xs"
              disabled={cursor === undefined}
              onClick={() => setCursors((values) => values.slice(0, -1))}
            >
              Previous
            </Button>
            <Button
              variant="ghost"
              size="xs"
              disabled={current.data?.nextCursor === undefined}
              onClick={() => {
                const next = current.data?.nextCursor;
                if (next !== undefined) setCursors((values) => [...values, next]);
              }}
            >
              Next
            </Button>
          </div>
        ) : null}
        {first.data?.preferences !== undefined ? (
          <OversightPreferences
            value={first.data.preferences}
            disabled={!canConfigure || busy || first.error !== null}
            onChange={(value) => void changePreferences(value)}
          />
        ) : null}
      </PopoverPopup>
    </Popover>
  );
}

// Adapted from RepoPrompt CE f0fab66c5f842b3677eb5aa3e53c41f3e79c1b5d, PR #1330 (Fb/baron).
// SwiftUI Canvas → SVG; Apache-2.0 license and attribution are in LICENSE and NOTICE beside this file.
export function OversightRoleIcon({ role }: { readonly role: "overseer" | "worker" }) {
  return (
    <svg
      viewBox="0 0 24 24"
      width="16"
      height="16"
      aria-label={role === "overseer" ? "Overseer" : "Worker"}
      role="img"
      className={
        role === "overseer"
          ? "text-oversight-overseer-light dark:text-oversight-overseer-dark"
          : "text-icon-muted"
      }
    >
      <g
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity={role === "worker" ? 0.5 : 1}
      >
        {role === "overseer" ? (
          <>
            <path d="M12 9V5.5M9.5 13.6 6.2 16.8M14.5 13.6 17.8 16.8" />
            <circle cx="12" cy="3.5" r="2" />
            <circle cx="4.5" cy="18" r="2" />
            <circle cx="19.5" cy="18" r="2" />
          </>
        ) : (
          <>
            <path d="M12 7V12.8" />
            <circle cx="12" cy="4.5" r="2.3" />
          </>
        )}
      </g>
      <circle
        cx="12"
        cy={role === "overseer" ? 12 : 17}
        r={role === "overseer" ? 3 : 4}
        fill="currentColor"
      />
    </svg>
  );
}

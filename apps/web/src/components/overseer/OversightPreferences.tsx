import type { OversightPreferences as Preferences } from "@t3tools/contracts";
import { Switch } from "../ui/switch";
import { Select, SelectTrigger, SelectValue, SelectPopup, SelectItem } from "../ui/select";

export function OversightPreferences({
  value,
  disabled,
  onChange,
}: {
  readonly value: Preferences;
  readonly disabled: boolean;
  readonly onChange: (value: Partial<Preferences>) => void;
}) {
  return (
    <fieldset disabled={disabled} className="space-y-3 border-t border-border pt-3 text-xs">
      <legend className="sr-only">Wake preferences</legend>
      <label className="flex items-center justify-between gap-3">
        Auto-wake on all updates
        <Switch
          checked={value.autoWake}
          disabled={disabled}
          onCheckedChange={(autoWake) => onChange({ autoWake })}
        />
      </label>
      <div className="space-y-1.5">
        <label className="flex items-center justify-between gap-3">
          Routine wake-ups no more often than
          <Switch
            checked={value.cadenceEnabled}
            disabled={disabled}
            onCheckedChange={(cadenceEnabled) => onChange({ cadenceEnabled })}
          />
        </label>
        <Select
          value={value.cadenceMinutes}
          disabled={disabled || !value.cadenceEnabled}
          onValueChange={(cadenceMinutes) => {
            if (cadenceMinutes !== null) onChange({ cadenceMinutes });
          }}
        >
          <SelectTrigger size="xs" aria-label="Routine wake-up interval">
            <SelectValue>{value.cadenceMinutes} min</SelectValue>
          </SelectTrigger>
          <SelectPopup>
            {([1, 5, 10, 15, 30, 60, 180] as const).map((minutes) => (
              <SelectItem key={minutes} value={minutes}>
                {minutes} min
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      </div>
      <div className="space-y-1.5">
        <label className="flex items-center justify-between gap-3">
          Wake when idle for
          <Switch
            checked={value.idleEnabled}
            disabled={disabled}
            onCheckedChange={(idleEnabled) => onChange({ idleEnabled })}
          />
        </label>
        <Select
          value={value.idleMinutes}
          disabled={disabled || !value.idleEnabled}
          onValueChange={(idleMinutes) => {
            if (idleMinutes !== null) onChange({ idleMinutes });
          }}
        >
          <SelectTrigger size="xs" aria-label="Idle wake-up interval">
            <SelectValue>{value.idleMinutes} min</SelectValue>
          </SelectTrigger>
          <SelectPopup>
            {([10, 30, 60, 120, 360] as const).map((minutes) => (
              <SelectItem key={minutes} value={minutes}>
                {minutes} min
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      </div>
    </fieldset>
  );
}

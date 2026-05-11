"use client";
import { useState } from "react";
import { Search, Sliders, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { cn } from "@/lib/utils";

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G", "NC"] as const;
const DEPARTEMENTS = ["75", "77", "78", "91", "92", "93", "94", "95"];
const PERIODES = [
  "AVANT_1949",
  "1949_1974",
  "1975_1993",
  "1994_2000",
  "2001_2010",
  "APRES_2011",
];

export type MapFilters = {
  q: string;
  cp: string;
  syndic: string;
  dept: string;
  dpeClasses: string[];
  minLots: number | null;
  periode: string;
};

export const DEFAULT_FILTERS: MapFilters = {
  q: "",
  cp: "",
  syndic: "",
  dept: "",
  dpeClasses: [],
  minLots: null,
  periode: "",
};

export function FiltersBar({
  value,
  onChange,
  onSubmit,
  onReset,
  resultCount,
}: {
  value: MapFilters;
  onChange: (next: MapFilters) => void;
  onSubmit: () => void;
  onReset: () => void;
  resultCount: number;
}) {
  const [open, setOpen] = useState(true);
  const toggleClass = (c: string) => {
    const next = value.dpeClasses.includes(c)
      ? value.dpeClasses.filter((x) => x !== c)
      : [...value.dpeClasses, c];
    onChange({ ...value, dpeClasses: next });
  };

  return (
    <div className="absolute left-4 top-4 z-10 w-[360px] max-w-[calc(100vw-2rem)]">
      <div className="premium-card overflow-hidden">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="flex items-center gap-2">
            <Sliders className="h-4 w-4 text-primary" />
            <span className="text-sm font-semibold">Filtres prospection</span>
          </div>
          <button
            onClick={() => setOpen((v) => !v)}
            className="text-xs text-muted-foreground hover:text-foreground"
          >
            {open ? "Replier" : "Déplier"}
          </button>
        </div>

        {open ? (
          <div className="space-y-3 p-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={value.q}
                onChange={(e) => onChange({ ...value, q: e.target.value })}
                onKeyDown={(e) => e.key === "Enter" && onSubmit()}
                placeholder="Adresse, nom de copro, n° immatriculation…"
                className="pl-9"
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <Input
                value={value.cp}
                onChange={(e) => onChange({ ...value, cp: e.target.value })}
                placeholder="Code postal"
                inputMode="numeric"
              />
              <select
                value={value.dept}
                onChange={(e) => onChange({ ...value, dept: e.target.value })}
                className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
              >
                <option value="">Département</option>
                {DEPARTEMENTS.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </div>

            <Input
              value={value.syndic}
              onChange={(e) => onChange({ ...value, syndic: e.target.value })}
              placeholder="Syndic / gestionnaire"
            />

            <div>
              <div className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Classe DPE finale
              </div>
              <div className="flex flex-wrap gap-1.5">
                {DPE_CLASSES.map((c) => {
                  const active = value.dpeClasses.includes(c);
                  return (
                    <button
                      key={c}
                      onClick={() => toggleClass(c)}
                      className={cn(
                        "cursor-pointer transition-transform",
                        active
                          ? "scale-110 ring-2 ring-foreground/70 ring-offset-1"
                          : "opacity-50 hover:opacity-100",
                      )}
                      aria-pressed={active}
                    >
                      <DpeBadge classe={c} />
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-2">
              <Input
                type="number"
                min={0}
                value={value.minLots ?? ""}
                onChange={(e) =>
                  onChange({
                    ...value,
                    minLots: e.target.value === "" ? null : Number(e.target.value),
                  })
                }
                placeholder="Lots min."
              />
              <select
                value={value.periode}
                onChange={(e) => onChange({ ...value, periode: e.target.value })}
                className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
              >
                <option value="">Période</option>
                {PERIODES.map((p) => (
                  <option key={p} value={p}>
                    {p.replace(/_/g, " ").replace("AVANT", "<").replace("APRES", ">")}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-center justify-between gap-2 pt-1">
              <Button variant="ghost" size="sm" onClick={onReset}>
                <X className="h-3.5 w-3.5" />
                Réinitialiser
              </Button>
              <Button size="sm" onClick={onSubmit} className="flex-1">
                Appliquer ({resultCount})
              </Button>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

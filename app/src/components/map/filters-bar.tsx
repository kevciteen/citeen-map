"use client";
import { useState } from "react";
import { Building2, Building, Briefcase, Home, Search, Sliders, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { cn } from "@/lib/utils";

export type MapMode = "copros" | "maisons" | "appartements" | "tertiaire" | "both";

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
const SECTEURS_TERTIAIRE = [
  "Bureaux",
  "Commerces",
  "Hotellerie / Restauration",
  "Sante",
  "Enseignement",
  "Autres secteurs",
];

export type MapFilters = {
  q: string;
  cp: string;
  commune: string;
  syndic: string;
  dept: string;
  dpeClasses: string[];
  minLots: number | null;
  periode: string;
  mode: MapMode;
  // Avancés tertiaire + maisons/apparts
  secteurTertiaire: string;
  surfaceMin: number | null;
  surfaceMax: number | null;
  yearMin: number | null;
  yearMax: number | null;
  proprietaire: string;
};

export const DEFAULT_FILTERS: MapFilters = {
  q: "",
  cp: "",
  commune: "",
  syndic: "",
  dept: "",
  dpeClasses: [],
  minLots: null,
  periode: "",
  mode: "copros",
  secteurTertiaire: "",
  surfaceMin: null,
  surfaceMax: null,
  yearMin: null,
  yearMax: null,
  proprietaire: "",
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

  const showsCopros = value.mode === "copros" || value.mode === "both";
  const showsTertiaire = value.mode === "tertiaire" || value.mode === "both";
  const showsMaisons = value.mode === "maisons" || value.mode === "appartements" || value.mode === "both";

  return (
    <div className="absolute left-4 top-4 z-10 w-[380px] max-w-[calc(100vw-2rem)]">
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
            {/* Mode toggle : 5 types */}
            <div className="grid grid-cols-5 gap-1 rounded-lg border border-border bg-background p-0.5">
              <ModeBtn
                active={value.mode === "copros"}
                onClick={() => onChange({ ...value, mode: "copros" })}
                icon={<Building2 className="h-3 w-3" />}
                label="Copros"
              />
              <ModeBtn
                active={value.mode === "tertiaire"}
                onClick={() => onChange({ ...value, mode: "tertiaire" })}
                icon={<Briefcase className="h-3 w-3" />}
                label="Tertiaire"
              />
              <ModeBtn
                active={value.mode === "maisons"}
                onClick={() => onChange({ ...value, mode: "maisons" })}
                icon={<Home className="h-3 w-3" />}
                label="Maisons"
              />
              <ModeBtn
                active={value.mode === "appartements"}
                onClick={() => onChange({ ...value, mode: "appartements" })}
                icon={<Building className="h-3 w-3" />}
                label="Apparts"
              />
              <ModeBtn
                active={value.mode === "both"}
                onClick={() => onChange({ ...value, mode: "both" })}
                icon={null}
                label="Tous"
              />
            </div>

            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={value.q}
                onChange={(e) => onChange({ ...value, q: e.target.value })}
                onKeyDown={(e) => e.key === "Enter" && onSubmit()}
                placeholder={
                  value.mode === "tertiaire"
                    ? "Adresse, nom bâtiment, n° DPE…"
                    : value.mode === "maisons"
                    ? "Adresse, n° DPE…"
                    : "Adresse, nom de copro, n° immatriculation…"
                }
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
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </div>

            {/* Commune : utile pour maisons + tertiaire */}
            {(showsMaisons || showsTertiaire) ? (
              <Input
                value={value.commune}
                onChange={(e) => onChange({ ...value, commune: e.target.value })}
                placeholder="Commune (ex. Romainville, Paris 11e…)"
              />
            ) : null}

            {/* Syndic : copros uniquement */}
            {showsCopros && !showsTertiaire ? (
              <Input
                value={value.syndic}
                onChange={(e) => onChange({ ...value, syndic: e.target.value })}
                placeholder="Syndic / gestionnaire"
              />
            ) : null}

            {/* Secteur d'activité tertiaire */}
            {showsTertiaire ? (
              <select
                value={value.secteurTertiaire}
                onChange={(e) => onChange({ ...value, secteurTertiaire: e.target.value })}
                className="h-10 w-full rounded-lg border border-input bg-background px-3 text-sm"
              >
                <option value="">Tous secteurs d'activité</option>
                {SECTEURS_TERTIAIRE.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            ) : null}

            {/* Classe DPE multi-select */}
            <div>
              <div className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Classe DPE
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
                        active ? "scale-110 ring-2 ring-foreground/70 ring-offset-1" : "opacity-50 hover:opacity-100",
                      )}
                      aria-pressed={active}
                    >
                      <DpeBadge classe={c} />
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Surface min/max (tertiaire + maisons/apparts) */}
            {(showsTertiaire || showsMaisons) ? (
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type="number"
                  min={0}
                  value={value.surfaceMin ?? ""}
                  onChange={(e) =>
                    onChange({ ...value, surfaceMin: e.target.value === "" ? null : Number(e.target.value) })
                  }
                  placeholder="Surface min m²"
                />
                <Input
                  type="number"
                  min={0}
                  value={value.surfaceMax ?? ""}
                  onChange={(e) =>
                    onChange({ ...value, surfaceMax: e.target.value === "" ? null : Number(e.target.value) })
                  }
                  placeholder="Surface max m²"
                />
              </div>
            ) : null}

            {/* Année construction min/max (tertiaire + maisons/apparts) */}
            {(showsTertiaire || showsMaisons) ? (
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type="number"
                  min={1700}
                  max={new Date().getFullYear()}
                  value={value.yearMin ?? ""}
                  onChange={(e) =>
                    onChange({ ...value, yearMin: e.target.value === "" ? null : Number(e.target.value) })
                  }
                  placeholder="Année construct. min"
                />
                <Input
                  type="number"
                  min={1700}
                  max={new Date().getFullYear()}
                  value={value.yearMax ?? ""}
                  onChange={(e) =>
                    onChange({ ...value, yearMax: e.target.value === "" ? null : Number(e.target.value) })
                  }
                  placeholder="Année construct. max"
                />
              </div>
            ) : null}

            {/* Propriétaire / occupant (recherche texte) */}
            {(showsTertiaire || showsCopros) ? (
              <Input
                value={value.proprietaire}
                onChange={(e) => onChange({ ...value, proprietaire: e.target.value })}
                placeholder={showsTertiaire ? "Occupant / société (nom)" : "Syndic / propriétaire (nom)"}
              />
            ) : null}

            {/* Lots min + Période : copros uniquement */}
            {showsCopros && !showsTertiaire ? (
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type="number"
                  min={0}
                  value={value.minLots ?? ""}
                  onChange={(e) =>
                    onChange({ ...value, minLots: e.target.value === "" ? null : Number(e.target.value) })
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
            ) : null}

            {/* Mini-aide */}
            {showsMaisons && !showsTertiaire ? (
              <p className="rounded-md bg-blue-50 px-2 py-1.5 text-[11px] text-blue-900">
                💡 Pour les maisons, indique un <strong>code postal</strong> ou une <strong>commune</strong>
              </p>
            ) : null}

            {showsTertiaire ? (
              <p className="rounded-md bg-amber-50 px-2 py-1.5 text-[11px] text-amber-900">
                💡 80k bâtiments tertiaires IDF — combine code postal/dept + DPE + secteur pour cibler.
              </p>
            ) : null}

            <div className="flex items-center justify-between gap-2 pt-1">
              <Button variant="ghost" size="sm" onClick={onReset}>
                <X className="h-3.5 w-3.5" />
                Réinitialiser
              </Button>
              <Button size="sm" onClick={onSubmit} className="flex-1">
                Rechercher ({resultCount})
              </Button>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function ModeBtn({
  active,
  onClick,
  icon,
  label,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  label: string;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex flex-1 items-center justify-center gap-1 rounded px-1.5 py-1.5 text-[10px] font-semibold transition-colors",
        active
          ? "bg-primary text-primary-foreground shadow-sm"
          : "text-muted-foreground hover:bg-secondary hover:text-foreground",
      )}
    >
      {icon}
      {label}
    </button>
  );
}

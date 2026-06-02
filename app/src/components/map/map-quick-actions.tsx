"use client";
/**
 * Pastilles flottantes sur la carte /map :
 *   - Bouton "Passoires F+G" qui coche les classes F et G en 1 clic
 *   - Légende DPE A→G+NC fixe (collapsible)
 *
 * Positionnée top-left de la carte. Indépendant de FiltersBar (qui reste
 * à gauche en plein) pour ne pas surcharger l'UI.
 */
import { useState } from "react";
import { Flame, Palette, ChevronUp, ChevronDown } from "lucide-react";
import { DPE_COLORS } from "@/components/annuaire/annuaire-map";

const DPE_LIST = ["A", "B", "C", "D", "E", "F", "G", "NC"] as const;

export function MapQuickActions({
  dpeClasses,
  onSetDpe,
  onSubmit,
}: {
  dpeClasses: string[];
  onSetDpe: (cls: string[]) => void;
  onSubmit: () => void;
}) {
  const [legendOpen, setLegendOpen] = useState(true);

  const isFGActive =
    dpeClasses.length === 2 &&
    dpeClasses.includes("F") &&
    dpeClasses.includes("G");

  const togglePassoires = () => {
    if (isFGActive) {
      onSetDpe([]);
    } else {
      onSetDpe(["F", "G"]);
    }
    // Lance la recherche après changement
    setTimeout(onSubmit, 50);
  };

  return (
    <div className="pointer-events-none absolute left-4 top-4 z-10 flex flex-col gap-2">
      {/* Bouton Passoires F+G */}
      <button
        onClick={togglePassoires}
        className={
          "pointer-events-auto inline-flex items-center gap-1.5 rounded-full border-2 px-3 py-1.5 text-xs font-bold shadow-md backdrop-blur transition-colors " +
          (isFGActive
            ? "border-rose-700 bg-rose-600 text-white"
            : "border-rose-300 bg-white text-rose-900 hover:bg-rose-50")
        }
        title="Filtrer en 1 clic les copros classées F ou G (passoires énergétiques)"
      >
        <Flame className="h-3.5 w-3.5" />
        {isFGActive ? "Filtre F+G actif (clic pour annuler)" : "🎯 Passoires F+G"}
      </button>

      {/* Légende DPE */}
      <div className="pointer-events-auto overflow-hidden rounded-lg border border-border bg-card/95 shadow-md backdrop-blur">
        <button
          onClick={() => setLegendOpen((v) => !v)}
          className="flex w-full items-center justify-between gap-2 px-2 py-1 text-[11px] font-semibold text-foreground hover:bg-secondary/40"
        >
          <span className="flex items-center gap-1.5">
            <Palette className="h-3 w-3 text-primary" />
            Légende DPE
          </span>
          {legendOpen ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        </button>
        {legendOpen ? (
          <div className="space-y-0.5 border-t border-border bg-card/95 px-2 py-1.5">
            {DPE_LIST.map((cls) => (
              <div key={cls} className="flex items-center gap-2 text-[11px]">
                <span
                  className="inline-block h-3 w-3 rounded-full ring-1 ring-white"
                  style={{ background: DPE_COLORS[cls] }}
                />
                <span className="font-semibold">{cls}</span>
                <span className="ml-auto text-[10px] text-muted-foreground">
                  {LABELS[cls]}
                </span>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

const LABELS: Record<string, string> = {
  A: "≤ 70",
  B: "≤ 110",
  C: "≤ 180",
  D: "≤ 250",
  E: "≤ 330",
  F: "≤ 420",
  G: "> 420",
  NC: "non classé",
};

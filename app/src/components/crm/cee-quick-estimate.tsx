"use client";
import { useMemo, useState } from "react";
import { Coins, Zap, Info, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";
import { quickEstimate, type QuickEstimateInput, type Evaluation } from "@/lib/services/cee/engine";

type ClasseDpe = "A" | "B" | "C" | "D" | "E" | "F" | "G" | "NC";

export function CeeQuickEstimate({
  typeBatiment,
  classeDpe,
  surface,
  postalCode,
  constructionYear,
}: {
  typeBatiment: "maison" | "appartement";
  classeDpe: string;
  surface: number | null;
  postalCode: string | null;
  constructionYear: number | null;
}) {
  const [expanded, setExpanded] = useState(false);
  const housingType =
    typeBatiment === "appartement" ? "Appartement" : "Maison individuelle";

  const result = useMemo(() => {
    if (!surface || !postalCode || !constructionYear) {
      return {
        applicable: false,
        reason:
          "Données projet incomplètes (surface, code postal ou année de construction).",
        housingType,
        scenarios: [],
      };
    }
    const input: QuickEstimateInput = {
      housingType,
      surface,
      postalCode,
      constructionYear,
      classeDpe: (classeDpe?.toUpperCase() as ClasseDpe) || "NC",
      mwhCumacPrice: 7, // marché 2026 ~7€/MWh cumac, conservateur
      mwhCumacPricePrecarious: 9,
    };
    return quickEstimate(input);
  }, [housingType, surface, postalCode, constructionYear, classeDpe]);

  if (!result.applicable) {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
        <div className="mb-1 flex items-center gap-1 font-semibold">
          <Info className="h-3.5 w-3.5" />
          Estimation CEE — non disponible
        </div>
        <p>{result.reason}</p>
      </div>
    );
  }

  // Le scénario "2 sauts" comme tête de gondole. Les autres dépliables.
  const head = result.scenarios[0];
  const others = result.scenarios.slice(1);

  return (
    <div className="rounded-xl border border-emerald-200 bg-gradient-to-br from-emerald-50 to-white p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-600 text-white">
            <Coins className="h-4 w-4" />
          </div>
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-wider text-emerald-700">
              Estimation prime CEE
            </div>
            <div className="text-sm font-bold text-emerald-900">
              Rénovation d'ampleur ({result.housingType === "Maison individuelle" ? "BAR-TH-174" : "BAR-TH-175"})
            </div>
          </div>
        </div>
      </div>

      {head ? <ScenarioRow scenario={head} highlight /> : null}

      {others.length > 0 ? (
        <>
          <button
            type="button"
            onClick={() => setExpanded(!expanded)}
            className="mt-2 flex w-full items-center justify-center gap-1 rounded-md py-1.5 text-[11px] font-semibold text-emerald-700 hover:bg-emerald-100"
          >
            {expanded ? (
              <>
                <ChevronUp className="h-3 w-3" /> Masquer les autres scénarios
              </>
            ) : (
              <>
                <ChevronDown className="h-3 w-3" /> Voir {others.length} autres scénarios
              </>
            )}
          </button>
          {expanded ? (
            <div className="mt-2 space-y-2">
              {others.map((s) => (
                <ScenarioRow key={s.classJumpCount} scenario={s} />
              ))}
            </div>
          ) : null}
        </>
      ) : null}

      <div className="mt-3 rounded-lg bg-emerald-100/50 p-2 text-[10px] leading-relaxed text-emerald-900/80">
        💡 <strong>Hypothèses :</strong> résidence principale, prix CEE = 7&nbsp;€/MWh cumac (marché conservateur 2026),
        précaire = 9&nbsp;€/MWh cumac. Le bonus "Coup de pouce ×2" s'applique aux ménages
        modestes ou très modestes hors parcours Anah. Estimation indicative — un audit
        énergétique préalable est requis avant signature.
      </div>
    </div>
  );
}

function ScenarioRow({
  scenario,
  highlight,
}: {
  scenario: {
    label: string;
    classJumpCount: "2" | "3" | "4 ou plus";
    evaluationStandard: Evaluation;
    evaluationCoupDePouce: Evaluation;
  };
  highlight?: boolean;
}) {
  const std = scenario.evaluationStandard;
  const cdp = scenario.evaluationCoupDePouce;

  return (
    <div
      className={cn(
        "rounded-lg border bg-card p-3",
        highlight ? "border-emerald-300 shadow-sm" : "border-border",
      )}
    >
      <div className="mb-2 flex items-center justify-between">
        <div className="text-xs font-bold text-foreground">{scenario.label}</div>
        <Badge status={std.status} />
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="rounded-md bg-slate-100 p-2">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-600">
            Ménages intermédiaires/supérieurs
          </div>
          <div className="mt-0.5 text-base font-black text-slate-900">
            {formatEuros(std.euroAmount)}
          </div>
          <div className="text-[10px] text-slate-600">
            ({formatKwh(std.kwhCumac)} kWh cumac)
          </div>
        </div>
        <div className="rounded-md bg-emerald-100 p-2">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-emerald-700">
            Ménages modestes (+ Coup de pouce ×2)
          </div>
          <div className="mt-0.5 text-base font-black text-emerald-900">
            {formatEuros(cdp.euroAmount)}
          </div>
          <div className="text-[10px] text-emerald-800/80">
            ({formatKwh(cdp.kwhCumac)} kWh cumac)
          </div>
        </div>
      </div>

      {std.calculationLabel ? (
        <p className="mt-2 font-mono text-[10px] text-muted-foreground">
          {std.calculationLabel}
        </p>
      ) : null}
    </div>
  );
}

function Badge({ status }: { status: string }) {
  const color =
    status === "Eligible"
      ? "bg-emerald-600 text-white"
      : status === "Eligibilite a confirmer"
        ? "bg-amber-500 text-white"
        : status === "Non eligible"
          ? "bg-red-600 text-white"
          : "bg-slate-400 text-white";
  const label =
    status === "Eligibilite a confirmer" ? "À confirmer" : status;
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider",
        color,
      )}
    >
      {label}
    </span>
  );
}

function formatEuros(v: number | null): string {
  if (v == null) return "—";
  return new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(v);
}

function formatKwh(v: number | null): string {
  if (v == null) return "—";
  return Math.round(v).toLocaleString("fr-FR");
}

void Zap; // hint au tree-shaker

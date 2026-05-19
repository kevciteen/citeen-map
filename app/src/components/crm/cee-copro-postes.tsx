"use client";
import { useMemo, useState } from "react";
import {
  Coins,
  Info,
  Flame,
  Shield,
  Wind,
  Layers,
  Cog,
  ChevronDown,
  ChevronUp,
  Sparkles,
  Pencil,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import {
  evaluateAllSheets,
  groupByFamily,
  sumEstimates,
  type FullEvaluateResult,
} from "@/lib/services/cee/engine";
import type { Action, Project } from "@/lib/services/cee/types";

type IndividualLite = {
  energie_principale_chauffage?: string | null;
  classe?: string;
  surface?: number | null;
};

type SheetFamily =
  | "Bouquet"
  | "Enveloppe"
  | "Thermique"
  | "Equipement"
  | "Services";

type IncomeKey = "veryModest" | "modest" | "intermediate" | "high";

/**
 * Estimation CEE complète pour une copropriété d'habitation.
 *
 * Comportement :
 *  - Cache les fiches "Non éligibles" et "Potentiellement éligibles"
 *  - Affiche uniquement Éligibles + À confirmer
 *  - Permet de compléter les champs manquants sur les fiches "À confirmer"
 *    via un mini formulaire dynamique → recalcul live
 *  - Prix kWh cumac saisissable dans le bandeau (par défaut 7 € marché)
 *  - Affiche les coups de pouce (×2, ×3, ×4, ×5) avec détail standard vs bonifié
 */
export function CeeCoproPostes({
  classeDpeCollective,
  periodeConstruction,
  nbLotsHabitation,
  codePostal,
  matchedIndividuals,
}: {
  classeDpeCollective: string | null;
  periodeConstruction: string | null;
  nbLotsHabitation: number | null;
  codePostal: string | null;
  matchedIndividuals?: IndividualLite[];
}) {
  const [income, setIncome] = useState<IncomeKey>("intermediate");
  const [priceStd, setPriceStd] = useState<string>("7");
  const [pricePrecaire, setPricePrecaire] = useState<string>("9");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [actionsOverride, setActionsOverride] = useState<Record<string, Action>>(
    {},
  );

  // Énergie chauffage dominante depuis DPE matchés
  const aggregates = useMemo(() => {
    if (!matchedIndividuals || matchedIndividuals.length === 0) {
      return {
        energieDominante: null as string | null,
        surfaceMoyenne: null as number | null,
        partPassoires: 0,
      };
    }
    const energyCount = new Map<string, number>();
    let surfaceSum = 0;
    let surfaceN = 0;
    let passoires = 0;
    for (const ind of matchedIndividuals) {
      const e = (ind.energie_principale_chauffage ?? "").toLowerCase().trim();
      if (e) energyCount.set(e, (energyCount.get(e) ?? 0) + 1);
      if (ind.surface && Number.isFinite(ind.surface)) {
        surfaceSum += ind.surface;
        surfaceN += 1;
      }
      const c = (ind.classe ?? "").toUpperCase();
      if (c === "F" || c === "G") passoires += 1;
    }
    let dominant: string | null = null;
    let maxCount = 0;
    for (const [e, c] of energyCount) {
      if (c > maxCount) {
        maxCount = c;
        dominant = e;
      }
    }
    return {
      energieDominante: dominant,
      surfaceMoyenne: surfaceN > 0 ? surfaceSum / surfaceN : null,
      partPassoires: passoires / matchedIndividuals.length,
    };
  }, [matchedIndividuals]);

  const project = useMemo<Project>(() => {
    const yearFromPeriod = mapPeriodToYear(periodeConstruction);
    const totalSurface =
      aggregates.surfaceMoyenne && nbLotsHabitation
        ? Math.round(aggregates.surfaceMoyenne * nbLotsHabitation)
        : nbLotsHabitation
          ? nbLotsHabitation * 60
          : 0;
    return {
      buildingType: "Habitation",
      housingType: "Batiment d'habitation collectif en copropriete",
      postalCode: codePostal ?? undefined,
      constructionYear: yearFromPeriod ?? undefined,
      buildingSurface: totalSurface || undefined,
      householdSize: 3,
      incomeBracket: income,
      mwhCumacPrice: Number.parseFloat(priceStd.replace(",", ".")) || 7,
      mwhCumacPricePrecarious:
        Number.parseFloat(pricePrecaire.replace(",", ".")) || 9,
      projectSystemHeating: true,
      projectSystemDhw: true,
      projectSystemVentilation: true,
    };
  }, [
    codePostal,
    periodeConstruction,
    aggregates.surfaceMoyenne,
    nbLotsHabitation,
    income,
    priceStd,
    pricePrecaire,
  ]);

  // Actions par défaut + overrides utilisateur
  const actionsMerged = useMemo<Record<string, Action>>(() => {
    const dpe = (classeDpeCollective ?? "").toUpperCase();
    const classJump =
      dpe === "G" ? "4 ou plus" : dpe === "F" ? "3" : dpe === "E" ? "2" : "2";
    const defaults: Record<string, Action> = {
      "BAR-TH-174": {
        classJumpCount: classJump,
        priorWorkStageDone: "Non",
        secondaryResidence: "Non",
        anahRenovationRoute: "Non",
      },
      "BAR-TH-175": {
        classJumpCount: classJump,
        priorWorkStageDone: "Non",
        secondaryResidence: "Non",
        anahRenovationRoute: "Non",
      },
      "BAR-TH-177": { classJumpCount: classJump },
    };
    // Merge overrides over defaults
    const merged: Record<string, Action> = { ...defaults };
    for (const [code, override] of Object.entries(actionsOverride)) {
      merged[code] = { ...(defaults[code] ?? {}), ...override };
    }
    return merged;
  }, [classeDpeCollective, actionsOverride]);

  const results = useMemo(() => {
    return evaluateAllSheets(project, {
      buildingTypes: ["Habitation"],
      actions: actionsMerged,
    });
  }, [project, actionsMerged]);

  // Filtre : on cache Non éligible + Potentiellement éligible
  const visibleResults = useMemo(
    () =>
      results.filter(
        (r) =>
          r.evaluation.status === "Eligible" ||
          r.evaluation.status === "Eligibilite a confirmer",
      ),
    [results],
  );

  const grouped = useMemo(
    () => groupByFamily(visibleResults),
    [visibleResults],
  );

  const totals = useMemo(() => {
    const eligibles = visibleResults.filter(
      (r) => r.evaluation.status === "Eligible" && r.evaluation.kwhCumac != null,
    );
    return sumEstimates(eligibles);
  }, [visibleResults]);

  const eligibleCount = visibleResults.filter(
    (r) => r.evaluation.status === "Eligible",
  ).length;
  const confirmCount = visibleResults.filter(
    (r) => r.evaluation.status === "Eligibilite a confirmer",
  ).length;

  if (visibleResults.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
        <div className="mb-1 flex items-center gap-1 font-semibold">
          <Info className="h-3.5 w-3.5" />
          Aucune fiche éligible ou à confirmer
        </div>
        <p>
          Vérifie que le DPE collectif est chargé (bouton "Recalculer depuis
          l'ADEME" plus haut) et que la période de construction est renseignée.
        </p>
      </div>
    );
  }

  const familyOrder: SheetFamily[] = [
    "Thermique",
    "Enveloppe",
    "Equipement",
    "Services",
  ];

  const updateAction = (code: string, key: string, value: unknown) => {
    setActionsOverride((prev) => ({
      ...prev,
      [code]: { ...(prev[code] ?? {}), [key]: value },
    }));
  };

  return (
    <div className="space-y-3">
      {/* Bandeau principal */}
      <div className="rounded-lg bg-gradient-to-br from-emerald-50 to-white p-3 ring-1 ring-emerald-200">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="mb-1 flex items-center gap-1 text-xs font-bold text-emerald-900">
              <Coins className="h-3.5 w-3.5" />
              Estimation CEE — fiches éligibles uniquement
            </div>
            <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
              <StatusPill count={eligibleCount} label="éligibles" tone="emerald" />
              <StatusPill count={confirmCount} label="à confirmer" tone="amber" />
            </div>
            {totals.kwhCumac > 0 ? (
              <div className="mt-2 flex items-baseline gap-2">
                <span className="text-2xl font-black tabular-nums text-emerald-900">
                  {Math.round(totals.kwhCumac / 1000).toLocaleString("fr-FR")} MWh cumac
                </span>
                <span className="text-sm font-bold text-emerald-700">
                  ≈ {formatEuros(totals.euroAmount)} de prime CEE
                </span>
              </div>
            ) : null}
            <p className="mt-1 text-[10px] text-emerald-800/70">
              Somme des fiches éligibles · revenus <strong>{income}</strong>
              {aggregates.energieDominante
                ? ` · énergie dominante "${aggregates.energieDominante}"`
                : ""}
            </p>
          </div>
          <div className="flex flex-col gap-2">
            <div className="flex flex-col gap-1">
              <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Revenus copro
              </label>
              <select
                value={income}
                onChange={(e) => setIncome(e.target.value as IncomeKey)}
                className="h-7 rounded-md border border-border bg-background px-2 text-[11px]"
              >
                <option value="veryModest">Très modestes</option>
                <option value="modest">Modestes</option>
                <option value="intermediate">Intermédiaires</option>
                <option value="high">Supérieurs</option>
              </select>
            </div>
            <div className="flex gap-2">
              <div className="flex flex-col gap-1">
                <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Prix CEE std (€/MWh)
                </label>
                <Input
                  value={priceStd}
                  onChange={(e) => setPriceStd(e.target.value)}
                  type="number"
                  step="0.1"
                  className="h-7 w-20 text-[11px]"
                />
              </div>
              <div className="flex flex-col gap-1">
                <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Prix CEE précaire (€/MWh)
                </label>
                <Input
                  value={pricePrecaire}
                  onChange={(e) => setPricePrecaire(e.target.value)}
                  type="number"
                  step="0.1"
                  className="h-7 w-20 text-[11px]"
                />
              </div>
            </div>
          </div>
        </div>
      </div>

      <p className="text-[10px] italic text-muted-foreground">
        Estimation indicative — audit énergétique recommandé avant engagement.
        Décisions AG : majorité absolue (art. 24) ou article 25 selon les
        travaux. Cumul possible avec MaPrimeRénov' Copro selon revenus.
      </p>

      {/* Listing par famille */}
      {familyOrder.map((family) => {
        const items = grouped[family] ?? [];
        if (items.length === 0) return null;
        const isOpen = expanded[family] ?? family === "Thermique";
        return (
          <div key={family}>
            <button
              type="button"
              onClick={() =>
                setExpanded((p) => ({ ...p, [family]: !isOpen }))
              }
              className="mb-1.5 flex w-full items-center justify-between rounded-md px-1.5 py-1 hover:bg-secondary"
            >
              <span className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                {familyIcon(family)}
                {family} · {items.length}
              </span>
              {isOpen ? (
                <ChevronUp className="h-3 w-3 text-muted-foreground" />
              ) : (
                <ChevronDown className="h-3 w-3 text-muted-foreground" />
              )}
            </button>
            {isOpen ? (
              <div className="space-y-1.5">
                {items.map((r) => (
                  <SheetRow
                    key={r.sheet.code}
                    result={r}
                    currentAction={actionsMerged[r.sheet.code] ?? {}}
                    onUpdateAction={(key, value) =>
                      updateAction(r.sheet.code, key, value)
                    }
                  />
                ))}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

// === Sub-components ====================================================

function SheetRow({
  result,
  currentAction,
  onUpdateAction,
}: {
  result: FullEvaluateResult;
  currentAction: Action;
  onUpdateAction: (key: string, value: unknown) => void;
}) {
  const { evaluation, sheet } = result;
  const isEligible = evaluation.status === "Eligible";
  const [editing, setEditing] = useState(false);
  const cdp = evaluation.coupDePouce;

  return (
    <div
      className={cn(
        "rounded-lg border bg-card px-3 py-2 text-xs",
        isEligible
          ? "border-emerald-300 bg-emerald-50/40"
          : "border-amber-200 bg-amber-50/30",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="font-mono text-[10px] font-bold text-muted-foreground">
              {sheet.code}
            </span>
            <span className="text-xs font-bold text-foreground">
              {sheet.title}
            </span>
          </div>
          {evaluation.calculationLabel ? (
            <p className="mt-1 font-mono text-[10px] text-muted-foreground">
              {evaluation.calculationLabel}
            </p>
          ) : null}
          {evaluation.missing.length > 0 ? (
            <ul className="mt-1 space-y-0.5">
              {evaluation.missing.slice(0, 3).map((m, i) => (
                <li key={i} className="text-[10px] text-amber-800">
                  ⚠ {m}
                </li>
              ))}
            </ul>
          ) : null}
          {/* Coup de pouce */}
          {cdp ? (
            <div className="mt-1.5 rounded bg-purple-100 px-2 py-1 text-[10px] text-purple-900">
              <div className="flex items-center gap-1 font-semibold">
                <Sparkles className="h-3 w-3" />
                {cdp.name}
                {cdp.factor ? (
                  <span className="ml-1 rounded-full bg-purple-700 px-1.5 py-0 text-[9px] font-bold text-white">
                    ×{cdp.factor}
                  </span>
                ) : null}
                <span
                  className={cn(
                    "ml-auto rounded-full px-1.5 py-0 text-[9px] font-bold",
                    cdp.status === "Eligible" ||
                      String(cdp.status) === "Applicable"
                      ? "bg-emerald-600 text-white"
                      : "bg-amber-500 text-white",
                  )}
                >
                  {cdp.status}
                </span>
              </div>
              {evaluation.standardKwhCumac != null &&
              evaluation.kwhCumac != null &&
              evaluation.kwhCumac > evaluation.standardKwhCumac ? (
                <p className="mt-0.5 font-mono text-[9px] opacity-80">
                  Standard : {formatKwh(evaluation.standardKwhCumac)} →
                  bonifié : {formatKwh(evaluation.kwhCumac)} kWh cumac
                </p>
              ) : null}
            </div>
          ) : null}
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          {evaluation.kwhCumac != null ? (
            <>
              <span className="text-sm font-black tabular-nums text-emerald-900">
                {formatEuros(evaluation.euroAmount)}
              </span>
              <span className="text-[10px] tabular-nums text-muted-foreground">
                {formatKwh(evaluation.kwhCumac)} kWh cumac
              </span>
            </>
          ) : null}
          <StatusBadge status={evaluation.status} />
          {!isEligible || evaluation.missing.length > 0 ? (
            <button
              type="button"
              onClick={() => setEditing(!editing)}
              className="flex items-center gap-0.5 rounded-md border border-amber-300 bg-amber-100 px-1.5 py-0.5 text-[10px] font-semibold text-amber-900 hover:bg-amber-200"
            >
              <Pencil className="h-2.5 w-2.5" />
              {editing ? "Fermer" : "Compléter"}
            </button>
          ) : null}
        </div>
      </div>

      {/* Mini formulaire dynamique */}
      {editing ? (
        <ActionEditor
          code={sheet.code}
          missing={evaluation.missing}
          currentAction={currentAction}
          onUpdate={onUpdateAction}
        />
      ) : null}
    </div>
  );
}

/**
 * Mini formulaire dynamique : pour chaque champ identifié dans les `missing`
 * strings, on génère un input adapté. Heuristique simple basée sur les
 * patterns du simulateur source.
 */
function ActionEditor({
  code,
  missing,
  currentAction,
  onUpdate,
}: {
  code: string;
  missing: string[];
  currentAction: Action;
  onUpdate: (key: string, value: unknown) => void;
}) {
  // Inputs détectés depuis les missing
  const inputs = inferInputsFromMissing(code, missing);

  if (inputs.length === 0) {
    return (
      <div className="mt-2 rounded bg-amber-100/60 p-2 text-[10px] text-amber-900">
        Cette fiche nécessite des paramètres spécifiques que le simulateur
        rapide ne couvre pas. Pour un calcul complet, utilise la page
        /simulateur-cee dédiée (à venir).
      </div>
    );
  }

  return (
    <div className="mt-2 rounded-md bg-amber-100/60 p-2">
      <div className="mb-1.5 text-[10px] font-bold uppercase tracking-wider text-amber-900">
        Compléter pour calculer
      </div>
      <div className="grid grid-cols-2 gap-2">
        {inputs.map((input) => (
          <div key={input.key} className="flex flex-col gap-0.5">
            <label className="text-[9px] font-semibold text-amber-900">
              {input.label}
            </label>
            {input.kind === "select" ? (
              <select
                value={String(currentAction[input.key] ?? "")}
                onChange={(e) => onUpdate(input.key, e.target.value)}
                className="h-7 rounded-md border border-amber-300 bg-white px-2 text-[11px]"
              >
                <option value="">Choisir…</option>
                {(input.options ?? []).map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            ) : (
              <Input
                value={String(currentAction[input.key] ?? "")}
                onChange={(e) => onUpdate(input.key, e.target.value)}
                type={input.kind === "number" ? "number" : "text"}
                className="h-7 text-[11px]"
              />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

// === Heuristique missing → input ========================================

interface InputSpec {
  key: string;
  label: string;
  kind: "text" | "number" | "select";
  options?: string[];
}

function inferInputsFromMissing(code: string, missing: string[]): InputSpec[] {
  const specs: InputSpec[] = [];
  const seen = new Set<string>();
  const add = (spec: InputSpec) => {
    if (seen.has(spec.key)) return;
    seen.add(spec.key);
    specs.push(spec);
  };

  for (const m of missing) {
    const lc = m.toLowerCase();
    if (lc.includes("surface") && lc.includes("logement")) {
      add({ key: "buildingSurface", label: "Surface (m²)", kind: "number" });
    } else if (lc.includes("surface") && lc.includes("chauffee")) {
      add({ key: "heatedSurface", label: "Surface chauffée (m²)", kind: "number" });
    } else if (lc.includes("saut") && lc.includes("classe")) {
      if (lc.includes("premiere etape")) {
        add({
          key: "firstStageClassJumpCount",
          label: "Sauts de classe 1ère étape",
          kind: "select",
          options: ["1", "2", "3"],
        });
      } else {
        add({
          key: "classJumpCount",
          label: "Nombre de sauts de classe",
          kind: "select",
          options: ["2", "3", "4 ou plus"],
        });
      }
    } else if (lc.includes("annee de construction")) {
      add({
        key: "constructionYear",
        label: "Année construction",
        kind: "number",
      });
    } else if (lc.includes("code postal")) {
      add({ key: "postalCode", label: "Code postal", kind: "text" });
    } else if (lc.includes("type d'habitation") || lc.includes("type d habitation")) {
      add({
        key: "housingType",
        label: "Type d'habitation",
        kind: "select",
        options: [
          "Maison individuelle",
          "Appartement",
          "Batiment d'habitation collectif en monopropriete",
          "Batiment d'habitation collectif en copropriete",
        ],
      });
    } else if (lc.includes("residence principale") || lc.includes("residence secondaire")) {
      add({
        key: "coupDePoucePrimaryResidence",
        label: "Résidence principale ?",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else if (lc.includes("equipement remplace") || lc.includes("équipement remplacé")) {
      add({
        key: "coupDePouceReplacedEquipment",
        label: "Équipement remplacé",
        kind: "select",
        options: [
          "Chaudiere au charbon",
          "Chaudiere au fioul",
          "Chaudiere au gaz",
          "Autre",
        ],
      });
    } else if (lc.includes("reseau de chaleur")) {
      add({
        key: "coupDePouceHeatNetworkImpossible",
        label: "Raccordement RdC impossible ?",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else if (lc.includes("nombre de logements") || lc.includes("apartmentcount")) {
      add({ key: "apartmentCount", label: "Nombre de logements", kind: "number" });
    } else if (lc.includes("revenus")) {
      add({
        key: "incomeBracket",
        label: "Catégorie de revenus",
        kind: "select",
        options: ["veryModest", "modest", "intermediate", "high"],
      });
    } else if (lc.includes("anah") || lc.includes("maprimerenov")) {
      add({
        key: "anahRenovationRoute",
        label: "Parcours Anah/MPR Ampleur ?",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else if (
      lc.includes("usage") &&
      (lc.includes("pac") || lc.includes("chauffage") || code.includes("TH-163"))
    ) {
      add({
        key: "usage",
        label: "Usage de la PAC",
        kind: "select",
        options: ["Chauffage", "Chauffage + ECS", "ECS uniquement"],
      });
    } else if (lc.includes("puissance")) {
      add({
        key: "powerBand",
        label: "Bande de puissance",
        kind: "select",
        options: ["Inferieure ou egale a 400 kW", "Superieure a 400 kW"],
      });
    } else if (lc.includes("secteur")) {
      add({
        key: "sector",
        label: "Secteur d'activité",
        kind: "select",
        options: [
          "Bureaux",
          "Enseignement",
          "Commerces",
          "Hotellerie / Restauration",
          "Sante",
          "Autres secteurs",
        ],
      });
    }
  }

  return specs;
}

// === Utils communs ======================================================

function StatusPill({
  count,
  label,
  tone,
}: {
  count: number;
  label: string;
  tone: "emerald" | "amber" | "slate";
}) {
  if (count === 0) return null;
  const cls =
    tone === "emerald"
      ? "bg-emerald-600 text-white"
      : tone === "amber"
        ? "bg-amber-500 text-white"
        : "bg-slate-400 text-white";
  return (
    <span className={cn("rounded-full px-2 py-0.5 text-[10px] font-bold", cls)}>
      {count} {label}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  const cls =
    status === "Eligible"
      ? "bg-emerald-600 text-white"
      : status === "Eligibilite a confirmer"
        ? "bg-amber-500 text-white"
        : "bg-slate-400 text-white";
  const label =
    status === "Eligibilite a confirmer" ? "À compléter" : status;
  return (
    <span
      className={cn(
        "rounded-full px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider",
        cls,
      )}
    >
      {label}
    </span>
  );
}

function familyIcon(f: SheetFamily) {
  switch (f) {
    case "Enveloppe":
      return <Shield className="h-3 w-3 text-blue-600" />;
    case "Thermique":
      return <Flame className="h-3 w-3 text-orange-600" />;
    case "Equipement":
      return <Cog className="h-3 w-3 text-slate-600" />;
    case "Services":
      return <Wind className="h-3 w-3 text-sky-600" />;
    case "Bouquet":
      return <Layers className="h-3 w-3 text-emerald-700" />;
  }
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

function mapPeriodToYear(period: string | null): number | null {
  if (!period) return null;
  switch (period) {
    case "AVANT_1949":
      return 1930;
    case "DE_1949_A_1974":
      return 1965;
    case "DE_1975_A_1993":
      return 1985;
    case "DE_1994_A_2000":
      return 1997;
    case "DE_2001_A_2010":
      return 2005;
    case "APRES_2011":
      return 2015;
    default:
      return null;
  }
}

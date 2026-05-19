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

/**
 * Estimation CEE complète pour une copropriété d'habitation.
 *
 * - Cache les fiches "Non éligibles" et "Potentiellement éligibles"
 * - Affiche en parallèle Standard (revenus intermédiaires) ET Très modeste
 *   (avec Coup de pouce ×2/×3/×4/×5 selon fiche) pour chaque fiche
 * - Mini formulaire pour compléter les champs manquants des "À confirmer"
 *   → recalcul live → bascule en Éligible
 * - Prix kWh cumac standard et précaire saisissables dans le bandeau
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
  const [priceStd, setPriceStd] = useState<string>("7");
  const [pricePrecaire, setPricePrecaire] = useState<string>("9");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [actionsOverride, setActionsOverride] = useState<Record<string, Action>>({});

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

  // Project commun (toutes les valeurs sauf incomeBracket)
  const baseProject = useMemo<Omit<Project, "incomeBracket">>(() => {
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
    priceStd,
    pricePrecaire,
  ]);

  // Actions par défaut + overrides
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
        coupDePoucePrimaryResidence: "Oui",
      },
      "BAR-TH-175": {
        classJumpCount: classJump,
        priorWorkStageDone: "Non",
        secondaryResidence: "Non",
        anahRenovationRoute: "Non",
        coupDePoucePrimaryResidence: "Oui",
      },
      "BAR-TH-177": { classJumpCount: classJump },
    };
    const merged: Record<string, Action> = { ...defaults };
    for (const [code, override] of Object.entries(actionsOverride)) {
      merged[code] = { ...(defaults[code] ?? {}), ...override };
    }
    return merged;
  }, [classeDpeCollective, actionsOverride]);

  // === Évaluation 2 fois : intermediate + veryModest ===================
  const resultsStd = useMemo(
    () =>
      evaluateAllSheets(
        { ...baseProject, incomeBracket: "intermediate" } as Project,
        { buildingTypes: ["Habitation"], actions: actionsMerged },
      ),
    [baseProject, actionsMerged],
  );

  const resultsModest = useMemo(
    () =>
      evaluateAllSheets(
        { ...baseProject, incomeBracket: "veryModest" } as Project,
        { buildingTypes: ["Habitation"], actions: actionsMerged },
      ),
    [baseProject, actionsMerged],
  );

  // Index par code pour zip
  const modestByCode = useMemo(() => {
    const m = new Map<string, FullEvaluateResult>();
    for (const r of resultsModest) m.set(r.sheet.code, r);
    return m;
  }, [resultsModest]);

  // Filtre : on cache Non éligible + Potentiellement éligible (sur standard)
  const visibleResults = useMemo(
    () =>
      resultsStd.filter(
        (r) =>
          r.evaluation.status === "Eligible" ||
          r.evaluation.status === "Eligibilite a confirmer",
      ),
    [resultsStd],
  );

  const grouped = useMemo(
    () => groupByFamily(visibleResults),
    [visibleResults],
  );

  const totalsStd = useMemo(() => {
    return sumEstimates(
      visibleResults.filter((r) => r.evaluation.status === "Eligible"),
    );
  }, [visibleResults]);

  const totalsModest = useMemo(() => {
    const eligibleStdCodes = new Set(
      visibleResults
        .filter((r) => r.evaluation.status === "Eligible")
        .map((r) => r.sheet.code),
    );
    const matching = resultsModest.filter((r) =>
      eligibleStdCodes.has(r.sheet.code),
    );
    return sumEstimates(matching);
  }, [visibleResults, resultsModest]);

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
              <StatusPill count={confirmCount} label="à compléter" tone="amber" />
            </div>
            {totalsStd.kwhCumac > 0 ? (
              <div className="mt-2 grid grid-cols-2 gap-2">
                <div className="rounded-md bg-slate-100 p-2">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-700">
                    Revenus standard
                  </div>
                  <div className="mt-0.5 text-lg font-black tabular-nums text-slate-900">
                    {formatEuros(totalsStd.euroAmount)}
                  </div>
                  <div className="text-[10px] text-slate-600">
                    {Math.round(totalsStd.kwhCumac / 1000).toLocaleString("fr-FR")} MWh cumac
                  </div>
                </div>
                <div className="rounded-md bg-purple-100 p-2 ring-1 ring-purple-300">
                  <div className="flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-purple-700">
                    <Sparkles className="h-3 w-3" />
                    Ménages très modestes
                  </div>
                  <div className="mt-0.5 text-lg font-black tabular-nums text-purple-900">
                    {formatEuros(totalsModest.euroAmount)}
                  </div>
                  <div className="text-[10px] text-purple-700">
                    {Math.round(totalsModest.kwhCumac / 1000).toLocaleString("fr-FR")} MWh cumac · avec Coups de pouce
                  </div>
                </div>
              </div>
            ) : null}
            <p className="mt-1 text-[10px] text-emerald-800/70">
              Somme des fiches éligibles
              {aggregates.energieDominante
                ? ` · énergie chauffage dominante "${aggregates.energieDominante}"`
                : ""}
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Prix CEE (€/MWh cumac)
            </label>
            <div className="flex gap-2">
              <div className="flex flex-col gap-0.5">
                <span className="text-[9px] text-muted-foreground">Standard</span>
                <Input
                  value={priceStd}
                  onChange={(e) => setPriceStd(e.target.value)}
                  type="number"
                  step="0.1"
                  className="h-7 w-20 text-[11px]"
                />
              </div>
              <div className="flex flex-col gap-0.5">
                <span className="text-[9px] text-muted-foreground">Précaire</span>
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
        Audit énergétique recommandé avant engagement. Décisions AG : majorité
        absolue (art. 24) ou article 25 selon les travaux. Cumul possible avec
        MaPrimeRénov' Copro selon revenus.
      </p>

      {/* Liste par famille */}
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
                    resultStd={r}
                    resultModest={modestByCode.get(r.sheet.code) ?? r}
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
  resultStd,
  resultModest,
  currentAction,
  onUpdateAction,
}: {
  resultStd: FullEvaluateResult;
  resultModest: FullEvaluateResult;
  currentAction: Action;
  onUpdateAction: (key: string, value: unknown) => void;
}) {
  const { evaluation, sheet } = resultStd;
  const evMod = resultModest.evaluation;
  const isEligible = evaluation.status === "Eligible";
  const [editing, setEditing] = useState(false);
  const cdp = evMod.coupDePouce ?? evaluation.coupDePouce;

  // Toutes les missing sur les 2 évaluations (std + modeste). On les déduplique.
  const allMissing = useMemo(() => {
    const set = new Set<string>();
    for (const m of evaluation.missing) if (m) set.add(m);
    for (const m of evMod.missing) if (m) set.add(m);
    return [...set];
  }, [evaluation.missing, evMod.missing]);

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
          {allMissing.length > 0 ? (
            <ul className="mt-1 space-y-0.5">
              {allMissing.slice(0, 5).map((m, i) => (
                <li key={i} className="text-[10px] text-amber-800">
                  ⚠ {m}
                </li>
              ))}
              {allMissing.length > 5 ? (
                <li className="text-[10px] italic text-amber-700">
                  + {allMissing.length - 5} autres…
                </li>
              ) : null}
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
                  {String(cdp.status)}
                </span>
              </div>
            </div>
          ) : null}
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          {/* Deux colonnes de montants */}
          <div className="grid grid-cols-2 gap-1 text-right">
            <div>
              <div className="text-[9px] font-semibold uppercase tracking-wider text-slate-600">
                Standard
              </div>
              <div className="text-sm font-black tabular-nums text-slate-900">
                {formatEuros(evaluation.euroAmount)}
              </div>
              <div className="text-[9px] tabular-nums text-slate-500">
                {formatKwh(evaluation.kwhCumac)}
              </div>
            </div>
            <div>
              <div className="text-[9px] font-semibold uppercase tracking-wider text-purple-700">
                Très modeste
              </div>
              <div className="text-sm font-black tabular-nums text-purple-900">
                {formatEuros(evMod.euroAmount)}
              </div>
              <div className="text-[9px] tabular-nums text-purple-600">
                {formatKwh(evMod.kwhCumac)}
              </div>
            </div>
          </div>
          <StatusBadge status={evaluation.status} />
          {allMissing.length > 0 ? (
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

      {editing ? (
        <ActionEditor
          code={sheet.code}
          missing={allMissing}
          currentAction={currentAction}
          onUpdate={onUpdateAction}
        />
      ) : null}
    </div>
  );
}

/**
 * Mini formulaire dynamique pour compléter les `missing[]` d'une fiche.
 * Combine heuristique (patterns connus → inputs typés) + fallback (input
 * texte libre étiqueté avec la string missing). L'utilisateur peut donc
 * combler n'importe quel champ.
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
  const { typed, untyped } = useMemo(
    () => splitInputs(code, missing),
    [code, missing],
  );

  if (typed.length === 0 && untyped.length === 0) return null;

  return (
    <div className="mt-2 rounded-md bg-amber-100/60 p-2">
      <div className="mb-1.5 text-[10px] font-bold uppercase tracking-wider text-amber-900">
        Compléter pour calculer
      </div>
      <div className="grid grid-cols-2 gap-2">
        {typed.map((input) => (
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
        {/* Fallback : inputs texte libres pour les missing non-typés */}
        {untyped.map((mraw, idx) => {
          const key = makeKeyFromLabel(mraw);
          return (
            <div key={`untyped-${idx}`} className="col-span-2 flex flex-col gap-0.5">
              <label className="text-[9px] font-semibold text-amber-900">
                {mraw}
              </label>
              <Input
                value={String(currentAction[key] ?? "")}
                onChange={(e) => onUpdate(key, e.target.value)}
                type="text"
                placeholder="Saisir la valeur…"
                className="h-7 text-[11px]"
              />
              <span className="text-[9px] italic text-amber-700">
                champ libre · key = <code className="font-mono">{key}</code>
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// === Heuristique missing → input (étendue) =============================

interface InputSpec {
  key: string;
  label: string;
  kind: "text" | "number" | "select";
  options?: string[];
}

function splitInputs(
  code: string,
  missing: string[],
): { typed: InputSpec[]; untyped: string[] } {
  const typed: InputSpec[] = [];
  const untyped: string[] = [];
  const seenKeys = new Set<string>();
  const add = (spec: InputSpec) => {
    if (seenKeys.has(spec.key)) return;
    seenKeys.add(spec.key);
    typed.push(spec);
  };

  for (const m of missing) {
    const lc = m.toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
    let matched = true;
    if (lc.includes("surface") && (lc.includes("logement") || lc.includes("habit"))) {
      add({ key: "buildingSurface", label: "Surface (m²)", kind: "number" });
    } else if (lc.includes("surface") && lc.includes("chauf")) {
      add({ key: "heatedSurface", label: "Surface chauffée (m²)", kind: "number" });
    } else if (lc.includes("surface") && (lc.includes("refroid") || lc.includes("clim"))) {
      add({ key: "coolingSurface", label: "Surface refroidie (m²)", kind: "number" });
    } else if (lc.includes("surface") && lc.includes("isole")) {
      add({ key: "isolatedSurface", label: "Surface isolée (m²)", kind: "number" });
    } else if (lc.includes("surface") && lc.includes("vitre")) {
      add({ key: "glazedSurface", label: "Surface vitrée (m²)", kind: "number" });
    } else if (lc.includes("surface")) {
      add({ key: "surface", label: m, kind: "number" });
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
      add({ key: "constructionYear", label: "Année construction", kind: "number" });
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
    } else if (lc.includes("equipement remplace") || lc.includes("equipement remplac")) {
      add({
        key: "coupDePouceReplacedEquipment",
        label: "Équipement remplacé",
        kind: "select",
        options: ["Chaudiere au charbon", "Chaudiere au fioul", "Chaudiere au gaz", "Autre"],
      });
    } else if (lc.includes("reseau de chaleur") && lc.includes("impossib")) {
      add({
        key: "coupDePouceHeatNetworkImpossible",
        label: "Raccordement réseau de chaleur impossible ?",
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
        label: "Parcours Anah / MPR Ampleur ?",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else if (
      lc.includes("usage") &&
      (lc.includes("pac") || lc.includes("chauffage") || code.includes("TH-163"))
    ) {
      add({
        key: "usage",
        label: "Usage",
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
    } else if (lc.includes("etas") || lc.includes("rendement saisonnier")) {
      add({
        key: "etasBand",
        label: "Tranche d'Etas",
        kind: "select",
        options: [
          "Inferieur a 111 %",
          "De 111 % a moins de 126 %",
          "De 126 % a moins de 175 %",
          "175 % ou plus",
        ],
      });
    } else if (lc.includes("cop") && !lc.includes("scop")) {
      add({
        key: "copBand",
        label: "Tranche de COP",
        kind: "select",
        options: ["Inferieur a 3,4", "De 3,4 a moins de 4,5", "4,5 ou plus"],
      });
    } else if (lc.includes("scop")) {
      add({ key: "scop", label: "SCOP", kind: "number" });
    } else if (lc.includes("temperature")) {
      add({
        key: "temperatureType",
        label: "Type température",
        kind: "select",
        options: ["Basse temperature", "Moyenne ou haute temperature"],
      });
    } else if (lc.includes("resistance thermique") || lc.includes(" r ")) {
      add({ key: "thermalResistance", label: "Résistance R (m².K/W)", kind: "number" });
    } else if (lc.includes("coefficient u") || lc.includes("uw")) {
      add({ key: "uCoeff", label: "Coefficient U", kind: "number" });
    } else if (lc.includes("classe du systeme") || lc.includes("classe installee")) {
      add({
        key: "installedClass",
        label: "Classe du système installé",
        kind: "select",
        options: ["A", "B"],
      });
    } else if (lc.includes("zone climatique")) {
      add({
        key: "climateZone",
        label: "Zone climatique",
        kind: "select",
        options: ["H1", "H2", "H3"],
      });
    } else if (lc.includes("nature de l'operation") || lc.includes("nature operation")) {
      add({
        key: "operationMode",
        label: "Nature de l'opération",
        kind: "select",
        options: ["Achat d'un systeme neuf", "Amelioration d'un systeme existant"],
      });
    } else if (lc.includes("nombre de personnes") || lc.includes("foyer")) {
      add({ key: "householdSize", label: "Taille du foyer", kind: "number" });
    } else if (
      lc.includes("type d'energie") ||
      lc.includes("energie utilisee") ||
      lc.includes("combustible") ||
      lc.includes("electricite")
    ) {
      add({
        key: "energyType",
        label: "Type d'énergie",
        kind: "select",
        options: ["Combustible", "Electricite"],
      });
    } else if (lc.includes("ventilation")) {
      add({
        key: "ventilationType",
        label: "Type de ventilation",
        kind: "select",
        options: [
          "Simple flux autoreglable",
          "Simple flux hygroreglable Type A",
          "Simple flux hygroreglable Type B",
          "Double flux",
        ],
      });
    } else if (lc.includes("caisson")) {
      add({
        key: "caissonType",
        label: "Type de caisson",
        kind: "select",
        options: ["Caisson basse consommation", "Caisson standard", "Caisson basse pression"],
      });
    } else if (lc.includes("type de generateur") || lc.includes("type generateur")) {
      add({ key: "generatorType", label: "Type de générateur", kind: "text" });
    } else if (lc.includes("nombre d'occupants")) {
      add({ key: "occupantCount", label: "Nombre d'occupants", kind: "number" });
    } else if (lc.includes("nombre de chambres") || lc.includes("chambres equipees")) {
      add({ key: "roomCount", label: "Nombre de chambres", kind: "number" });
    } else if (lc.includes("nombre de douches") || lc.includes("douches raccord")) {
      add({ key: "showerCount", label: "Nombre de douches raccordées", kind: "number" });
    } else if (lc.includes("part de la pac") || lc.includes("pac power")) {
      add({
        key: "pacPowerSharePercent",
        label: "Part de la PAC (%)",
        kind: "number",
      });
    } else if (lc.includes("comfort") || lc.includes("confort")) {
      add({
        key: "comfortPresent",
        label: "Confort présent",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else if (lc.includes("equipement existant") && lc.includes("classe")) {
      add({
        key: "existingClassAtMostC",
        label: "Système existant ≤ classe C",
        kind: "select",
        options: ["Oui", "Non"],
      });
    } else {
      matched = false;
    }
    if (!matched) {
      untyped.push(m);
    }
  }

  return { typed, untyped };
}

function makeKeyFromLabel(label: string): string {
  return (
    label
      .normalize("NFD")
      .replace(/[̀-ͯ]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim()
      .split(" ")
      .slice(0, 4)
      .map((w, i) => (i === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1)))
      .join("") || "extraField"
  );
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

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
import { SHEET_FIELDS, type SheetField } from "@/lib/services/cee/sheet-fields";

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
  const defaultYear = mapPeriodToYear(periodeConstruction);
  const [yearOverride, setYearOverride] = useState<string>(
    defaultYear ? String(defaultYear) : "",
  );
  const defaultSurface = nbLotsHabitation
    ? Math.max(40, Math.round(60))
    : 60;
  const [surfaceOverride, setSurfaceOverride] = useState<string>(
    String(defaultSurface),
  );
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [actionsOverride, setActionsOverride] = useState<Record<string, Action>>({});
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set());

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

  // Project commun (housingType + incomeBracket sont injectés au moment d'évaluer)
  const baseProject = useMemo<Omit<Project, "incomeBracket" | "housingType">>(() => {
    const yearFromOverride = Number(yearOverride);
    const yearFromPeriod = mapPeriodToYear(periodeConstruction);
    const year = Number.isFinite(yearFromOverride) && yearFromOverride > 0
      ? yearFromOverride
      : (yearFromPeriod ?? undefined);
    const surfaceFromOverride = Number(surfaceOverride);
    const aptSurface = Number.isFinite(surfaceFromOverride) && surfaceFromOverride > 0
      ? surfaceFromOverride
      : (aggregates.surfaceMoyenne ? Math.round(aggregates.surfaceMoyenne) : 60);
    return {
      buildingType: "Habitation",
      postalCode: codePostal ?? undefined,
      constructionYear: year,
      // Surface "type" d'un appartement de la copro — utilisée par les fiches
      // qui calculent par lot. On la rend éditable depuis le bandeau.
      buildingSurface: aptSurface,
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
    priceStd,
    pricePrecaire,
    yearOverride,
    surfaceOverride,
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

  // === Évaluation 2 fois : intermediate + veryModest, et 2 housingTypes =
  //
  // On évalue avec housingType="Batiment d'habitation collectif en copropriete"
  // (pour les fiches purement collectives : BAR-TH-177, BAR-TH-179, etc.) ET
  // avec housingType="Appartement" (pour les fiches qui n'acceptent pas le
  // collectif strict mais qu'on veut appliquer par lot dans la copro).
  // On merge par code, en gardant le meilleur statut (Eligible > Confirmer
  // > Potentiel > Non éligible).
  const evalForIncomeAndHousing = useMemo(() => {
    return (income: "intermediate" | "veryModest"): FullEvaluateResult[] => {
      const projectCollectif: Project = {
        ...baseProject,
        incomeBracket: income,
        housingType: "Batiment d'habitation collectif en copropriete",
      };
      const projectAppt: Project = {
        ...baseProject,
        incomeBracket: income,
        housingType: "Appartement",
      };
      const collectif = evaluateAllSheets(projectCollectif, {
        buildingTypes: ["Habitation"],
        actions: actionsMerged,
      });
      const appt = evaluateAllSheets(projectAppt, {
        buildingTypes: ["Habitation"],
        actions: actionsMerged,
      });
      const priority: Record<string, number> = {
        Eligible: 0,
        "Eligibilite a confirmer": 1,
        "Potentiellement eligible": 2,
        "Non eligible": 3,
      };
      const merged = new Map<string, FullEvaluateResult>();
      for (const r of [...collectif, ...appt]) {
        const existing = merged.get(r.sheet.code);
        if (
          !existing ||
          (priority[r.evaluation.status] ?? 99) <
            (priority[existing.evaluation.status] ?? 99)
        ) {
          merged.set(r.sheet.code, r);
        }
      }
      return [...merged.values()];
    };
  }, [baseProject, actionsMerged]);

  const resultsStd = useMemo(
    () => evalForIncomeAndHousing("intermediate"),
    [evalForIncomeAndHousing],
  );
  const resultsModest = useMemo(
    () => evalForIncomeAndHousing("veryModest"),
    [evalForIncomeAndHousing],
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

  // Si l'utilisateur a coché des fiches → on cumule SA sélection
  // Sinon → cumul des fiches éligibles par défaut
  const hasSelection = selectedCodes.size > 0;

  const eligibleStdCodes = useMemo(() => {
    return new Set(
      visibleResults
        .filter((r) => r.evaluation.status === "Eligible")
        .map((r) => r.sheet.code),
    );
  }, [visibleResults]);

  const cumulCodes = useMemo(() => {
    if (hasSelection) return selectedCodes;
    return eligibleStdCodes;
  }, [hasSelection, selectedCodes, eligibleStdCodes]);

  const totalsStd = useMemo(() => {
    return sumEstimates(
      visibleResults.filter((r) => cumulCodes.has(r.sheet.code)),
    );
  }, [visibleResults, cumulCodes]);

  const totalsModest = useMemo(() => {
    const matching = resultsModest.filter((r) => cumulCodes.has(r.sheet.code));
    return sumEstimates(matching);
  }, [resultsModest, cumulCodes]);

  const toggleSelected = (code: string) => {
    setSelectedCodes((prev) => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code);
      else next.add(code);
      return next;
    });
  };

  const selectAllEligible = () => {
    setSelectedCodes(new Set(eligibleStdCodes));
  };

  const clearSelection = () => {
    setSelectedCodes(new Set());
  };

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
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="flex items-center gap-1 text-xs font-bold text-emerald-900">
            <Coins className="h-3.5 w-3.5" />
            {hasSelection
              ? `Mon panier de travaux (${selectedCodes.size} fiches sélectionnées)`
              : "Estimation CEE — fiches éligibles uniquement"}
          </div>
          <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
            <StatusPill count={eligibleCount} label="éligibles" tone="emerald" />
            <StatusPill count={confirmCount} label="à compléter" tone="amber" />
            {hasSelection ? (
              <button
                type="button"
                onClick={clearSelection}
                className="rounded-full bg-slate-200 px-2 py-0.5 text-[10px] font-bold hover:bg-slate-300"
              >
                Vider la sélection
              </button>
            ) : eligibleCount > 0 ? (
              <button
                type="button"
                onClick={selectAllEligible}
                className="rounded-full bg-emerald-200 px-2 py-0.5 text-[10px] font-bold text-emerald-900 hover:bg-emerald-300"
              >
                Tout cocher
              </button>
            ) : null}
          </div>
        </div>

        {totalsStd.kwhCumac > 0 || hasSelection ? (
          <div className="grid grid-cols-2 gap-2">
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

        {/* Inputs paramètres copro */}
        <div className="mt-2 grid grid-cols-2 gap-2 sm:grid-cols-4">
          <div className="flex flex-col gap-0.5">
            <label className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
              Année construction
            </label>
            <Input
              value={yearOverride}
              onChange={(e) => setYearOverride(e.target.value)}
              type="number"
              placeholder={defaultYear ? String(defaultYear) : "ex: 1965"}
              className="h-7 text-[11px]"
            />
          </div>
          <div className="flex flex-col gap-0.5">
            <label className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
              Surface appart. moy. (m²)
            </label>
            <Input
              value={surfaceOverride}
              onChange={(e) => setSurfaceOverride(e.target.value)}
              type="number"
              placeholder="60"
              className="h-7 text-[11px]"
            />
          </div>
          <div className="flex flex-col gap-0.5">
            <label className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
              Prix CEE std (€/MWh)
            </label>
            <Input
              value={priceStd}
              onChange={(e) => setPriceStd(e.target.value)}
              type="number"
              step="0.1"
              className="h-7 text-[11px]"
            />
          </div>
          <div className="flex flex-col gap-0.5">
            <label className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
              Prix CEE précaire (€/MWh)
            </label>
            <Input
              value={pricePrecaire}
              onChange={(e) => setPricePrecaire(e.target.value)}
              type="number"
              step="0.1"
              className="h-7 text-[11px]"
            />
          </div>
        </div>

        <p className="mt-2 text-[10px] text-emerald-800/70">
          {hasSelection
            ? `Cumul des ${selectedCodes.size} fiches cochées ci-dessous.`
            : "Cumul automatique des fiches éligibles. Coche des fiches dans la liste pour personnaliser ton panier."}
          {aggregates.energieDominante
            ? ` · Énergie dominante "${aggregates.energieDominante}"`
            : ""}
        </p>
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
                    project={
                      {
                        ...baseProject,
                        incomeBracket: "intermediate",
                        housingType: "Appartement",
                      } as Project
                    }
                    selected={selectedCodes.has(r.sheet.code)}
                    onToggleSelected={() => toggleSelected(r.sheet.code)}
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
  project,
  selected,
  onToggleSelected,
  onUpdateAction,
}: {
  resultStd: FullEvaluateResult;
  resultModest: FullEvaluateResult;
  currentAction: Action;
  project: Project;
  selected: boolean;
  onToggleSelected: () => void;
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
        "rounded-lg border bg-card px-3 py-2 text-xs transition",
        selected
          ? "border-emerald-500 bg-emerald-50 ring-2 ring-emerald-300"
          : isEligible
            ? "border-emerald-300 bg-emerald-50/40"
            : "border-amber-200 bg-amber-50/30",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex flex-1 items-start gap-2 min-w-0">
          <input
            type="checkbox"
            checked={selected}
            onChange={onToggleSelected}
            disabled={!isEligible}
            title={
              isEligible
                ? "Inclure cette fiche dans le panier"
                : "Complète d'abord la fiche pour la rendre éligible et l'ajouter au panier"
            }
            className="mt-0.5 h-4 w-4 shrink-0 disabled:opacity-30"
          />
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
          project={project}
          currentAction={currentAction}
          onUpdate={onUpdateAction}
        />
      ) : null}
    </div>
  );
}

/**
 * Mini formulaire pour compléter une fiche "à confirmer", basé sur le
 * catalogue exact des `fields` portés depuis sheets.js source.
 *
 * Plus de fallback texte libre : chaque champ a un type et des options
 * connus du moteur, donc la saisie déclenche bien la validation.
 */
function ActionEditor({
  code,
  project,
  currentAction,
  onUpdate,
}: {
  code: string;
  project: Project;
  currentAction: Action;
  onUpdate: (key: string, value: unknown) => void;
}) {
  const allFields = SHEET_FIELDS[code];
  if (!allFields || allFields.length === 0) {
    return (
      <div className="mt-2 rounded-md bg-slate-100 p-2 text-[10px] italic text-slate-700">
        Saisie détaillée non disponible pour cette fiche dans le simulateur
        rapide. Utilise la page complète /simulateur-cee pour ce cas.
      </div>
    );
  }

  // Filtre les fields actifs (selon showWhen)
  const visibleFields = allFields.filter((f) => {
    if (!f.showWhen) return true;
    try {
      return f.showWhen(currentAction, project);
    } catch {
      return true;
    }
  });

  const sheetFields = visibleFields.filter((f) => f.group !== "coupDePouce");
  const cdpFields = visibleFields.filter((f) => f.group === "coupDePouce");

  return (
    <div className="mt-2 space-y-2">
      <FieldGroup
        title="Compléter pour calculer"
        tone="amber"
        fields={sheetFields}
        currentAction={currentAction}
        onUpdate={onUpdate}
      />
      {cdpFields.length > 0 ? (
        <FieldGroup
          title="Coup de pouce — conditions"
          tone="purple"
          fields={cdpFields}
          currentAction={currentAction}
          onUpdate={onUpdate}
        />
      ) : null}
    </div>
  );
}

function FieldGroup({
  title,
  tone,
  fields,
  currentAction,
  onUpdate,
}: {
  title: string;
  tone: "amber" | "purple";
  fields: SheetField[];
  currentAction: Action;
  onUpdate: (key: string, value: unknown) => void;
}) {
  if (fields.length === 0) return null;
  const bg = tone === "amber" ? "bg-amber-100/60" : "bg-purple-100/50";
  const titleCol = tone === "amber" ? "text-amber-900" : "text-purple-900";
  const inputBorder = tone === "amber" ? "border-amber-300" : "border-purple-300";
  return (
    <div className={cn("rounded-md p-2", bg)}>
      <div
        className={cn(
          "mb-1.5 text-[10px] font-bold uppercase tracking-wider",
          titleCol,
        )}
      >
        {title}
      </div>
      <div className="grid grid-cols-2 gap-2">
        {fields.map((field) => (
          <div
            key={field.name}
            className={cn(
              "flex flex-col gap-0.5",
              field.type === "checkbox" ? "col-span-2 flex-row items-center" : "",
            )}
          >
            {field.type === "checkbox" ? (
              <label className="flex items-center gap-2 text-[10px] font-semibold">
                <input
                  type="checkbox"
                  checked={Boolean(currentAction[field.name])}
                  onChange={(e) => onUpdate(field.name, e.target.checked)}
                  className="h-3.5 w-3.5"
                />
                <span className={titleCol}>{field.label}</span>
              </label>
            ) : (
              <>
                <label className={cn("text-[9px] font-semibold", titleCol)}>
                  {field.label}
                </label>
                {field.type === "select" ? (
                  <select
                    value={String(currentAction[field.name] ?? "")}
                    onChange={(e) => onUpdate(field.name, e.target.value)}
                    className={cn(
                      "h-7 rounded-md border bg-white px-2 text-[11px]",
                      inputBorder,
                    )}
                  >
                    {(field.options ?? []).map((opt) => (
                      <option key={opt} value={opt}>
                        {opt === "" ? "Choisir…" : opt}
                      </option>
                    ))}
                  </select>
                ) : (
                  <Input
                    value={String(currentAction[field.name] ?? "")}
                    onChange={(e) => onUpdate(field.name, e.target.value)}
                    type={field.type === "number" ? "number" : "text"}
                    className={cn("h-7 text-[11px]", inputBorder)}
                  />
                )}
              </>
            )}
          </div>
        ))}
      </div>
    </div>
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

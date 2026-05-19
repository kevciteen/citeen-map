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
  Sparkles,
  Pencil,
  Plus,
  Check,
  Download,
  X,
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

type IncomeKey = "veryModest" | "modest" | "intermediate" | "high";

const FAMILY_ORDER: SheetFamily[] = [
  "Thermique",
  "Enveloppe",
  "Equipement",
  "Services",
];

const FAMILY_LABELS: Record<SheetFamily, string> = {
  Bouquet: "Bouquet",
  Enveloppe: "Enveloppe",
  Thermique: "Thermique",
  Equipement: "Équipement",
  Services: "Services",
};

/**
 * Estimation CEE complète pour une copropriété — version marketplace.
 *
 * Layout :
 *  1. Header KPI : totaux Standard / Très modeste en très grand
 *  2. Paramètres en bandeau collapsible (année, surface, prix CEE)
 *  3. Filtres horizontaux par famille (avec compteurs)
 *  4. Grille de cards (3 colonnes desktop, 1 mobile)
 *  5. Panier flottant en bas à droite avec total + exports
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
  const defaultSurface = 60;
  const [surfaceOverride, setSurfaceOverride] = useState<string>(
    String(defaultSurface),
  );
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [familyFilter, setFamilyFilter] = useState<SheetFamily | "all">("all");
  const [actionsOverride, setActionsOverride] = useState<Record<string, Action>>({});
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set());
  const [editingCode, setEditingCode] = useState<string | null>(null);

  // === Agrégats DPE individuels ========================================
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

  // === Project & actions ===============================================
  const baseProject = useMemo<Omit<Project, "incomeBracket" | "housingType">>(() => {
    const yearFromOverride = Number(yearOverride);
    const yearFromPeriod = mapPeriodToYear(periodeConstruction);
    const year =
      Number.isFinite(yearFromOverride) && yearFromOverride > 0
        ? yearFromOverride
        : (yearFromPeriod ?? undefined);
    const surfaceFromOverride = Number(surfaceOverride);
    const aptSurface =
      Number.isFinite(surfaceFromOverride) && surfaceFromOverride > 0
        ? surfaceFromOverride
        : aggregates.surfaceMoyenne
          ? Math.round(aggregates.surfaceMoyenne)
          : 60;
    return {
      buildingType: "Habitation",
      postalCode: codePostal ?? undefined,
      constructionYear: year,
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

  // === Double évaluation Appartement + Collectif ========================
  const evalFor = useMemo(() => {
    return (income: IncomeKey): FullEvaluateResult[] => {
      const priority: Record<string, number> = {
        Eligible: 0,
        "Eligibilite a confirmer": 1,
        "Potentiellement eligible": 2,
        "Non eligible": 3,
      };
      const merged = new Map<string, FullEvaluateResult>();
      for (const ht of [
        "Batiment d'habitation collectif en copropriete",
        "Appartement",
      ] as const) {
        const project: Project = {
          ...baseProject,
          incomeBracket: income,
          housingType: ht,
        };
        const r = evaluateAllSheets(project, {
          buildingTypes: ["Habitation"],
          actions: actionsMerged,
        });
        for (const x of r) {
          const existing = merged.get(x.sheet.code);
          if (
            !existing ||
            (priority[x.evaluation.status] ?? 99) <
              (priority[existing.evaluation.status] ?? 99)
          ) {
            merged.set(x.sheet.code, x);
          }
        }
      }
      return [...merged.values()];
    };
  }, [baseProject, actionsMerged]);

  const resultsStd = useMemo(() => evalFor("intermediate"), [evalFor]);
  const resultsModest = useMemo(() => evalFor("veryModest"), [evalFor]);
  const modestByCode = useMemo(() => {
    const m = new Map<string, FullEvaluateResult>();
    for (const r of resultsModest) m.set(r.sheet.code, r);
    return m;
  }, [resultsModest]);

  // Garde uniquement éligibles + à confirmer
  const visibleResults = useMemo(
    () =>
      resultsStd.filter(
        (r) =>
          r.evaluation.status === "Eligible" ||
          r.evaluation.status === "Eligibilite a confirmer",
      ),
    [resultsStd],
  );

  // === Cumul panier ====================================================
  const eligibleCodes = useMemo(
    () =>
      new Set(
        visibleResults
          .filter((r) => r.evaluation.status === "Eligible")
          .map((r) => r.sheet.code),
      ),
    [visibleResults],
  );

  // Cumul UNIQUEMENT sur les fiches cochées par l'utilisateur. Si rien
  // coché → totaux à 0 et message d'invitation. On évite l'effet "total
  // automatique qui peut induire en erreur".
  const hasSelection = selectedCodes.size > 0;
  const cumulCodes = selectedCodes;

  const totalsStd = useMemo(
    () =>
      sumEstimates(
        visibleResults.filter((r) => cumulCodes.has(r.sheet.code)),
      ),
    [visibleResults, cumulCodes],
  );
  const totalsModest = useMemo(
    () => sumEstimates(resultsModest.filter((r) => cumulCodes.has(r.sheet.code))),
    [resultsModest, cumulCodes],
  );

  // Comptage par famille pour les filtres
  const familyCounts = useMemo(() => {
    const counts: Record<SheetFamily, number> = {
      Bouquet: 0,
      Enveloppe: 0,
      Thermique: 0,
      Equipement: 0,
      Services: 0,
    };
    for (const r of visibleResults) counts[r.sheet.family as SheetFamily]++;
    return counts;
  }, [visibleResults]);

  const filteredResults = useMemo(
    () =>
      familyFilter === "all"
        ? visibleResults
        : visibleResults.filter((r) => r.sheet.family === familyFilter),
    [visibleResults, familyFilter],
  );

  // === Actions =========================================================
  const updateAction = (code: string, key: string, value: unknown) => {
    setActionsOverride((prev) => ({
      ...prev,
      [code]: { ...(prev[code] ?? {}), [key]: value },
    }));
  };
  const toggleSelected = (code: string) => {
    setSelectedCodes((prev) => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code);
      else next.add(code);
      return next;
    });
  };
  const selectAllEligible = () => setSelectedCodes(new Set(eligibleCodes));
  const clearSelection = () => setSelectedCodes(new Set());

  const exportCsv = () => {
    const lines: string[] = [];
    lines.push(
      [
        "Code",
        "Titre",
        "Famille",
        "Statut",
        "kWh cumac",
        "€ Standard",
        "€ Très modeste",
      ].join(";"),
    );
    for (const r of visibleResults.filter((r) => cumulCodes.has(r.sheet.code))) {
      const m = modestByCode.get(r.sheet.code)?.evaluation;
      lines.push(
        [
          r.sheet.code,
          r.sheet.title.replace(/;/g, ","),
          r.sheet.family,
          r.evaluation.status,
          r.evaluation.kwhCumac ?? "",
          r.evaluation.euroAmount ? Math.round(r.evaluation.euroAmount) : "",
          m?.euroAmount ? Math.round(m.euroAmount) : "",
        ].join(";"),
      );
    }
    lines.push("");
    lines.push(
      `Total;;;;;${Math.round(totalsStd.euroAmount)};${Math.round(totalsModest.euroAmount)}`,
    );
    const blob = new Blob(["﻿" + lines.join("\n")], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `cee-copro-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // === Rendu ===========================================================
  if (visibleResults.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
        <div className="mb-1 flex items-center gap-1 font-semibold">
          <Info className="h-3.5 w-3.5" />
          Aucune fiche éligible ou à confirmer
        </div>
        <p>
          Vérifie que le DPE collectif est chargé et que la période de
          construction est renseignée.
        </p>
      </div>
    );
  }

  const editingResult = editingCode
    ? visibleResults.find((r) => r.sheet.code === editingCode)
    : null;

  return (
    <div className="space-y-4">
      {/* ============ HEADER : TOTAUX XL ============ */}
      <div className="overflow-hidden rounded-2xl bg-gradient-to-br from-slate-900 via-emerald-900 to-emerald-700 p-5 text-white shadow-xl">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] opacity-80">
              <Coins className="h-3 w-3" />
              {hasSelection
                ? `Panier · ${selectedCodes.size} fiche${selectedCodes.size > 1 ? "s" : ""} sélectionnée${selectedCodes.size > 1 ? "s" : ""}`
                : `Catalogue · ${eligibleCodes.size} éligibles disponibles`}
            </div>
            {hasSelection ? (
              <div className="mt-3 grid grid-cols-2 gap-6">
                <div>
                  <div className="text-[10px] uppercase tracking-wider opacity-70">
                    Revenus standard
                  </div>
                  <div className="text-4xl font-black tabular-nums tracking-tight">
                    {formatEuros(totalsStd.euroAmount)}
                  </div>
                  <div className="text-[10px] opacity-70">
                    {Math.round(totalsStd.kwhCumac / 1000).toLocaleString("fr-FR")} MWh cumac
                  </div>
                </div>
                <div>
                  <div className="flex items-center gap-1 text-[10px] uppercase tracking-wider text-purple-200">
                    <Sparkles className="h-3 w-3" />
                    Très modestes
                  </div>
                  <div className="text-4xl font-black tabular-nums tracking-tight text-purple-100">
                    {formatEuros(totalsModest.euroAmount)}
                  </div>
                  <div className="text-[10px] text-purple-200/70">
                    Coup de pouce inclus
                  </div>
                </div>
              </div>
            ) : (
              <div className="mt-3 max-w-md">
                <p className="text-base font-semibold leading-tight">
                  Compose ton bouquet de travaux
                </p>
                <p className="mt-1 text-xs opacity-80">
                  Coche les fiches CEE qui t'intéressent dans la grille ci-dessous
                  pour voir le cumul de primes (revenus standard + très modestes
                  avec Coup de pouce). Les fiches "à compléter" se débloquent en
                  cliquant sur "Compléter".
                </p>
              </div>
            )}
          </div>
          <div className="flex flex-col gap-2">
            {hasSelection ? (
              <button
                type="button"
                onClick={clearSelection}
                className="rounded-full bg-white/10 px-3 py-1 text-[10px] font-bold uppercase tracking-wider backdrop-blur hover:bg-white/20"
              >
                Vider
              </button>
            ) : eligibleCodes.size > 0 ? (
              <button
                type="button"
                onClick={selectAllEligible}
                className="rounded-full bg-white/15 px-3 py-1 text-[10px] font-bold uppercase tracking-wider backdrop-blur hover:bg-white/25"
              >
                Tout cocher
              </button>
            ) : null}
            <button
              type="button"
              onClick={exportCsv}
              disabled={cumulCodes.size === 0}
              className="inline-flex items-center gap-1 rounded-full bg-emerald-400 px-3 py-1 text-[10px] font-bold uppercase tracking-wider text-emerald-950 hover:bg-emerald-300 disabled:opacity-50"
            >
              <Download className="h-3 w-3" /> Export CSV
            </button>
          </div>
        </div>
        {aggregates.energieDominante ? (
          <div className="mt-3 inline-flex items-center gap-1 rounded-full bg-white/10 px-2 py-0.5 text-[10px] backdrop-blur">
            <Flame className="h-3 w-3" />
            Énergie dominante &nbsp;<strong>{aggregates.energieDominante}</strong>
            {aggregates.partPassoires > 0
              ? ` · ${Math.round(aggregates.partPassoires * 100)}% de lots F/G`
              : ""}
          </div>
        ) : null}
      </div>

      {/* ============ PARAMÈTRES (collapsible) ============ */}
      <div className="rounded-xl border border-border bg-card">
        <button
          type="button"
          onClick={() => setSettingsOpen(!settingsOpen)}
          className="flex w-full items-center justify-between px-4 py-2 text-xs font-bold uppercase tracking-wider hover:bg-secondary/50"
        >
          <span className="flex items-center gap-2">
            <Cog className="h-3.5 w-3.5" />
            Paramètres de simulation
          </span>
          <span className="text-muted-foreground">
            {settingsOpen ? "▲" : "▼"}
          </span>
        </button>
        {settingsOpen ? (
          <div className="grid grid-cols-2 gap-3 border-t border-border p-3 sm:grid-cols-4">
            <ParamInput
              label="Année construction"
              value={yearOverride}
              onChange={setYearOverride}
              placeholder={defaultYear ? String(defaultYear) : "1965"}
              type="number"
            />
            <ParamInput
              label="Surface appart. moy. (m²)"
              value={surfaceOverride}
              onChange={setSurfaceOverride}
              placeholder="60"
              type="number"
            />
            <ParamInput
              label="Prix CEE std (€/MWh)"
              value={priceStd}
              onChange={setPriceStd}
              type="number"
              step="0.1"
            />
            <ParamInput
              label="Prix CEE précaire (€/MWh)"
              value={pricePrecaire}
              onChange={setPricePrecaire}
              type="number"
              step="0.1"
            />
          </div>
        ) : null}
      </div>

      {/* ============ FILTRES PAR FAMILLE ============ */}
      <div className="flex flex-wrap items-center gap-1.5">
        <FilterChip
          active={familyFilter === "all"}
          onClick={() => setFamilyFilter("all")}
          count={visibleResults.length}
        >
          Tous
        </FilterChip>
        {FAMILY_ORDER.map((f) =>
          familyCounts[f] > 0 ? (
            <FilterChip
              key={f}
              active={familyFilter === f}
              onClick={() => setFamilyFilter(f)}
              count={familyCounts[f]}
              icon={familyIcon(f)}
            >
              {FAMILY_LABELS[f]}
            </FilterChip>
          ) : null,
        )}
      </div>

      {/* ============ GRILLE DE CARDS ============ */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {filteredResults.map((r) => (
          <SheetCard
            key={r.sheet.code}
            resultStd={r}
            resultModest={modestByCode.get(r.sheet.code) ?? r}
            selected={selectedCodes.has(r.sheet.code)}
            onToggleSelected={() => toggleSelected(r.sheet.code)}
            onEdit={() => setEditingCode(r.sheet.code)}
          />
        ))}
      </div>

      {/* ============ DRAWER COMPLÉTER ============ */}
      {editingResult ? (
        <CompleteDrawer
          result={editingResult}
          project={
            {
              ...baseProject,
              incomeBracket: "intermediate",
              housingType: "Appartement",
            } as Project
          }
          currentAction={actionsMerged[editingResult.sheet.code] ?? {}}
          onUpdate={(key, value) =>
            updateAction(editingResult.sheet.code, key, value)
          }
          onClose={() => setEditingCode(null)}
        />
      ) : null}

      {/* ============ PANIER FLOTTANT ============ */}
      {hasSelection ? (
        <div className="sticky bottom-4 z-30 mx-auto max-w-2xl">
          <div className="flex items-center justify-between gap-3 rounded-full bg-emerald-600 px-4 py-2 text-white shadow-2xl">
            <div className="flex items-center gap-2 text-xs font-bold">
              <Check className="h-4 w-4" />
              {selectedCodes.size} fiche{selectedCodes.size > 1 ? "s" : ""}
            </div>
            <div className="flex items-baseline gap-2">
              <span className="text-base font-black tabular-nums">
                {formatEuros(totalsStd.euroAmount)}
              </span>
              <span className="text-[10px] opacity-80">
                ou {formatEuros(totalsModest.euroAmount)} très modestes
              </span>
            </div>
            <button
              type="button"
              onClick={exportCsv}
              className="rounded-full bg-white px-3 py-1 text-[10px] font-bold uppercase tracking-wider text-emerald-700 hover:bg-emerald-50"
            >
              Exporter
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

// === Sub-components ====================================================

function ParamInput({
  label,
  value,
  onChange,
  type = "text",
  placeholder,
  step,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  step?: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        type={type}
        placeholder={placeholder}
        step={step}
        className="h-8 text-xs"
      />
    </div>
  );
}

function FilterChip({
  active,
  onClick,
  count,
  icon,
  children,
}: {
  active: boolean;
  onClick: () => void;
  count: number;
  icon?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-[11px] font-semibold transition",
        active
          ? "bg-foreground text-background"
          : "bg-secondary text-foreground hover:bg-secondary/70",
      )}
    >
      {icon}
      {children}
      <span
        className={cn(
          "rounded-full px-1.5 py-0 text-[9px] font-bold tabular-nums",
          active ? "bg-background/20 text-background" : "bg-foreground/10",
        )}
      >
        {count}
      </span>
    </button>
  );
}

function SheetCard({
  resultStd,
  resultModest,
  selected,
  onToggleSelected,
  onEdit,
}: {
  resultStd: FullEvaluateResult;
  resultModest: FullEvaluateResult;
  selected: boolean;
  onToggleSelected: () => void;
  onEdit: () => void;
}) {
  const { evaluation, sheet } = resultStd;
  const evMod = resultModest.evaluation;
  const isEligible = evaluation.status === "Eligible";
  const isConfirm = evaluation.status === "Eligibilite a confirmer";
  const family = sheet.family as SheetFamily;
  const cdp = evMod.coupDePouce ?? evaluation.coupDePouce;

  return (
    <div
      className={cn(
        "group relative flex flex-col rounded-xl border bg-card p-4 transition hover:shadow-md",
        selected
          ? "border-emerald-500 ring-2 ring-emerald-300"
          : isEligible
            ? "border-border"
            : "border-amber-200 bg-amber-50/30",
      )}
    >
      {/* Header card */}
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="flex items-center gap-2">
          <div className={cn("rounded-lg p-1.5", familyBg(family))}>
            {familyIcon(family)}
          </div>
          <div>
            <div className="font-mono text-[9px] font-bold uppercase tracking-wider text-muted-foreground">
              {sheet.code}
            </div>
            <div className="text-[10px] text-muted-foreground">
              {FAMILY_LABELS[family]}
            </div>
          </div>
        </div>
        {isEligible ? (
          <span className="rounded-full bg-emerald-600 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-white">
            Éligible
          </span>
        ) : isConfirm ? (
          <span className="rounded-full bg-amber-500 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-white">
            À compléter
          </span>
        ) : null}
      </div>

      {/* Titre */}
      <h3 className="mb-3 line-clamp-2 min-h-[2.5rem] text-sm font-bold leading-tight">
        {sheet.title}
      </h3>

      {/* Montants */}
      <div className="mb-3 grid grid-cols-2 gap-2 border-y border-border py-2.5">
        <div>
          <div className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
            Standard
          </div>
          <div className="text-base font-black tabular-nums">
            {isEligible && evaluation.euroAmount != null
              ? formatEurosShort(evaluation.euroAmount)
              : "—"}
          </div>
        </div>
        <div>
          <div className="flex items-center gap-0.5 text-[9px] font-semibold uppercase tracking-wider text-purple-700">
            <Sparkles className="h-2.5 w-2.5" />
            Très modeste
          </div>
          <div className="text-base font-black tabular-nums text-purple-900">
            {isEligible && evMod.euroAmount != null
              ? formatEurosShort(evMod.euroAmount)
              : "—"}
          </div>
        </div>
      </div>

      {/* Coup de pouce */}
      {cdp && cdp.factor && cdp.factor > 1 ? (
        <div className="mb-3 inline-flex items-center gap-1 self-start rounded-full bg-purple-100 px-2 py-0.5 text-[9px] font-bold text-purple-900">
          <Sparkles className="h-2.5 w-2.5" />
          Coup de pouce ×{cdp.factor}
        </div>
      ) : null}

      {/* kWh cumac */}
      {isEligible && evaluation.kwhCumac != null ? (
        <div className="mb-3 text-[10px] text-muted-foreground">
          {Math.round(evaluation.kwhCumac).toLocaleString("fr-FR")} kWh cumac (standard)
        </div>
      ) : null}

      {/* Actions */}
      <div className="mt-auto flex items-center gap-2">
        {isEligible ? (
          <button
            type="button"
            onClick={onToggleSelected}
            className={cn(
              "flex flex-1 items-center justify-center gap-1 rounded-md px-3 py-1.5 text-[11px] font-bold transition",
              selected
                ? "bg-emerald-600 text-white hover:bg-emerald-700"
                : "bg-emerald-100 text-emerald-800 hover:bg-emerald-200",
            )}
          >
            {selected ? (
              <>
                <Check className="h-3 w-3" /> Dans le panier
              </>
            ) : (
              <>
                <Plus className="h-3 w-3" /> Ajouter au panier
              </>
            )}
          </button>
        ) : (
          <button
            type="button"
            onClick={onEdit}
            className="flex flex-1 items-center justify-center gap-1 rounded-md bg-amber-500 px-3 py-1.5 text-[11px] font-bold text-white hover:bg-amber-600"
          >
            <Pencil className="h-3 w-3" /> Compléter
          </button>
        )}
        {isEligible && SHEET_FIELDS[sheet.code] ? (
          <button
            type="button"
            onClick={onEdit}
            className="rounded-md border border-border bg-background px-2 py-1.5 text-[11px] hover:bg-secondary"
            title="Affiner les paramètres"
          >
            <Pencil className="h-3 w-3" />
          </button>
        ) : null}
      </div>
    </div>
  );
}

function CompleteDrawer({
  result,
  project,
  currentAction,
  onUpdate,
  onClose,
}: {
  result: FullEvaluateResult;
  project: Project;
  currentAction: Action;
  onUpdate: (key: string, value: unknown) => void;
  onClose: () => void;
}) {
  const { evaluation, sheet } = result;
  const allFields = SHEET_FIELDS[sheet.code];
  const visibleFields = useMemo(() => {
    if (!allFields) return [];
    return allFields.filter((f) => {
      if (!f.showWhen) return true;
      try {
        return f.showWhen(currentAction, project);
      } catch {
        return true;
      }
    });
  }, [allFields, currentAction, project]);
  const sheetFields = visibleFields.filter((f) => f.group !== "coupDePouce");
  const cdpFields = visibleFields.filter((f) => f.group === "coupDePouce");

  return (
    <div className="fixed inset-0 z-50 flex" onClick={onClose}>
      <div className="flex-1 bg-black/50 backdrop-blur-sm" />
      <div
        className="flex h-full w-full max-w-md flex-col bg-card shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between border-b border-border p-4">
          <div>
            <div className="font-mono text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
              {sheet.code}
            </div>
            <h3 className="mt-1 text-sm font-bold">{sheet.title}</h3>
          </div>
          <button
            onClick={onClose}
            className="rounded-md p-1 text-muted-foreground hover:bg-secondary"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-4">
          {!allFields || allFields.length === 0 ? (
            <p className="text-[11px] italic text-muted-foreground">
              Saisie détaillée non disponible pour cette fiche dans le simulateur
              rapide. Utilise la page complète /simulateur-cee.
            </p>
          ) : (
            <>
              {sheetFields.length > 0 ? (
                <FieldGroup
                  title="Caractéristiques du projet"
                  fields={sheetFields}
                  currentAction={currentAction}
                  onUpdate={onUpdate}
                />
              ) : null}
              {cdpFields.length > 0 ? (
                <div className="mt-4">
                  <FieldGroup
                    title="Coup de pouce — conditions"
                    fields={cdpFields}
                    currentAction={currentAction}
                    onUpdate={onUpdate}
                    purple
                  />
                </div>
              ) : null}
            </>
          )}
          {evaluation.missing.length > 0 ? (
            <div className="mt-4 rounded-md bg-amber-50 p-2 text-[11px] text-amber-900">
              <div className="mb-1 font-semibold">Champs encore manquants :</div>
              <ul className="space-y-0.5">
                {evaluation.missing.map((m, i) => (
                  <li key={i}>⚠ {m}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {evaluation.kwhCumac != null ? (
            <div className="mt-4 rounded-md bg-emerald-50 p-3">
              <div className="text-[10px] font-semibold uppercase tracking-wider text-emerald-700">
                Résultat actuel
              </div>
              <div className="mt-1 text-2xl font-black tabular-nums text-emerald-900">
                {formatEuros(evaluation.euroAmount)}
              </div>
              <div className="text-[10px] text-emerald-800/80">
                {Math.round(evaluation.kwhCumac).toLocaleString("fr-FR")} kWh cumac
              </div>
              {evaluation.calculationLabel ? (
                <p className="mt-1 font-mono text-[10px] text-emerald-800/70">
                  {evaluation.calculationLabel}
                </p>
              ) : null}
            </div>
          ) : null}
        </div>
        <div className="border-t border-border p-3">
          <button
            type="button"
            onClick={onClose}
            className="w-full rounded-md bg-primary px-3 py-2 text-xs font-bold text-primary-foreground hover:bg-primary/90"
          >
            Fermer
          </button>
        </div>
      </div>
    </div>
  );
}

function FieldGroup({
  title,
  fields,
  currentAction,
  onUpdate,
  purple,
}: {
  title: string;
  fields: SheetField[];
  currentAction: Action;
  onUpdate: (key: string, value: unknown) => void;
  purple?: boolean;
}) {
  return (
    <div>
      <h4
        className={cn(
          "mb-2 text-[10px] font-bold uppercase tracking-wider",
          purple ? "text-purple-700" : "text-muted-foreground",
        )}
      >
        {title}
      </h4>
      <div className="space-y-2">
        {fields.map((field) => (
          <div key={field.name}>
            {field.type === "checkbox" ? (
              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={Boolean(currentAction[field.name])}
                  onChange={(e) => onUpdate(field.name, e.target.checked)}
                  className="h-4 w-4"
                />
                <span>{field.label}</span>
              </label>
            ) : (
              <>
                <label className="text-[10px] font-medium text-muted-foreground">
                  {field.label}
                </label>
                {field.type === "select" ? (
                  <select
                    value={String(currentAction[field.name] ?? "")}
                    onChange={(e) => onUpdate(field.name, e.target.value)}
                    className="mt-0.5 h-8 w-full rounded-md border border-border bg-background px-2 text-xs"
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
                    className="mt-0.5 h-8 text-xs"
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

// === Utils ===============================================================

function familyIcon(f: SheetFamily) {
  switch (f) {
    case "Enveloppe":
      return <Shield className="h-3.5 w-3.5 text-blue-600" />;
    case "Thermique":
      return <Flame className="h-3.5 w-3.5 text-orange-600" />;
    case "Equipement":
      return <Cog className="h-3.5 w-3.5 text-slate-600" />;
    case "Services":
      return <Wind className="h-3.5 w-3.5 text-sky-600" />;
    case "Bouquet":
      return <Layers className="h-3.5 w-3.5 text-emerald-700" />;
  }
}

function familyBg(f: SheetFamily): string {
  switch (f) {
    case "Enveloppe":
      return "bg-blue-50";
    case "Thermique":
      return "bg-orange-50";
    case "Equipement":
      return "bg-slate-100";
    case "Services":
      return "bg-sky-50";
    case "Bouquet":
      return "bg-emerald-50";
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

function formatEurosShort(v: number): string {
  if (v >= 1000) {
    const k = v / 1000;
    return `${k.toLocaleString("fr-FR", { maximumFractionDigits: 1 })} k€`;
  }
  return `${Math.round(v)} €`;
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

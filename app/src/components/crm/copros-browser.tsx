"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import {
  Building2,
  Search,
  Loader2,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  X,
  SlidersHorizontal,
  Download,
  Plus,
  CheckSquare,
  Square,
  TrendingUp,
  FileSpreadsheet,
  RefreshCw,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { cn, stageMeta } from "@/lib/utils";
import { toast } from "sonner";

type Row = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  periode_construction: string | null;
  classe_finale: string | null;
  classe_reelle: string | null;
  conso_moyenne: number | null;
  nb_dpe_individuels: number | null;
  quality_level: "verified" | "approximate" | "uncertain" | "no_data" | null;
  prospect_id: number | null;
  prospect_stage: string | null;
};

const QUALITY_COLORS: Record<string, string> = {
  verified: "#22c55e",
  approximate: "#f59e0b",
  uncertain: "#f87171",
  no_data: "#94a3b8",
};
const QUALITY_LABELS: Record<string, string> = {
  verified: "Vérifié",
  approximate: "Approximatif",
  uncertain: "Incertain",
  no_data: "Aucune donnée",
};

const DEPARTEMENTS = ["", "75", "77", "78", "91", "92", "93", "94", "95"];
const DPE_FILTER = ["", "A", "B", "C", "D", "E", "F", "G", "NC"];
const PERIODES = [
  ["", "Toutes périodes"],
  ["AVANT_1949", "Avant 1949"],
  ["DE_1949_A_1974", "1949 — 1974"],
  ["DE_1975_A_1993", "1975 — 1993"],
  ["DE_1994_A_2000", "1994 — 2000"],
  ["DE_2001_A_2010", "2001 — 2010"],
  ["APRES_2011", "Après 2011"],
  ["NON_CONNUE", "Non connue"],
] as const;
type SortKey =
  | "default"
  | "name_asc" | "name_desc"
  | "commune_asc" | "commune_desc"
  | "lots_desc" | "lots_asc"
  | "periode_asc" | "periode_desc"
  | "syndic_asc" | "syndic_desc"
  | "dpe_asc" | "dpe_desc"
  | "conso_asc" | "conso_desc";

const SORT_OPTIONS = [
  ["default", "Commune / adresse"],
  ["conso_desc", "Conso DPE ↓ (plus énergivore)"],
  ["conso_asc", "Conso DPE ↑ (plus performant)"],
  ["lots_desc", "Lots habitation ↓"],
  ["lots_asc", "Lots habitation ↑"],
] as const;

const PAGE_SIZE = 50;

export function CoprosBrowser({ totalInDb }: { totalInDb: number }) {
  // Permet d'arriver pré-filtré via ?syndic=... ?cp=... ?dept=... ?dpe=...
  // depuis la fiche syndic ou n'importe quel autre point d'entrée.
  const searchParams = useSearchParams();
  const [q, setQ] = useState(() => searchParams.get("q") ?? "");
  const [dept, setDept] = useState(() => searchParams.get("dept") ?? "");
  const [cp, setCp] = useState(() => searchParams.get("cp") ?? "");
  const [syndic, setSyndic] = useState(() => searchParams.get("syndic") ?? "");
  const [dpe, setDpe] = useState(() => searchParams.get("dpe") ?? "");
  const [minLots, setMinLots] = useState("");
  const [maxLots, setMaxLots] = useState("");
  const [periode, setPeriode] = useState("");
  const [consoMin, setConsoMin] = useState("");
  const [consoMax, setConsoMax] = useState("");
  const [hasCollectif, setHasCollectif] = useState<"" | "1" | "0">("");
  const [inPipeline, setInPipeline] = useState<"" | "1" | "0">("");
  const [dpeComputed, setDpeComputed] = useState<"" | "1" | "0">("");
  const [qualityFilter, setQualityFilter] = useState<Set<string>>(new Set());
  const [sort, setSort] = useState<SortKey>("default");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [page, setPage] = useState(1);

  const [rows, setRows] = useState<Row[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [bulkAddBusy, setBulkAddBusy] = useState(false);
  const [autoCompute, setAutoCompute] = useState(true);
  const [computing, setComputing] = useState<{ done: number; total: number } | null>(null);
  const [recomputing, setRecomputing] = useState<{ done: number; total: number } | null>(null);
  const seq = useRef(0);
  const computeSeq = useRef(0);

  const qs = useMemo(() => {
    const sp = new URLSearchParams();
    if (q.trim()) sp.set("q", q.trim());
    if (dept) sp.set("dept", dept);
    if (cp.trim()) sp.set("cp", cp.trim());
    if (syndic.trim()) sp.set("syndic", syndic.trim());
    if (dpe) sp.set("dpe", dpe);
    if (minLots.trim()) sp.set("minLots", minLots.trim());
    if (maxLots.trim()) sp.set("maxLots", maxLots.trim());
    if (periode) sp.set("periode", periode);
    if (consoMin.trim()) sp.set("consoMin", consoMin.trim());
    if (consoMax.trim()) sp.set("consoMax", consoMax.trim());
    if (hasCollectif) sp.set("hasCollectif", hasCollectif);
    if (inPipeline) sp.set("inPipeline", inPipeline);
    if (dpeComputed) sp.set("dpeComputed", dpeComputed);
    if (qualityFilter.size > 0) sp.set("quality", Array.from(qualityFilter).join(","));
    if (sort && sort !== "default") sp.set("sort", sort);
    sp.set("limit", String(PAGE_SIZE));
    sp.set("offset", String((page - 1) * PAGE_SIZE));
    return sp.toString();
  }, [
    q, dept, cp, syndic, dpe, minLots, maxLots, periode, consoMin, consoMax,
    hasCollectif, inPipeline, dpeComputed, qualityFilter, sort, page,
  ]);

  const refetchList = async () => {
    const r = await fetch(`/api/copros/list?${qs}`);
    if (!r.ok) return;
    const j = await r.json();
    setRows(j.items as Row[]);
    setCount(j.total);
  };

  useEffect(() => {
    const t = setTimeout(async () => {
      const n = ++seq.current;
      setLoading(true);
      try {
        const r = await fetch(`/api/copros/list?${qs}`);
        if (!r.ok) return;
        const j = await r.json();
        if (n !== seq.current) return;
        setRows(j.items as Row[]);
        setCount(j.total);
      } finally {
        if (n === seq.current) setLoading(false);
      }
    }, 220);
    return () => clearTimeout(t);
  }, [qs]);

  /* Auto-precompute DPE for rows without an estimate yet, in parallel batches.
     This is what makes the "DPE filter" actually return results: the cache
     fills as the user browses pages. */
  useEffect(() => {
    if (!autoCompute) return;
    const missing = rows.filter((r) => r.classe_finale == null).map((r) => r.id);
    if (missing.length === 0) {
      setComputing(null);
      return;
    }
    const myRun = ++computeSeq.current;
    let cancelled = false;
    (async () => {
      const CHUNK = 25;
      const total = missing.length;
      let done = 0;
      setComputing({ done, total });
      for (let i = 0; i < missing.length; i += CHUNK) {
        if (cancelled || myRun !== computeSeq.current) return;
        const slice = missing.slice(i, i + CHUNK);
        const r = await fetch("/api/copros/dpe-batch", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ coproIds: slice, concurrency: 10 }),
        }).catch(() => null);
        if (r?.ok) {
          done += slice.length;
          if (myRun === computeSeq.current && !cancelled) {
            setComputing({ done, total });
            // Refetch list to update DPE pills for the rows we just processed
            await refetchList();
          }
        }
      }
      if (myRun === computeSeq.current && !cancelled) setComputing(null);
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows.length, autoCompute, page, qs]);

  useEffect(() => {
    setPage(1);
  }, [q, dept, cp, syndic, dpe, minLots, maxLots, periode, consoMin, consoMax, hasCollectif, inPipeline, dpeComputed, qualityFilter]);

  const reset = () => {
    setQ(""); setDept(""); setCp(""); setSyndic(""); setDpe("");
    setMinLots(""); setMaxLots(""); setPeriode(""); setConsoMin(""); setConsoMax("");
    setHasCollectif(""); setInPipeline(""); setDpeComputed("");
    setQualityFilter(new Set());
    setSort("default");
    setSelected(new Set());
  };

  const toggleQuality = (q: string) => {
    const next = new Set(qualityFilter);
    if (next.has(q)) next.delete(q);
    else next.add(q);
    setQualityFilter(next);
  };

  const pages = Math.max(1, Math.ceil(count / PAGE_SIZE));

  const toggleAll = () => {
    if (selected.size === rows.length && rows.length > 0) setSelected(new Set());
    else setSelected(new Set(rows.map((r) => r.id)));
  };

  /* Force le recalcul DPE des copros visibles (utile après une mise à jour
     de la logique de détection collectif/individuel — on doit invalider le
     cache des copros déjà calculées sous l'ancienne règle). */
  const forceRecompute = async () => {
    if (rows.length === 0) return;
    if (
      !confirm(
        `Recalculer le DPE de ${rows.length} copropriété${rows.length > 1 ? "s" : ""} visible${rows.length > 1 ? "s" : ""} ? Les valeurs précédentes seront écrasées.`,
      )
    ) {
      return;
    }
    const ids = rows.map((r) => r.id);
    const CHUNK = 25;
    setRecomputing({ done: 0, total: ids.length });
    let done = 0;
    for (let i = 0; i < ids.length; i += CHUNK) {
      const slice = ids.slice(i, i + CHUNK);
      const r = await fetch("/api/copros/dpe-batch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          coproIds: slice,
          forceRefresh: true,
          concurrency: 10,
        }),
      }).catch(() => null);
      if (r?.ok) {
        done += slice.length;
        setRecomputing({ done, total: ids.length });
      }
    }
    setRecomputing(null);
    // Refetch pour rafraîchir les pills DPE et les filtres
    await refetchList();
    toast.success(`${done}/${ids.length} copros recalculées`);
  };
  const toggleOne = (id: number) => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setSelected(next);
  };

  const bulkAddToPipeline = async () => {
    if (selected.size === 0) return;
    setBulkAddBusy(true);
    try {
      const r = await fetch("/api/prospects/bulk", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          coproIds: Array.from(selected),
          stage: "to_contact",
          priority: 2,
        }),
      });
      if (r.ok) {
        const j = await r.json();
        toast.success(
          `Pipeline : ${j.created} créé(s)${j.alreadyExists ? `, ${j.alreadyExists} déjà présent(s)` : ""}`,
        );
        setSelected(new Set());
        // re-fetch so prospect_id appears
        seq.current = 0;
        setLoading(true);
        const r2 = await fetch(`/api/copros/list?${qs}`);
        if (r2.ok) {
          const j2 = await r2.json();
          setRows(j2.items);
          setCount(j2.total);
        }
        setLoading(false);
      } else {
        toast.error("Erreur");
      }
    } finally {
      setBulkAddBusy(false);
    }
  };

  const bulkExportXlsx = () => {
    if (selected.size === 0) return;
    const ids = Array.from(selected).join(",");
    window.location.href = `/api/export/copros.xlsx?ids=${ids}`;
  };
  const bulkExportCsv = () => {
    if (selected.size === 0) return;
    const ids = Array.from(selected).join(",");
    window.location.href = `/api/export/copros.csv?ids=${ids}`;
  };

  const activeFilterCount = [
    q, dept, cp, syndic, dpe, minLots, maxLots, periode, consoMin, consoMax,
    hasCollectif, inPipeline, dpeComputed,
  ].filter((v) => v && v !== "").length + (qualityFilter.size > 0 ? 1 : 0);

  return (
    <div className="flex h-full flex-col">
      {/* SEARCH BAR (always visible) */}
      <div className="flex flex-wrap items-center gap-2 border-b border-border bg-card p-3">
        <div className="relative min-w-[260px] flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Adresse, nom, syndic, n° immatriculation, commune… (multi‑mots)"
            className="pl-9"
          />
          {loading && q ? (
            <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-primary" />
          ) : null}
        </div>
        <select
          value={dept}
          onChange={(e) => setDept(e.target.value)}
          className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
        >
          {DEPARTEMENTS.map((d) => (
            <option key={d} value={d}>
              {d || "Tous dpt"}
            </option>
          ))}
        </select>
        <Input
          value={cp}
          onChange={(e) => setCp(e.target.value)}
          placeholder="Code postal"
          className="w-32"
          inputMode="numeric"
        />
        <select
          value={dpe}
          onChange={(e) => setDpe(e.target.value)}
          className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
        >
          {DPE_FILTER.map((d) => (
            <option key={d} value={d}>
              {d || "DPE: toutes"}
            </option>
          ))}
        </select>
        <Button
          variant={advancedOpen ? "default" : "outline"}
          size="sm"
          onClick={() => setAdvancedOpen((v) => !v)}
        >
          <SlidersHorizontal className="h-3.5 w-3.5" />
          Filtres avancés
          {activeFilterCount > 0 ? (
            <Badge variant="secondary" className="ml-1 px-1.5 text-[10px]">
              {activeFilterCount}
            </Badge>
          ) : null}
        </Button>
        {activeFilterCount > 0 ? (
          <Button variant="ghost" size="sm" onClick={reset}>
            <X className="h-3.5 w-3.5" /> Réinitialiser
          </Button>
        ) : null}
      </div>

      {/* ACTIVE FILTER CHIPS — visibilité claire de ce qui filtre */}
      {activeFilterCount > 0 ? (
        <div className="flex flex-wrap items-center gap-1.5 border-b border-border bg-secondary/30 px-3 py-2 text-[11px]">
          <span className="font-semibold text-muted-foreground">Filtres actifs :</span>
          {q && <Chip onRemove={() => setQ("")}>recherche « {q} »</Chip>}
          {dept && <Chip onRemove={() => setDept("")}>dept {dept}</Chip>}
          {cp && <Chip onRemove={() => setCp("")}>CP {cp}*</Chip>}
          {syndic && <Chip onRemove={() => setSyndic("")}>syndic « {syndic} »</Chip>}
          {dpe && <Chip onRemove={() => setDpe("")}>DPE {dpe}</Chip>}
          {periode && <Chip onRemove={() => setPeriode("")}>période {periode.replace(/_/g, " ")}</Chip>}
          {minLots && <Chip onRemove={() => setMinLots("")}>lots ≥ {minLots}</Chip>}
          {maxLots && <Chip onRemove={() => setMaxLots("")}>lots ≤ {maxLots}</Chip>}
          {consoMin && <Chip onRemove={() => setConsoMin("")}>conso ≥ {consoMin}</Chip>}
          {consoMax && <Chip onRemove={() => setConsoMax("")}>conso ≤ {consoMax}</Chip>}
          {hasCollectif === "1" && <Chip onRemove={() => setHasCollectif("")}>DPE collectif réel</Chip>}
          {hasCollectif === "0" && <Chip onRemove={() => setHasCollectif("")}>sans DPE collectif</Chip>}
          {dpeComputed === "1" && <Chip onRemove={() => setDpeComputed("")}>avec DPE estimé</Chip>}
          {dpeComputed === "0" && <Chip onRemove={() => setDpeComputed("")}>sans DPE estimé</Chip>}
          {inPipeline === "1" && <Chip onRemove={() => setInPipeline("")}>déjà pipeline</Chip>}
          {inPipeline === "0" && <Chip onRemove={() => setInPipeline("")}>pas pipeline</Chip>}
          {Array.from(qualityFilter).map((qq) => (
            <Chip key={qq} onRemove={() => toggleQuality(qq)}>
              qualité {QUALITY_LABELS[qq]}
            </Chip>
          ))}
        </div>
      ) : null}

      {/* ADVANCED FILTERS */}
      {advancedOpen ? (
        <div className="grid gap-3 border-b border-border bg-secondary/40 p-4 lg:grid-cols-4">
          <Field label="Syndic">
            <Input
              value={syndic}
              onChange={(e) => setSyndic(e.target.value)}
              placeholder="Nom syndic"
            />
          </Field>
          <Field label="Période construction">
            <select
              value={periode}
              onChange={(e) => setPeriode(e.target.value)}
              className="h-10 w-full rounded-lg border border-input bg-background px-3 text-sm"
            >
              {PERIODES.map(([v, l]) => (
                <option key={v} value={v}>
                  {l}
                </option>
              ))}
            </select>
          </Field>
          <Field label="Lots habitation (min)">
            <Input
              value={minLots}
              onChange={(e) => setMinLots(e.target.value)}
              placeholder="ex. 10"
              inputMode="numeric"
            />
          </Field>
          <Field label="Lots habitation (max)">
            <Input
              value={maxLots}
              onChange={(e) => setMaxLots(e.target.value)}
              placeholder="ex. 200"
              inputMode="numeric"
            />
          </Field>
          <Field label="Conso min (kWhep/m²/an)">
            <Input
              value={consoMin}
              onChange={(e) => setConsoMin(e.target.value)}
              placeholder="ex. 250"
              inputMode="numeric"
            />
          </Field>
          <Field label="Conso max (kWhep/m²/an)">
            <Input
              value={consoMax}
              onChange={(e) => setConsoMax(e.target.value)}
              placeholder="ex. 999"
              inputMode="numeric"
            />
          </Field>
          <Field label="Tri">
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value as SortKey)}
              className="h-10 w-full rounded-lg border border-input bg-background px-3 text-sm"
            >
              {SORT_OPTIONS.map(([v, l]) => (
                <option key={v} value={v}>
                  {l}
                </option>
              ))}
            </select>
          </Field>
          <Field label="Tri rapide">
            <div className="flex flex-wrap gap-1.5">
              <Toggle
                active={dpeComputed === "1"}
                onClick={() => setDpeComputed(dpeComputed === "1" ? "" : "1")}
              >
                Avec DPE estimé
              </Toggle>
              <Toggle
                active={hasCollectif === "1"}
                onClick={() => setHasCollectif(hasCollectif === "1" ? "" : "1")}
              >
                DPE collectif réel
              </Toggle>
              <Toggle
                active={inPipeline === "0"}
                onClick={() => setInPipeline(inPipeline === "0" ? "" : "0")}
              >
                Pas dans le pipeline
              </Toggle>
              <Toggle
                active={inPipeline === "1"}
                onClick={() => setInPipeline(inPipeline === "1" ? "" : "1")}
              >
                Déjà dans le pipeline
              </Toggle>
            </div>
          </Field>
          <Field label="Qualité de matching DPE">
            <div className="flex flex-wrap gap-1.5">
              {(["verified", "approximate", "uncertain", "no_data"] as const).map((q) => (
                <button
                  key={q}
                  onClick={() => toggleQuality(q)}
                  className={cn(
                    "rounded-full border px-2.5 py-1 text-[11px] font-medium transition-colors",
                    qualityFilter.has(q)
                      ? "border-transparent text-white"
                      : "border-border bg-background hover:bg-secondary",
                  )}
                  style={
                    qualityFilter.has(q)
                      ? { background: QUALITY_COLORS[q] }
                      : undefined
                  }
                >
                  {QUALITY_LABELS[q]}
                </button>
              ))}
            </div>
          </Field>
        </div>
      ) : null}

      {/* HEADER (count + pagination) */}
      <div className="flex items-center justify-between gap-2 border-b border-border bg-card/40 px-4 py-2 text-xs">
        <div className="flex flex-wrap items-center gap-3 text-muted-foreground">
          {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
          <span>
            <span className="font-bold text-foreground">
              {count.toLocaleString("fr-FR")}
            </span>{" "}
            résultat{count > 1 ? "s" : ""}{activeFilterCount > 0 ? " (filtré)" : ""}
            {" · "}
            <span className="text-muted-foreground">
              {totalInDb.toLocaleString("fr-FR")} copros en base
            </span>
          </span>
          {computing ? (
            <span className="flex items-center gap-1.5 rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-900">
              <Loader2 className="h-3 w-3 animate-spin" />
              Calcul DPE : {computing.done}/{computing.total}
            </span>
          ) : null}
          {recomputing ? (
            <span className="flex items-center gap-1.5 rounded-full bg-orange-100 px-2 py-0.5 text-[10px] font-semibold text-orange-900">
              <Loader2 className="h-3 w-3 animate-spin" />
              Recalcul forcé : {recomputing.done}/{recomputing.total}
            </span>
          ) : null}
          <label className="flex cursor-pointer items-center gap-1.5 text-[10px]">
            <input
              type="checkbox"
              checked={autoCompute}
              onChange={(e) => setAutoCompute(e.target.checked)}
              className="h-3 w-3"
            />
            Pré‑calculer DPE des résultats
          </label>
          <button
            type="button"
            onClick={forceRecompute}
            disabled={
              rows.length === 0 ||
              recomputing !== null ||
              computing !== null
            }
            className="inline-flex items-center gap-1 rounded-lg border border-orange-300 bg-orange-50 px-2 py-1 text-[10px] font-semibold text-orange-900 hover:bg-orange-100 disabled:opacity-50"
            title="Force le recalcul du DPE pour les copropriétés visibles (écrase le cache)"
          >
            <RefreshCw className="h-3 w-3" />
            Forcer recalcul ({rows.length})
          </button>
          <a
            href={`/api/export/copros-by-filter.xlsx?${qs}`}
            className="inline-flex items-center gap-1 rounded-lg bg-emerald-600 px-2 py-1 text-[10px] font-bold text-white shadow-sm transition-colors hover:bg-emerald-700"
            title={`Exporter les ${count.toLocaleString("fr-FR")} résultats du filtre en Excel`}
          >
            <FileSpreadsheet className="h-3 w-3" />
            Exporter tout (Excel)
          </a>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled={page === 1} onClick={() => setPage((p) => Math.max(1, p - 1))}>
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <span className="text-muted-foreground">
            Page <span className="font-bold text-foreground">{page}</span> / {pages}
          </span>
          <Button variant="outline" size="sm" disabled={page >= pages} onClick={() => setPage((p) => Math.min(pages, p + 1))}>
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>

      {/* TABLE */}
      <div className="flex-1 overflow-auto p-4 pb-24">
        <div
          className={cn(
            "overflow-hidden rounded-xl border border-border bg-card transition-opacity",
            loading && "opacity-50",
          )}
        >
          <table className="w-full text-sm">
            <thead className="border-b border-border bg-secondary/50 text-left text-xs uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="w-10 px-3 py-2">
                  <button onClick={toggleAll} aria-label="Tout sélectionner">
                    {selected.size === rows.length && rows.length > 0 ? (
                      <CheckSquare className="h-4 w-4 text-primary" />
                    ) : (
                      <Square className="h-4 w-4 text-muted-foreground" />
                    )}
                  </button>
                </th>
                <SortHeader label="Copropriété" col="name" sort={sort} setSort={setSort} />
                <SortHeader label="Localisation" col="commune" sort={sort} setSort={setSort} />
                <SortHeader label="Lots" col="lots" sort={sort} setSort={setSort} />
                <SortHeader label="Période" col="periode" sort={sort} setSort={setSort} />
                <SortHeader label="Syndic" col="syndic" sort={sort} setSort={setSort} />
                <SortHeader label="DPE" col="dpe" sort={sort} setSort={setSort} />
                <SortHeader label="Conso" col="conso" sort={sort} setSort={setSort} />
                <th className="px-3 py-2">Pipeline</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const isSelected = selected.has(r.id);
                return (
                  <tr
                    key={r.id}
                    className={cn(
                      "border-b border-border last:border-0 hover:bg-secondary/30",
                      isSelected && "bg-primary/5",
                    )}
                  >
                    <td className="px-3 py-2">
                      <button onClick={() => toggleOne(r.id)}>
                        {isSelected ? (
                          <CheckSquare className="h-4 w-4 text-primary" />
                        ) : (
                          <Square className="h-4 w-4 text-muted-foreground" />
                        )}
                      </button>
                    </td>
                    <td className="px-3 py-2">
                      <Link
                        href={`/copros/${r.id}`}
                        className="flex items-center gap-2 font-medium hover:text-primary"
                      >
                        <Building2 className="h-3.5 w-3.5 text-primary" />
                        {r.nom_copro || r.adresse || `Copro #${r.id}`}
                      </Link>
                      <p className="font-mono text-[10px] text-muted-foreground">
                        {r.numero_immatriculation}
                      </p>
                    </td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">
                      <span className="block">{r.adresse}</span>
                      <span className="block">
                        {r.code_postal} {r.commune}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-xs">{r.nb_lots_habitation ?? "—"}</td>
                    <td className="px-3 py-2 text-[11px] text-muted-foreground">
                      {(r.periode_construction ?? "—").replace(/_/g, " ").replace("AVANT", "<").replace("APRES", ">")}
                    </td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">
                      {r.syndic ?? "—"}
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-1.5">
                        <DpeBadge classe={r.classe_finale} size="sm" />
                        {r.classe_reelle ? (
                          <span title="DPE collectif réel">
                            <TrendingUp className="h-3 w-3 text-emerald-600" />
                          </span>
                        ) : null}
                        {r.quality_level ? (
                          <span
                            className="h-2 w-2 shrink-0 rounded-full"
                            title={QUALITY_LABELS[r.quality_level]}
                            style={{ background: QUALITY_COLORS[r.quality_level] }}
                          />
                        ) : null}
                      </div>
                    </td>
                    <td className="px-3 py-2 text-xs">
                      {r.conso_moyenne ? `${r.conso_moyenne} kWhep` : "—"}
                    </td>
                    <td className="px-3 py-2">
                      {r.prospect_id ? (
                        <Link
                          href={`/prospects/${r.prospect_id}`}
                          className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-semibold hover:underline"
                          style={{
                            background: stageMeta(r.prospect_stage ?? "lead").color + "20",
                            color: stageMeta(r.prospect_stage ?? "lead").color,
                          }}
                        >
                          {stageMeta(r.prospect_stage ?? "lead").label}
                        </Link>
                      ) : (
                        <span className="text-[10px] text-muted-foreground">—</span>
                      )}
                    </td>
                  </tr>
                );
              })}
              {!loading && rows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="p-8 text-center text-sm text-muted-foreground">
                    Aucune copropriété ne correspond à vos critères.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      {/* STICKY BULK ACTIONS */}
      {selected.size > 0 ? (
        <div className="fixed bottom-4 left-1/2 z-30 -translate-x-1/2">
          <div className="flex items-center gap-3 rounded-full border border-border bg-card px-4 py-2 shadow-lg">
            <span className="text-sm font-semibold">
              <span className="text-primary">{selected.size}</span> sélectionné{selected.size > 1 ? "s" : ""}
            </span>
            <span className="h-4 w-px bg-border" />
            <Button size="sm" variant="ghost" onClick={() => setSelected(new Set())}>
              <X className="h-3.5 w-3.5" /> Effacer
            </Button>
            <Button size="sm" onClick={bulkAddToPipeline} disabled={bulkAddBusy}>
              {bulkAddBusy ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Plus className="h-3.5 w-3.5" />
              )}
              Ajouter au pipeline
            </Button>
            <Button size="sm" variant="outline" onClick={bulkExportXlsx}>
              <FileSpreadsheet className="h-3.5 w-3.5" /> Exporter Excel
            </Button>
            <Button size="sm" variant="ghost" onClick={bulkExportCsv}>
              <Download className="h-3.5 w-3.5" /> CSV
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {children}
    </div>
  );
}

function Toggle({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] font-medium transition-colors",
        active
          ? "border-primary bg-primary text-primary-foreground"
          : "border-border bg-background hover:bg-secondary",
      )}
    >
      {children}
    </button>
  );
}

function Chip({
  children,
  onRemove,
}: {
  children: React.ReactNode;
  onRemove: () => void;
}) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full bg-primary/10 px-2 py-0.5 font-medium text-primary">
      {children}
      <button
        onClick={onRemove}
        className="rounded-full hover:bg-primary/20"
        aria-label="Retirer ce filtre"
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}

function SortHeader({
  label,
  col,
  sort,
  setSort,
}: {
  label: string;
  col: "name" | "commune" | "lots" | "periode" | "syndic" | "dpe" | "conso";
  sort: SortKey;
  setSort: (s: SortKey) => void;
}) {
  const asc = `${col}_asc` as SortKey;
  const desc = `${col}_desc` as SortKey;
  const isAsc = sort === asc;
  const isDesc = sort === desc;
  const isActive = isAsc || isDesc;
  const onClick = () => {
    if (isAsc) setSort(desc);
    else setSort(asc);
  };
  return (
    <th className="px-3 py-2">
      <button
        onClick={onClick}
        className={cn(
          "flex items-center gap-1 transition-colors hover:text-foreground",
          isActive && "text-primary",
        )}
      >
        {label}
        {isAsc ? (
          <ChevronUp className="h-3 w-3" />
        ) : isDesc ? (
          <ChevronDown className="h-3 w-3" />
        ) : (
          <ChevronDown className="h-3 w-3 opacity-30" />
        )}
      </button>
    </th>
  );
}

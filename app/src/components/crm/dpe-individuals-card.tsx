"use client";
import { useMemo, useState } from "react";
import { Calendar, ArrowDownUp, X, ExternalLink } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { cn } from "@/lib/utils";

type Rec = {
  numero_dpe: string | null;
  date: string | null;
  classe: string;
  ges: string;
  conso: number | null;
  surface: number | null;
  numero_voie: string | null;
  rue: string | null;
  code_postal: string | null;
  commune: string | null;
  isCollectif: boolean;
};

const CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;

type SortKey = "date_desc" | "date_asc" | "conso_desc" | "conso_asc" | "classe";

export function DpeIndividualsCard({
  details,
}: {
  details: { matchedRecords?: Rec[]; totalDpeFound: number } | null;
}) {
  const [classes, setClasses] = useState<Set<string>>(new Set());
  const [collectif, setCollectif] = useState<"" | "1" | "0">("");
  const [surfaceMin, setSurfaceMin] = useState("");
  const [surfaceMax, setSurfaceMax] = useState("");
  const [dateMin, setDateMin] = useState("");
  const [sort, setSort] = useState<SortKey>("date_desc");

  const records = details?.matchedRecords ?? [];

  const filtered = useMemo(() => {
    let out = records;
    if (classes.size > 0) out = out.filter((r) => classes.has(r.classe));
    if (collectif === "1") out = out.filter((r) => r.isCollectif);
    if (collectif === "0") out = out.filter((r) => !r.isCollectif);
    const sMin = Number(surfaceMin);
    const sMax = Number(surfaceMax);
    if (Number.isFinite(sMin) && surfaceMin)
      out = out.filter((r) => (r.surface ?? 0) >= sMin);
    if (Number.isFinite(sMax) && surfaceMax)
      out = out.filter((r) => (r.surface ?? 99999) <= sMax);
    if (dateMin)
      out = out.filter((r) => (r.date ?? "0000") >= dateMin);
    const arr = [...out];
    arr.sort((a, b) => {
      if (sort === "date_desc") return (b.date ?? "").localeCompare(a.date ?? "");
      if (sort === "date_asc") return (a.date ?? "").localeCompare(b.date ?? "");
      if (sort === "conso_desc") return (b.conso ?? 0) - (a.conso ?? 0);
      if (sort === "conso_asc") return (a.conso ?? 0) - (b.conso ?? 0);
      if (sort === "classe") return (a.classe ?? "").localeCompare(b.classe ?? "");
      return 0;
    });
    return arr;
  }, [records, classes, collectif, surfaceMin, surfaceMax, dateMin, sort]);

  const reset = () => {
    setClasses(new Set());
    setCollectif("");
    setSurfaceMin("");
    setSurfaceMax("");
    setDateMin("");
  };

  const filterCount =
    classes.size + (collectif ? 1 : 0) + (surfaceMin ? 1 : 0) + (surfaceMax ? 1 : 0) + (dateMin ? 1 : 0);

  const toggleClass = (c: string) => {
    const next = new Set(classes);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    setClasses(next);
  };

  return (
    <Card className="print:shadow-none">
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="flex items-center gap-2">
          <Calendar className="h-4 w-4 text-primary" />
          <CardTitle className="text-sm">
            DPE individuels retenus pour l'estimation immeuble
          </CardTitle>
        </div>
        <span className="text-xs text-muted-foreground">
          {filtered.length} affiché{filtered.length > 1 ? "s" : ""} / {records.length} retenus pour l'immeuble
        </span>
      </CardHeader>

      {/* FILTERS */}
      <div className="border-y border-border/40 bg-secondary/30 px-5 py-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs">
          <div className="flex items-center gap-1.5">
            <span className="font-semibold text-muted-foreground">Classe :</span>
            {CLASSES.map((c) => {
              const active = classes.has(c);
              return (
                <button
                  key={c}
                  onClick={() => toggleClass(c)}
                  className={cn(
                    "transition-transform",
                    active ? "scale-110 ring-2 ring-foreground/70 ring-offset-1" : "opacity-50 hover:opacity-100",
                  )}
                >
                  <DpeBadge classe={c} size="sm" />
                </button>
              );
            })}
          </div>
          <div className="flex items-center gap-1.5">
            <span className="font-semibold text-muted-foreground">Type :</span>
            <Toggle active={collectif === "1"} onClick={() => setCollectif(collectif === "1" ? "" : "1")}>Collectif</Toggle>
            <Toggle active={collectif === "0"} onClick={() => setCollectif(collectif === "0" ? "" : "0")}>Individuel</Toggle>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="font-semibold text-muted-foreground">Surface m² :</span>
            <Input
              value={surfaceMin}
              onChange={(e) => setSurfaceMin(e.target.value)}
              placeholder="min"
              className="h-8 w-20 text-xs"
              inputMode="numeric"
            />
            <Input
              value={surfaceMax}
              onChange={(e) => setSurfaceMax(e.target.value)}
              placeholder="max"
              className="h-8 w-20 text-xs"
              inputMode="numeric"
            />
          </div>
          <div className="flex items-center gap-1.5">
            <span className="font-semibold text-muted-foreground">Depuis :</span>
            <Input
              type="date"
              value={dateMin}
              onChange={(e) => setDateMin(e.target.value)}
              className="h-8 w-36 text-xs"
            />
          </div>
          <div className="flex items-center gap-1.5">
            <ArrowDownUp className="h-3 w-3 text-muted-foreground" />
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value as SortKey)}
              className="h-8 rounded-lg border border-input bg-background px-2 text-xs"
            >
              <option value="date_desc">Date ↓</option>
              <option value="date_asc">Date ↑</option>
              <option value="conso_desc">Conso ↓</option>
              <option value="conso_asc">Conso ↑</option>
              <option value="classe">Classe</option>
            </select>
          </div>
          {filterCount > 0 ? (
            <Button variant="ghost" size="sm" onClick={reset} className="ml-auto h-8">
              <X className="h-3 w-3" /> Effacer ({filterCount})
            </Button>
          ) : null}
        </div>
      </div>

      <CardContent>
        {!details ? (
          <p className="text-sm text-muted-foreground">Chargement…</p>
        ) : records.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            Aucun DPE individuel matché par le scoring d'adresse. Cliquez sur
            "Recalculer depuis l'ADEME" pour ré‑interroger.
          </p>
        ) : filtered.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            Aucun DPE ne correspond à ces filtres.
          </p>
        ) : (
          <div className="overflow-auto">
            <table className="w-full text-xs">
              <thead className="border-b border-border text-left text-[10px] uppercase tracking-wider text-muted-foreground">
                <tr>
                  <th className="py-2 pr-2">Classe</th>
                  <th className="py-2 pr-2">GES</th>
                  <th className="py-2 pr-2">Conso (kWhep/m²/an)</th>
                  <th className="py-2 pr-2">Surface m²</th>
                  <th className="py-2 pr-2">Adresse</th>
                  <th className="py-2 pr-2">Type</th>
                  <th className="py-2 pr-2">Date</th>
                  <th className="py-2 pr-2">N° DPE</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r, i) => (
                  <tr key={i} className="border-b border-border/40 last:border-0">
                    <td className="py-1.5 pr-2">
                      <DpeBadge classe={r.classe} size="sm" />
                    </td>
                    <td className="py-1.5 pr-2">
                      <DpeBadge classe={r.ges} size="sm" />
                    </td>
                    <td className="py-1.5 pr-2 font-semibold">{r.conso ?? "—"}</td>
                    <td className="py-1.5 pr-2">{r.surface ?? "—"}</td>
                    <td className="py-1.5 pr-2 text-muted-foreground">
                      {r.numero_voie ?? ""} {r.rue ?? ""}
                      {r.code_postal || r.commune ? (
                        <span className="block text-[10px]">
                          {r.code_postal} {r.commune}
                        </span>
                      ) : null}
                    </td>
                    <td className="py-1.5 pr-2">
                      {r.isCollectif ? (
                        <Badge variant="success" className="text-[10px]">
                          Collectif
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">Individuel</span>
                      )}
                    </td>
                    <td className="py-1.5 pr-2">
                      {r.date ? new Date(r.date).toLocaleDateString("fr-FR") : "—"}
                    </td>
                    <td className="py-1.5 pr-2 font-mono text-[10px]">
                      {r.numero_dpe ? (
                        <a
                          href={`https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${r.numero_dpe}`}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-primary hover:underline"
                          title="Ouvrir la fiche DPE officielle sur ADEME"
                        >
                          {r.numero_dpe}
                          <ExternalLink className="h-2.5 w-2.5" />
                        </a>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
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
        "rounded-full border px-2 py-0.5 text-[10px] font-medium transition-colors",
        active ? "border-primary bg-primary text-primary-foreground" : "border-border hover:bg-secondary",
      )}
    >
      {children}
    </button>
  );
}

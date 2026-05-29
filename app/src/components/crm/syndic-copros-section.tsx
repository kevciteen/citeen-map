"use client";
/**
 * Section "Copros gérées" sur la fiche syndic /syndics/[slug].
 *
 * Affiche les copros gérées par le syndic :
 *   - Mini-carte MapLibre coloriée par DPE
 *   - Filtres DPE et lots min
 *   - Table compacte triée par classe DPE (G en premier = passoires prioritaires)
 *   - Bouton "Exporter campagne CSV" qui prépare un fichier prêt à phoner
 */
import { useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
  Building2, Loader2, Download, Filter, ChevronRight,
} from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { AnnuaireMap, DPE_COLORS, type AnnuaireMapPoint } from "@/components/annuaire/annuaire-map";

type CoproItem = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  lat: number | null;
  lon: number | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  classe_finale: string | null;
};

type CoproResp = {
  name: string;
  count: number;
  items: CoproItem[];
  dpeDistribution: Array<{ classe: string; n: number }>;
};

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G", "NC"] as const;

export function SyndicCoprosSection({
  slug,
  name,
}: {
  slug: string;
  name: string;
}) {
  const [dpeFilter, setDpeFilter] = useState<string[]>([]);
  const [minLots, setMinLots] = useState("");

  const buildQuery = () => {
    const p = new URLSearchParams();
    p.set("name", name);
    if (dpeFilter.length > 0) p.set("dpe", dpeFilter.join(","));
    if (minLots.trim()) p.set("minLots", minLots.trim());
    return p.toString();
  };

  const { data, isPending } = useQuery({
    queryKey: ["syndic-copros", slug, name, dpeFilter.join(","), minLots],
    queryFn: ({ signal }) =>
      jsonFetcher<CoproResp>(`/api/syndics/${slug}/copros?${buildQuery()}`, signal),
    staleTime: 60 * 1000,
  });

  const items = data?.items ?? [];
  const mapPoints: AnnuaireMapPoint[] = items
    .filter((c) => c.lat != null && c.lon != null)
    .map((c) => ({
      id: c.id,
      entity_type: "copro",
      entity_ref: String(c.id),
      display_name: c.nom_copro || c.adresse || c.numero_immatriculation,
      display_subtitle: [c.adresse, c.code_postal, c.commune].filter(Boolean).join(" "),
      lat: c.lat as number,
      lon: c.lon as number,
      phone: null,
      email: null,
      website: null,
      dpe_class: c.classe_finale,
    }));

  const toggleDpe = (k: string) => {
    setDpeFilter((prev) =>
      prev.includes(k) ? prev.filter((x) => x !== k) : [...prev, k],
    );
  };

  const exportCsv = () => {
    // Reuse l'export directory mais filtré sur ce syndic.
    // Le syndic n'est PAS dans `directory` comme parent → on génère le CSV
    // côté client à partir des items chargés (simple et suffisant ≤ 2000 lignes).
    const headers = [
      "numero_immatriculation", "nom_copro", "adresse",
      "code_postal", "commune", "nb_lots", "dpe",
    ];
    const cell = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return /[",;\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [
      headers.join(";"),
      ...items.map((c) => [
        c.numero_immatriculation, c.nom_copro, c.adresse,
        c.code_postal, c.commune,
        c.nb_lots_habitation ?? c.nb_lots,
        c.classe_finale,
      ].map(cell).join(";")),
    ];
    const csv = "﻿" + lines.join("\r\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const filtersSuffix = dpeFilter.length > 0 ? `-DPE-${dpeFilter.join("")}` : "";
    a.download = `copros-${slug}${filtersSuffix}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <section className="space-y-3 rounded-xl border border-border bg-card p-5 shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="flex items-center gap-2 text-sm font-bold">
          <Building2 className="h-4 w-4 text-primary" />
          Copros gérées
          {data ? (
            <span className="text-xs font-normal text-muted-foreground">
              ({data.count} affichées
              {dpeFilter.length > 0 || minLots ? " · filtrées" : ""})
            </span>
          ) : null}
        </h2>
        <Button
          variant="outline"
          size="sm"
          onClick={exportCsv}
          disabled={items.length === 0}
        >
          <Download className="h-3.5 w-3.5" />
          Exporter campagne CSV
        </Button>
      </div>

      {/* Filtres */}
      <div className="flex flex-wrap items-end gap-3 rounded-md border border-border bg-secondary/30 p-3 text-xs">
        <div className="flex-1 min-w-[260px]">
          <p className="mb-1 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            <Filter className="h-3 w-3" /> Filtrer par classe DPE
          </p>
          <div className="flex flex-wrap gap-1">
            {DPE_CLASSES.map((k) => (
              <button
                key={k}
                onClick={() => toggleDpe(k)}
                className={
                  "rounded-md border px-2 py-0.5 text-[11px] font-semibold transition-colors " +
                  (dpeFilter.includes(k)
                    ? "border-transparent text-white"
                    : "border-border bg-background hover:bg-secondary")
                }
                style={
                  dpeFilter.includes(k)
                    ? { background: DPE_COLORS[k] }
                    : undefined
                }
              >
                {k}
              </button>
            ))}
            {dpeFilter.length > 0 ? (
              <button
                onClick={() => setDpeFilter([])}
                className="rounded-md px-2 py-0.5 text-[11px] text-muted-foreground hover:bg-secondary"
              >
                × Effacer
              </button>
            ) : null}
          </div>
        </div>
        <div>
          <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Lots min
          </label>
          <Input
            type="number"
            min="0"
            value={minLots}
            onChange={(e) => setMinLots(e.target.value)}
            placeholder="ex: 30"
            className="h-8 w-24"
          />
        </div>
      </div>

      {/* Mini-carte (couleur DPE) */}
      {isPending ? (
        <Skeleton className="h-72 w-full" />
      ) : mapPoints.length > 0 ? (
        <AnnuaireMap points={mapPoints} colorMode="dpe" />
      ) : (
        <p className="rounded-md border border-border bg-secondary/30 p-3 text-center text-xs text-muted-foreground">
          Aucune copro avec coordonnées géolocalisées.
        </p>
      )}

      {/* Table compacte */}
      <div className="overflow-hidden rounded-md border border-border">
        {isPending ? (
          <div className="space-y-2 p-3">
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
          </div>
        ) : items.length === 0 ? (
          <p className="p-6 text-center text-xs text-muted-foreground">
            Aucune copro ne correspond aux filtres.
          </p>
        ) : (
          <table className="w-full text-xs">
            <thead className="border-b border-border bg-secondary/40 text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">DPE</th>
                <th className="px-3 py-2 text-left">Nom / Adresse</th>
                <th className="px-3 py-2 text-left">Commune</th>
                <th className="px-3 py-2 text-right">Lots</th>
                <th className="px-2 py-2"></th>
              </tr>
            </thead>
            <tbody>
              {items.map((c) => {
                const lots = c.nb_lots_habitation ?? c.nb_lots;
                const klass = c.classe_finale ?? "NC";
                return (
                  <tr
                    key={c.id}
                    className="border-b border-border last:border-b-0 hover:bg-secondary/30"
                  >
                    <td className="px-3 py-2">
                      <span
                        className="inline-flex h-6 w-6 items-center justify-center rounded text-[11px] font-bold text-white"
                        style={{ background: DPE_COLORS[klass] }}
                      >
                        {klass}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <Link
                        href={`/copros/${c.id}`}
                        className="font-medium text-foreground hover:underline"
                      >
                        {c.nom_copro || c.adresse || c.numero_immatriculation}
                      </Link>
                      {c.nom_copro && c.adresse ? (
                        <p className="text-[10px] text-muted-foreground">{c.adresse}</p>
                      ) : null}
                    </td>
                    <td className="px-3 py-2 text-muted-foreground">
                      {c.commune}
                      {c.code_postal ? <span className="ml-1 font-mono text-[10px]">{c.code_postal}</span> : null}
                    </td>
                    <td className="px-3 py-2 text-right font-mono">{lots ?? "—"}</td>
                    <td className="px-2 py-2">
                      <Link
                        href={`/copros/${c.id}`}
                        className="text-muted-foreground hover:text-primary"
                      >
                        <ChevronRight className="h-4 w-4" />
                      </Link>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

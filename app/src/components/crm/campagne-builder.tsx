"use client";
/**
 * Wizard de construction d'une campagne de prospection.
 *
 * 1. CRITÈRES : DPE classes, dept, CP, syndic, lots min, période construction
 * 2. PREVIEW : count + carte coloriée + table + distribution DPE
 * 3. LANCER : crée N prospects via /api/prospects/bulk (stage = to_contact)
 */
import { useMemo, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import {
  Filter, MapPin, Loader2, Send, Rocket, ChevronRight,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useDebouncedValue } from "@/lib/hooks/use-debounced-value";
import { jsonFetcher } from "@/lib/fetcher";
import { toast } from "sonner";
import { AnnuaireMap, DPE_COLORS, type AnnuaireMapPoint } from "@/components/annuaire/annuaire-map";

type CoproRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  lat: number | null;
  lon: number | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  classe_finale: string | null;
  has_prospect: number;
};

type Preview = {
  count: number;
  inPipeline: number;
  eligible: number;
  dpeDistribution: Array<{ classe: string; n: number }>;
  items: CoproRow[];
};

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G", "NC"] as const;
const DEPARTEMENTS_IDF = ["75", "77", "78", "91", "92", "93", "94", "95"];
const PERIODES = [
  "Avant 1949",
  "1949-1974",
  "1975-1989",
  "1990-2005",
  "Après 2005",
];

export function CampagneBuilder() {
  const router = useRouter();
  // CRITÈRES
  const [dpe, setDpe] = useState<string[]>(["F", "G"]);
  const [dept, setDept] = useState("");
  const [cp, setCp] = useState("");
  const [syndic, setSyndic] = useState("");
  const [minLots, setMinLots] = useState("20");
  const [periode, setPeriode] = useState("");
  const [onlyNew, setOnlyNew] = useState(true);
  const [launching, setLaunching] = useState(false);

  const debouncedCp = useDebouncedValue(cp, 250);
  const debouncedSyndic = useDebouncedValue(syndic, 250);
  const debouncedMinLots = useDebouncedValue(minLots, 250);

  const buildQuery = () => {
    const p = new URLSearchParams();
    if (dpe.length > 0) p.set("dpe", dpe.join(","));
    if (dept) p.set("dept", dept);
    if (debouncedCp) p.set("cp", debouncedCp);
    if (debouncedSyndic) p.set("syndic", debouncedSyndic);
    if (debouncedMinLots) p.set("minLots", debouncedMinLots);
    if (periode) p.set("periode", periode);
    if (onlyNew) p.set("onlyNew", "1");
    p.set("sample", "300");
    return p.toString();
  };

  const previewKey = [
    "campagne-preview",
    dpe.join(","), dept, debouncedCp, debouncedSyndic,
    debouncedMinLots, periode, onlyNew,
  ];

  const { data, isPending, isFetching, refetch } = useQuery({
    queryKey: previewKey,
    queryFn: ({ signal }) =>
      jsonFetcher<Preview>(`/api/campagnes/preview?${buildQuery()}`, signal),
    enabled: dpe.length > 0 || dept !== "" || debouncedCp !== "" || debouncedSyndic !== "",
    staleTime: 30 * 1000,
  });

  const items = data?.items ?? [];
  const mapPoints: AnnuaireMapPoint[] = useMemo(
    () =>
      items
        .filter((c) => c.lat != null && c.lon != null)
        .map((c) => ({
          id: c.id,
          entity_type: "copro",
          entity_ref: String(c.id),
          display_name: c.nom_copro || c.adresse || c.numero_immatriculation,
          display_subtitle: c.syndic ? `Syndic : ${c.syndic}` : null,
          lat: c.lat as number,
          lon: c.lon as number,
          phone: null,
          email: null,
          website: null,
          dpe_class: c.classe_finale,
        })),
    [items],
  );

  const toggleDpe = (k: string) =>
    setDpe((prev) =>
      prev.includes(k) ? prev.filter((x) => x !== k) : [...prev, k],
    );

  const launch = async () => {
    if (!data) return;
    const eligibleIds = items
      .filter((c) => !c.has_prospect)
      .map((c) => c.id)
      .slice(0, 1000); // batch hard-cap
    if (eligibleIds.length === 0) {
      toast.error("Aucun prospect éligible à créer dans l'échantillon");
      return;
    }
    if (
      !window.confirm(
        `Créer ${eligibleIds.length} prospects en stage "À contacter" ?\n\nNote : seul l'échantillon affiché (${items.length} copros) est créé en une fois. Re-lance plusieurs fois pour traiter au-delà.`,
      )
    ) {
      return;
    }
    setLaunching(true);
    try {
      const r = await fetch("/api/prospects/bulk", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          coproIds: eligibleIds,
          stage: "to_contact",
          priority: 2,
          tags: ["campagne", ...(dpe.length > 0 ? [`dpe-${dpe.join("")}`] : [])],
        }),
      });
      const j = await r.json();
      if (!r.ok) {
        toast.error(j.error ?? "Erreur lors du lancement");
        return;
      }
      toast.success(
        `${j.created} prospect(s) créé(s), ${j.alreadyExists} déjà existant(s) ignoré(s)`,
      );
      void refetch();
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setLaunching(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* CRITÈRES */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <Filter className="h-4 w-4 text-primary" />
          1. Critères de la campagne
        </h2>

        <div className="space-y-3">
          {/* DPE chips */}
          <div>
            <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Classes DPE ciblées
            </p>
            <div className="flex flex-wrap gap-1.5">
              {DPE_CLASSES.map((k) => (
                <button
                  key={k}
                  onClick={() => toggleDpe(k)}
                  className={
                    "rounded-md border px-2.5 py-1 text-xs font-bold transition-colors " +
                    (dpe.includes(k)
                      ? "border-transparent text-white shadow-sm"
                      : "border-border bg-background hover:bg-secondary")
                  }
                  style={
                    dpe.includes(k) ? { background: DPE_COLORS[k] } : undefined
                  }
                >
                  {k}
                </button>
              ))}
              <button
                onClick={() => setDpe(["F", "G"])}
                className="rounded-md border border-rose-300 bg-rose-50 px-2 py-1 text-xs font-semibold text-rose-900 hover:bg-rose-100"
              >
                🎯 Passoires F+G
              </button>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div>
              <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Département
              </label>
              <select
                value={dept}
                onChange={(e) => setDept(e.target.value)}
                className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
              >
                <option value="">— tous —</option>
                {DEPARTEMENTS_IDF.map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Code postal
              </label>
              <Input
                placeholder="ex: 75011"
                value={cp}
                onChange={(e) => setCp(e.target.value)}
                maxLength={5}
                className="h-9"
              />
            </div>
            <div>
              <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Lots minimum
              </label>
              <Input
                type="number"
                min="0"
                value={minLots}
                onChange={(e) => setMinLots(e.target.value)}
                placeholder="ex: 20"
                className="h-9"
              />
            </div>
            <div>
              <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Période construction
              </label>
              <select
                value={periode}
                onChange={(e) => setPeriode(e.target.value)}
                className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
              >
                <option value="">— toutes —</option>
                {PERIODES.map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Syndic (nom exact)
              </label>
              <Input
                placeholder="laisse vide pour tous"
                value={syndic}
                onChange={(e) => setSyndic(e.target.value)}
                className="h-9"
              />
            </div>
            <div className="flex items-end">
              <label className="flex cursor-pointer items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={onlyNew}
                  onChange={(e) => setOnlyNew(e.target.checked)}
                  className="rounded"
                />
                Exclure les copros déjà en pipeline
              </label>
            </div>
          </div>
        </div>
      </section>

      {/* PREVIEW */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <MapPin className="h-4 w-4 text-primary" />
          2. Aperçu de la cible
          {isFetching ? (
            <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
          ) : null}
        </h2>

        {!data && !isPending ? (
          <p className="rounded-md border border-border bg-secondary/30 p-4 text-center text-xs text-muted-foreground">
            Choisis au moins un critère pour voir l&apos;aperçu.
          </p>
        ) : isPending ? (
          <div className="space-y-3">
            <div className="grid grid-cols-3 gap-3">
              <Skeleton className="h-16" />
              <Skeleton className="h-16" />
              <Skeleton className="h-16" />
            </div>
            <Skeleton className="h-72 w-full" />
          </div>
        ) : data ? (
          <>
            {/* KPIs */}
            <div className="mb-3 grid grid-cols-3 gap-3">
              <Kpi
                label="Total ciblé"
                value={data.count.toLocaleString("fr-FR")}
                hint="copros matching"
              />
              <Kpi
                label="Éligibles"
                value={data.eligible.toLocaleString("fr-FR")}
                accent="primary"
                hint="à créer (hors pipeline)"
              />
              <Kpi
                label="Déjà en pipeline"
                value={data.inPipeline.toLocaleString("fr-FR")}
                hint="prospects existants"
              />
            </div>

            {/* Distribution DPE */}
            {data.dpeDistribution.length > 0 ? (
              <div className="mb-3 flex h-3 w-full overflow-hidden rounded-full bg-secondary">
                {data.dpeDistribution.map((d) => {
                  const pct = data.count > 0 ? (d.n / data.count) * 100 : 0;
                  return (
                    <span
                      key={d.classe}
                      title={`${d.classe} — ${d.n} copros (${pct.toFixed(0)}%)`}
                      style={{ width: `${pct}%`, background: DPE_COLORS[d.classe] ?? "#94a3b8" }}
                    />
                  );
                })}
              </div>
            ) : null}

            {/* Carte */}
            {mapPoints.length > 0 ? (
              <AnnuaireMap points={mapPoints} colorMode="dpe" />
            ) : (
              <p className="rounded-md border border-border bg-secondary/30 p-4 text-center text-xs text-muted-foreground">
                Aucune copro géolocalisée dans l&apos;échantillon affiché.
              </p>
            )}

            <p className="mt-2 text-[11px] text-muted-foreground">
              Aperçu : {items.length} copros / {data.count.toLocaleString("fr-FR")}.
              Pour les très grandes cibles ({">"}300), élargis ton filtre dans
              l&apos;onglet <Link href="/annuaire" className="text-primary hover:underline">Annuaire</Link>.
            </p>
          </>
        ) : null}
      </section>

      {/* LANCER */}
      {data && data.eligible > 0 ? (
        <section className="rounded-xl border border-primary/40 bg-primary/5 p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Rocket className="h-4 w-4 text-primary" />
            3. Lancer la campagne
          </h2>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-sm">
              Crée <strong>{Math.min(items.filter((c) => !c.has_prospect).length, 1000)}</strong>{" "}
              nouveaux prospects en stage{" "}
              <span className="rounded bg-secondary/60 px-1.5 py-0.5 text-xs font-semibold">
                À contacter
              </span>{" "}
              avec le tag <span className="font-mono text-xs">campagne</span>.
            </p>
            <div className="flex items-center gap-2">
              <Button
                onClick={launch}
                disabled={launching}
                className="bg-primary text-primary-foreground hover:bg-primary/90"
              >
                {launching ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Send className="h-4 w-4" />
                )}
                Lancer
              </Button>
              <Link
                href="/pilotage"
                className="inline-flex items-center gap-1 rounded-md border border-border bg-card px-3 py-2 text-xs font-semibold hover:bg-secondary"
              >
                Voir pipeline <ChevronRight className="h-3 w-3" />
              </Link>
            </div>
          </div>
        </section>
      ) : null}
    </div>
  );
}

function Kpi({
  label, value, hint, accent,
}: {
  label: string;
  value: string;
  hint?: string;
  accent?: "primary";
}) {
  const accentClass = accent === "primary" ? "text-primary" : "text-foreground";
  return (
    <div className="rounded-lg border border-border bg-secondary/30 p-3">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className={`text-2xl font-bold ${accentClass}`}>{value}</p>
      {hint ? <p className="text-[10px] text-muted-foreground">{hint}</p> : null}
    </div>
  );
}

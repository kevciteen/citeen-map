"use client";
/**
 * Carte d'affichage des risques Géorisques pour une commune.
 *
 * Utilisée sur :
 *  - Fiche copro (commune de la copro)
 *  - Fiche tertiaire (commune du bâtiment)
 *  - Fiche DPE détaillée (commune ADEME)
 *
 * Groupe les risques par catégorie avec icônes et couleurs.
 */
import { useQuery } from "@tanstack/react-query";
import {
  Droplets, Mountain, Waves, Thermometer, AlertTriangle,
  Factory, Truck, Flame, Atom, Wrench, Shield, ExternalLink,
  Loader2,
} from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Skeleton } from "@/components/ui/skeleton";
import { categorize, type GeorisqueCategory } from "@/lib/services/georisques";

type GeorisqueDetail = {
  num_risque: string;
  libelle_risque_long: string;
  zone_sismicite: string | null;
};

type Response = {
  code_insee?: string;
  libelle_commune?: string;
  risques: GeorisqueDetail[];
  total: number;
};

const ICON: Record<GeorisqueCategory, React.ComponentType<{ className?: string }>> = {
  inondation: Droplets,
  mouvement_terrain: Mountain,
  sismique: Waves,
  argile: Wrench,
  radon: Thermometer,
  industriel: Factory,
  transport_matieres: Truck,
  rupture_barrage: Shield,
  feux_foret: Flame,
  nucleaire: Atom,
  autre: AlertTriangle,
};

const COLOR: Record<GeorisqueCategory, string> = {
  inondation: "border-blue-300 bg-blue-50 text-blue-900",
  mouvement_terrain: "border-amber-300 bg-amber-50 text-amber-900",
  sismique: "border-purple-300 bg-purple-50 text-purple-900",
  argile: "border-orange-300 bg-orange-50 text-orange-900",
  radon: "border-rose-300 bg-rose-50 text-rose-900",
  industriel: "border-slate-300 bg-slate-50 text-slate-900",
  transport_matieres: "border-yellow-300 bg-yellow-50 text-yellow-900",
  rupture_barrage: "border-indigo-300 bg-indigo-50 text-indigo-900",
  feux_foret: "border-red-300 bg-red-50 text-red-900",
  nucleaire: "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-900",
  autre: "border-border bg-card text-foreground",
};

export function GeorisquesCard({
  codeInsee,
  commune,
}: {
  codeInsee: string | null | undefined;
  commune?: string | null;
}) {
  const enabled = Boolean(codeInsee && /^\d{5}$/.test(codeInsee));

  const { data, isPending, error } = useQuery({
    queryKey: ["georisques", codeInsee],
    queryFn: ({ signal }) =>
      jsonFetcher<Response>(`/api/georisques?codeInsee=${codeInsee}`, signal),
    enabled,
    staleTime: 60 * 60 * 1000, // 1h frontend
  });

  if (!enabled) return null;

  if (isPending) {
    return (
      <div className="rounded-xl border border-border bg-card p-4 shadow-sm">
        <div className="mb-2 flex items-center gap-2 text-sm font-bold">
          <Shield className="h-4 w-4 text-primary" />
          Risques naturels et technologiques
          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        </div>
        <Skeleton className="h-20 w-full" />
      </div>
    );
  }

  if (error || !data || data.risques.length === 0) {
    return (
      <div className="rounded-xl border border-border bg-card p-4 shadow-sm">
        <div className="mb-2 flex items-center gap-2 text-sm font-bold">
          <Shield className="h-4 w-4 text-primary" />
          Risques naturels et technologiques
        </div>
        <p className="text-xs text-muted-foreground">
          Aucun risque référencé pour la commune INSEE {codeInsee}
          {commune ? ` (${commune})` : ""}.
        </p>
      </div>
    );
  }

  // Groupe par catégorie
  const groups = new Map<GeorisqueCategory, GeorisqueDetail[]>();
  for (const r of data.risques) {
    const cat = categorize(r.libelle_risque_long);
    if (!groups.has(cat)) groups.set(cat, []);
    groups.get(cat)!.push(r);
  }

  return (
    <div className="rounded-xl border border-border bg-card p-4 shadow-sm">
      <div className="mb-3 flex items-center justify-between gap-2">
        <h2 className="flex items-center gap-2 text-sm font-bold">
          <Shield className="h-4 w-4 text-primary" />
          Risques naturels et technologiques
          <span className="rounded-full bg-secondary/60 px-2 py-0.5 text-[10px] font-normal text-muted-foreground">
            {data.total} risque{data.total > 1 ? "s" : ""}
          </span>
        </h2>
        <a
          href={`https://www.georisques.gouv.fr/mes-risques/connaitre-les-risques-pres-de-chez-moi/rapport?codeInsee=${codeInsee}`}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1 text-[10px] text-primary hover:underline"
        >
          Rapport complet Géorisques <ExternalLink className="h-2.5 w-2.5" />
        </a>
      </div>

      <div className="grid gap-2 sm:grid-cols-2">
        {[...groups.entries()].map(([cat, risques]) => {
          const Icon = ICON[cat];
          return (
            <div key={cat} className={`rounded-lg border ${COLOR[cat]} p-2.5`}>
              <div className="mb-1.5 flex items-center gap-1.5 text-xs font-bold uppercase tracking-wider">
                <Icon className="h-3.5 w-3.5" />
                {labelOf(cat)}
                <span className="text-[10px] font-normal opacity-70">
                  ({risques.length})
                </span>
              </div>
              <ul className="space-y-0.5 text-[11px] leading-snug opacity-90">
                {risques.slice(0, 4).map((r) => (
                  <li key={r.num_risque} className="truncate">
                    · {r.libelle_risque_long}
                  </li>
                ))}
                {risques.length > 4 ? (
                  <li className="opacity-70">… +{risques.length - 4} autres</li>
                ) : null}
              </ul>
            </div>
          );
        })}
      </div>
      <p className="mt-2 text-[10px] text-muted-foreground">
        Source : Géorisques (gouv.fr) — données par commune, à confirmer
        avec l&apos;ERPI (état des risques) pour le bien précis.
      </p>
    </div>
  );
}

function labelOf(cat: GeorisqueCategory): string {
  return {
    inondation: "Inondation",
    mouvement_terrain: "Mouvement de terrain",
    sismique: "Sismique",
    argile: "Argile / retrait gonflement",
    radon: "Radon",
    industriel: "Industriel / Seveso",
    transport_matieres: "Transport matières dangereuses",
    rupture_barrage: "Rupture barrage",
    feux_foret: "Feux de forêt",
    nucleaire: "Nucléaire",
    autre: "Autre",
  }[cat];
}

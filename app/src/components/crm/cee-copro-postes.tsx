"use client";
import { useMemo } from "react";
import { Coins, Info, Flame, Shield, Wind, Layers, ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  getPostesEligiblesCopro,
  type Poste,
  type PosteFamille,
} from "@/lib/services/cee/postes-eligibles";

type IndividualLite = {
  energie_principale_chauffage?: string | null;
  classe?: string;
};

/**
 * Liste les postes CEE applicables à une copropriété d'habitation à partir
 * de la classe DPE collective, de la période de construction, du nb de
 * lots, et — si disponible — des DPE individuels matchés pour identifier
 * l'énergie de chauffage dominante.
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
  /** DPE individuels matchés sur la copro (depuis l'API /dpe/details). */
  matchedIndividuals?: IndividualLite[];
}) {
  const { energieDominante, partPassoires } = useMemo(() => {
    if (!matchedIndividuals || matchedIndividuals.length === 0) {
      return { energieDominante: null, partPassoires: null };
    }
    // Compte des énergies pour trouver la dominante
    const energyCount = new Map<string, number>();
    let passoireCount = 0;
    for (const ind of matchedIndividuals) {
      const e = (ind.energie_principale_chauffage ?? "")
        .toLowerCase()
        .trim();
      if (e) energyCount.set(e, (energyCount.get(e) ?? 0) + 1);
      const c = (ind.classe ?? "").toUpperCase();
      if (c === "F" || c === "G") passoireCount += 1;
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
      partPassoires:
        matchedIndividuals.length > 0
          ? passoireCount / matchedIndividuals.length
          : null,
    };
  }, [matchedIndividuals]);

  const postes = getPostesEligiblesCopro({
    classeDpeCollective,
    periodeConstruction,
    nbLotsHabitation,
    codePostal,
    energieChauffageDominante: energieDominante,
    partPassoires,
  });

  if (postes.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
        <div className="mb-1 flex items-center gap-1 font-semibold">
          <Info className="h-3.5 w-3.5" />
          Pas assez de données pour repérer des postes CEE
        </div>
        <p>
          Charger d'abord le DPE collectif depuis l'ADEME (bouton "Recalculer
          depuis l'ADEME" plus haut) ou compléter la période de construction.
        </p>
      </div>
    );
  }

  // Regrouper par famille
  const byFamille: Record<PosteFamille, Poste[]> = {
    Bouquet: [],
    Isolation: [],
    Chauffage: [],
    "Eau chaude": [],
    Ventilation: [],
  };
  for (const p of postes) byFamille[p.famille].push(p);
  const familleOrder: PosteFamille[] = [
    "Bouquet",
    "Isolation",
    "Chauffage",
    "Eau chaude",
    "Ventilation",
  ];

  return (
    <div className="space-y-3">
      <div className="rounded-lg bg-emerald-50 p-3 text-xs text-emerald-900">
        <div className="mb-1 flex items-center gap-1 font-bold">
          <Coins className="h-3.5 w-3.5" />
          {postes.length} poste{postes.length > 1 ? "s" : ""} de travaux CEE collectif
          {postes.length > 1 ? "s" : ""}
        </div>
        <p className="text-[11px]">
          Repérage automatique depuis le DPE collectif, la période de
          construction et l'agrégation des DPE individuels. Toutes les opérations
          collectives nécessitent une décision d'assemblée générale.
        </p>
        {energieDominante ? (
          <p className="mt-1 text-[10px] italic text-emerald-800/80">
            Énergie de chauffage dominante détectée sur {matchedIndividuals?.length ?? 0} DPE
            individuels : <strong>{energieDominante}</strong>
            {partPassoires != null && partPassoires > 0
              ? ` · ${Math.round(partPassoires * 100)}% de lots en F/G`
              : ""}
          </p>
        ) : null}
      </div>

      {familleOrder.map((famille) => {
        const items = byFamille[famille];
        if (items.length === 0) return null;
        return <FamilleBloc key={famille} famille={famille} postes={items} />;
      })}
    </div>
  );
}

function FamilleBloc({
  famille,
  postes,
}: {
  famille: PosteFamille;
  postes: Poste[];
}) {
  return (
    <div>
      <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        {familleIcon(famille)}
        {famille}
      </div>
      <div className="space-y-1.5">
        {postes.map((p) => (
          <PosteRow key={`${p.code}-${p.titre}`} poste={p} />
        ))}
      </div>
    </div>
  );
}

function PosteRow({ poste }: { poste: Poste }) {
  return (
    <div
      className={cn(
        "rounded-lg border bg-card px-3 py-2 text-xs",
        poste.status === "pertinent"
          ? "border-emerald-300 bg-emerald-50/40"
          : poste.status === "à confirmer"
            ? "border-amber-200 bg-amber-50/30"
            : "border-border opacity-60",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="font-mono text-[10px] font-bold text-muted-foreground">
              {poste.code}
            </span>
            <span className="text-xs font-bold text-foreground">
              {poste.titre}
            </span>
          </div>
          <ul className="mt-1 space-y-0.5">
            {poste.motifs.map((m, i) => (
              <li key={i} className="text-[11px] text-muted-foreground">
                • {m}
              </li>
            ))}
          </ul>
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          <StatusBadge status={poste.status} />
          {poste.sourceUrl ? (
            <a
              href={poste.sourceUrl}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-0.5 text-[10px] text-muted-foreground hover:text-foreground"
              title="Fiche officielle MTE"
            >
              <ExternalLink className="h-2.5 w-2.5" />
              fiche
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: Poste["status"] }) {
  const cls =
    status === "pertinent"
      ? "bg-emerald-600 text-white"
      : status === "à confirmer"
        ? "bg-amber-500 text-white"
        : "bg-slate-400 text-white";
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider",
        cls,
      )}
    >
      {status}
    </span>
  );
}

function familleIcon(f: PosteFamille) {
  switch (f) {
    case "Isolation":
      return <Shield className="h-3 w-3 text-blue-600" />;
    case "Chauffage":
      return <Flame className="h-3 w-3 text-orange-600" />;
    case "Ventilation":
      return <Wind className="h-3 w-3 text-sky-600" />;
    case "Bouquet":
      return <Layers className="h-3 w-3 text-emerald-700" />;
    case "Eau chaude":
      return null;
  }
}

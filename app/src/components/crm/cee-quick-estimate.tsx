"use client";
import { Coins, Info, Flame, Shield, Wind, Zap, Layers, ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  getPostesEligibles,
  type Poste,
  type PosteFamille,
} from "@/lib/services/cee/postes-eligibles";

type MaisonForCee = {
  classe: string;
  annee_construction: number | null;
  isolation_murs: string | null;
  isolation_toiture: string | null;
  isolation_plancher_bas: string | null;
  isolation_menuiseries: string | null;
  energie_principale_chauffage: string | null;
  generateur_chauffage: string | null;
  generateur_ecs: string | null;
  type_ventilation: string | null;
};

/**
 * Liste les postes de travaux CEE applicables détectés depuis les champs
 * DPE ADEME. Pas de calcul de prime — c'est un outil de repérage pour la
 * prospection. Le chiffrage précis se fera dans la page /simulateur-cee
 * avec saisie des coefficients et options (audit, dimensionnement, etc.).
 */
export function CeeQuickEstimate({
  typeBatiment,
  maison,
}: {
  typeBatiment: "maison" | "appartement";
  maison: MaisonForCee;
}) {
  const postes = getPostesEligibles({
    typeBatiment,
    classeDpe: maison.classe,
    anneeConstruction: maison.annee_construction,
    isolationMurs: maison.isolation_murs,
    isolationToiture: maison.isolation_toiture,
    isolationPlancherBas: maison.isolation_plancher_bas,
    isolationMenuiseries: maison.isolation_menuiseries,
    energiePrincipaleChauffage: maison.energie_principale_chauffage,
    generateurChauffage: maison.generateur_chauffage,
    generateurEcs: maison.generateur_ecs,
    typeVentilation: maison.type_ventilation,
  });

  if (postes.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
        <div className="mb-1 flex items-center gap-1 font-semibold">
          <Info className="h-3.5 w-3.5" />
          Aucun poste CEE évident depuis ce DPE
        </div>
        <p>
          Le DPE n'indique pas de défauts d'isolation, de chauffage fossile,
          ni de classe énergétique problématique. Un audit ciblé reste utile
          pour confirmer.
        </p>
      </div>
    );
  }

  // Regrouper par famille pour une meilleure lisibilité
  const byFamille: Record<PosteFamille, Poste[]> = {
    Isolation: [],
    Chauffage: [],
    "Eau chaude": [],
    Ventilation: [],
    Bouquet: [],
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
          {postes.length} poste{postes.length > 1 ? "s" : ""} de travaux CEE détecté{postes.length > 1 ? "s" : ""}
        </div>
        <p className="text-[11px]">
          Repérage automatique depuis le DPE ADEME. Le chiffrage précis nécessite
          un audit énergétique — pas d'estimation €&nbsp;ici, juste les pistes
          finançables CEE pour ce {typeBatiment === "appartement" ? "logement" : "logement"}.
        </p>
      </div>

      {familleOrder.map((famille) => {
        const items = byFamille[famille];
        if (items.length === 0) return null;
        return (
          <FamilleBloc key={famille} famille={famille} postes={items} />
        );
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
  const icon = familleIcon(famille);
  return (
    <div>
      <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        {icon}
        {famille}
      </div>
      <div className="space-y-1.5">
        {postes.map((p) => (
          <PosteRow key={p.code} poste={p} />
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
    case "Eau chaude":
      return <Zap className="h-3 w-3 text-cyan-600" />;
    case "Ventilation":
      return <Wind className="h-3 w-3 text-sky-600" />;
    case "Bouquet":
      return <Layers className="h-3 w-3 text-emerald-700" />;
  }
}

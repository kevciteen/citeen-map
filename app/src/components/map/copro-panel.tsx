"use client";
import { useEffect, useState } from "react";
import { Building2, X, Plus, RefreshCw, ExternalLink, Loader2, Users, Calendar } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";

type CoproDetail = {
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
  lat: number | null;
  lon: number | null;
  classe_finale?: string | null;
  classe_reelle?: string | null;
  classe_simulee?: string | null;
  conso_moyenne?: number | null;
  nb_dpe_individuels?: number | null;
  rayon_recherche?: number | null;
};

type Estimation = {
  rayonM: number;
  totalDpeFound: number;
  totalDpeMatched?: number;
  matchMode?: "address" | "no_match";
  matchReasons?: Record<string, number>;
  quality?: {
    level: "verified" | "approximate" | "uncertain" | "no_data";
    label: string;
    reason: string;
  };
  parcelle?: {
    idu: string;
    section: string;
    numero: string;
    surfaceM2: number;
  } | null;
  collectifReel: { classe: string; conso: number | null; date: string | null } | null;
  immeubleSimule: { classe: string; consoMoyenne: number; nbDpeUtilises: number } | null;
  immeubleFinal: {
    statut: "reel" | "simule" | "aucun";
    classe: string;
    conso: number | null;
    confiance: { score: number; label: string };
  };
};

const QUALITY_STYLE: Record<
  string,
  { bg: string; border: string; text: string; label: string }
> = {
  verified: { bg: "#dcfce7", border: "#22c55e", text: "#14532d", label: "VÉRIFIÉ" },
  approximate: { bg: "#fef3c7", border: "#f59e0b", text: "#78350f", label: "APPROXIMATIF" },
  uncertain: { bg: "#fef2f2", border: "#f87171", text: "#7f1d1d", label: "INCERTAIN" },
  no_data: { bg: "#f1f5f9", border: "#94a3b8", text: "#1e293b", label: "AUCUNE DONNÉE" },
};

export function CoproPanel({
  coproId,
  onClose,
  onAddedToPipeline,
}: {
  coproId: number;
  onClose: () => void;
  onAddedToPipeline?: (prospectId: number) => void;
}) {
  const [copro, setCopro] = useState<CoproDetail | null>(null);
  const [prospect, setProspect] = useState<{ id: number; stage: string } | null>(null);
  const [est, setEst] = useState<Estimation | null>(null);
  const [loadingDpe, setLoadingDpe] = useState(false);
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    setCopro(null);
    setEst(null);
    setProspect(null);
    let aborted = false;
    (async () => {
      const r = await fetch(`/api/copros/${coproId}`);
      if (!r.ok) return;
      const j = await r.json();
      if (aborted) return;
      setCopro(j.copro);
      setProspect(j.prospect);

      // Re-run strict estimation each open (ADEME calls are cached 10 min in memory)
      setLoadingDpe(true);
      try {
        const rd = await fetch(`/api/copros/${coproId}/dpe`);
        if (rd.ok) {
          const jd = await rd.json();
          if (!aborted) setEst(jd);
        }
      } finally {
        if (!aborted) setLoadingDpe(false);
      }
    })();
    return () => {
      aborted = true;
    };
  }, [coproId]);

  const refreshDpe = async () => {
    setLoadingDpe(true);
    try {
      const r = await fetch(`/api/copros/${coproId}/dpe`);
      if (r.ok) {
        const j = await r.json();
        setEst(j);
        toast.success("DPE recalculé");
      } else toast.error("Erreur lors du recalcul DPE");
    } finally {
      setLoadingDpe(false);
    }
  };

  const addToPipeline = async () => {
    setAdding(true);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ coproId, stage: "to_contact", priority: 2 }),
      });
      if (r.status === 409) {
        const j = await r.json();
        toast.info("Déjà dans le pipeline");
        setProspect({ id: j.prospectId, stage: "to_contact" });
        onAddedToPipeline?.(j.prospectId);
      } else if (r.ok) {
        const j = await r.json();
        toast.success("Ajouté au pipeline");
        setProspect({ id: j.id, stage: "to_contact" });
        onAddedToPipeline?.(j.id);
      } else {
        toast.error("Erreur");
      }
    } finally {
      setAdding(false);
    }
  };

  if (!copro) {
    return (
      <div className="absolute right-4 top-4 z-10 flex h-[200px] w-[420px] items-center justify-center">
        <div className="premium-card flex h-full w-full items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-primary" />
        </div>
      </div>
    );
  }

  return (
    <div className="absolute right-4 top-4 z-10 max-h-[calc(100vh-2rem)] w-[420px] max-w-[calc(100vw-2rem)] overflow-y-auto">
      <div className="premium-card">
        <div className="flex items-start justify-between gap-2 border-b border-border p-4">
          <div className="flex items-start gap-3">
            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
              <Building2 className="h-5 w-5" />
            </div>
            <div className="flex-1">
              <h2 className="text-sm font-bold leading-tight">
                {copro.nom_copro || copro.adresse || "Copropriété"}
              </h2>
              <p className="text-xs text-muted-foreground">
                {copro.adresse}
                {copro.adresse ? " · " : ""}
                {copro.code_postal} {copro.commune}
              </p>
              <div className="mt-1 flex items-center gap-1.5">
                <Badge variant="outline" className="font-mono text-[10px]">
                  {copro.numero_immatriculation}
                </Badge>
                {copro.departement ? (
                  <Badge variant="secondary" className="text-[10px]">
                    Dpt {copro.departement}
                  </Badge>
                ) : null}
              </div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-muted-foreground hover:bg-secondary"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="grid grid-cols-2 gap-3 p-4">
          <div>
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Lots habitation</p>
            <p className="flex items-center gap-1 text-sm font-semibold">
              <Users className="h-3.5 w-3.5" />
              {copro.nb_lots_habitation ?? copro.nb_lots ?? "—"}
            </p>
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Construction</p>
            <p className="flex items-center gap-1 text-sm font-semibold">
              <Calendar className="h-3.5 w-3.5" />
              {(copro.periode_construction || "—").replace(/_/g, " ")}
            </p>
          </div>
          <div className="col-span-2">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Syndic</p>
            <p className="truncate text-sm font-medium">{copro.syndic || "—"}</p>
          </div>
        </div>

        <Separator />

        <div className="p-4">
          <div className="mb-2 flex items-center justify-between">
            <h3 className="text-sm font-bold">DPE immeuble</h3>
            <button
              onClick={refreshDpe}
              disabled={loadingDpe}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-50"
            >
              {loadingDpe ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <RefreshCw className="h-3 w-3" />
              )}
              Recalculer
            </button>
          </div>

          {loadingDpe && !est ? (
            <div className="flex h-20 items-center justify-center">
              <Loader2 className="h-5 w-5 animate-spin text-primary" />
            </div>
          ) : est ? (
            <div className="space-y-3">
              {est.quality ? (
                <div
                  className="rounded-lg border-l-4 px-2.5 py-1.5"
                  style={{
                    background: QUALITY_STYLE[est.quality.level].bg,
                    borderLeftColor: QUALITY_STYLE[est.quality.level].border,
                    color: QUALITY_STYLE[est.quality.level].text,
                  }}
                >
                  <span
                    className="rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider"
                    style={{
                      background: QUALITY_STYLE[est.quality.level].border,
                      color: "white",
                    }}
                  >
                    {QUALITY_STYLE[est.quality.level].label}
                  </span>
                  <span className="ml-2 text-[11px] font-semibold">
                    {est.quality.label}
                  </span>
                </div>
              ) : null}

              <div className="flex items-center justify-between rounded-lg bg-secondary/50 p-3">
                <div>
                  <p className="text-xs uppercase tracking-wider text-muted-foreground">
                    {est.immeubleFinal.statut === "reel"
                      ? "DPE collectif réel"
                      : est.immeubleFinal.statut === "simule"
                        ? "Estimation immeuble"
                        : "Aucune donnée"}
                  </p>
                  <p className="mt-0.5 text-xs font-medium text-foreground/80">
                    {est.immeubleFinal.confiance.label}
                  </p>
                </div>
                <DpeBadge classe={est.immeubleFinal.classe} size="lg" />
              </div>

              <DpeScaleBar active={est.immeubleFinal.classe} />

              <div className="grid grid-cols-2 gap-2 text-xs">
                <div className="rounded-md bg-background/40 p-2">
                  <p className="text-muted-foreground">Conso (kWhep/m²/an)</p>
                  <p className="text-base font-bold">{est.immeubleFinal.conso ?? "—"}</p>
                </div>
                <div className="rounded-md bg-background/40 p-2">
                  <p className="text-muted-foreground">DPE retenus immeuble</p>
                  <p className="text-base font-bold">
                    {est.totalDpeMatched ?? 0}
                    <span className="ml-1 text-[10px] font-normal text-muted-foreground">
                      / {est.totalDpeFound} candidats
                    </span>
                  </p>
                </div>
              </div>

              <div className="space-y-1 text-[11px] text-muted-foreground">
                {est.immeubleSimule ? (
                  <p>
                    Méthode : moyenne {est.immeubleSimule.nbDpeUtilises} DPE individuels récents
                  </p>
                ) : null}
                <p>
                  Calibrage :{" "}
                  {est.matchMode === "address" ? (
                    <span className="font-semibold text-emerald-700">
                      Adresse stricte + parcelle cadastrale
                    </span>
                  ) : (
                    <span className="font-semibold text-red-700">
                      Aucun DPE strictement attribué
                    </span>
                  )}
                </p>
                {est.parcelle ? (
                  <p className="font-mono text-[10px]">
                    Parcelle IGN : {est.parcelle.idu} (
                    {est.parcelle.surfaceM2.toLocaleString("fr-FR")} m²)
                  </p>
                ) : null}
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Pas encore calculé.</p>
          )}
        </div>

        <Separator />

        <div className="space-y-2 p-4">
          <a
            href={`/copros/${coproId}`}
            className="flex w-full items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2 text-sm font-semibold text-primary-foreground shadow-sm hover:bg-primary/90"
          >
            <ExternalLink className="h-4 w-4" />
            Ouvrir la fiche complète
          </a>
          {prospect ? (
            <a
              href={`/prospects/${prospect.id}`}
              className="flex w-full items-center justify-center gap-2 rounded-lg border border-primary bg-primary/5 px-3 py-2 text-sm font-semibold text-primary hover:bg-primary/10"
            >
              <ExternalLink className="h-4 w-4" />
              Voir le prospect ({prospect.stage})
            </a>
          ) : (
            <Button
              onClick={addToPipeline}
              disabled={adding}
              variant="outline"
              className="w-full"
            >
              {adding ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Plus className="h-4 w-4" />
              )}
              Ajouter au pipeline
            </Button>
          )}
          {copro.lat && copro.lon ? (
            <a
              href={`https://www.google.com/maps/search/?api=1&query=${copro.lat},${copro.lon}`}
              target="_blank"
              rel="noreferrer"
              className="flex w-full items-center justify-center gap-2 rounded-lg border border-border px-3 py-2 text-xs text-muted-foreground hover:bg-secondary"
            >
              <ExternalLink className="h-3 w-3" />
              Voir sur Google Maps
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}

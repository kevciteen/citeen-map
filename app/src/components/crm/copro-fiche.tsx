"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  Building2,
  Calendar,
  Download,
  Edit3,
  ExternalLink,
  FileText,
  Loader2,
  MapPin,
  Plus,
  Printer,
  RefreshCw,
  Users,
  Zap,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { EditCoproSheet } from "@/components/crm/edit-copro-sheet";
import { DpeIndividualsCard } from "@/components/crm/dpe-individuals-card";
import { SimilarsCard } from "@/components/crm/similars-card";
import { CoproStreetView } from "@/components/crm/copro-streetview";
import { CeeCoproPostes } from "@/components/crm/cee-copro-postes";
import { CoproSyndicContacts } from "@/components/crm/copro-syndic-contacts";
import { toast } from "sonner";

const DPE_GRADIENT: Record<string, string> = {
  A: "linear-gradient(135deg, #1f9d55 0%, #15803d 100%)",
  B: "linear-gradient(135deg, #7cb342 0%, #4d7c0f 100%)",
  C: "linear-gradient(135deg, #cddc39 0%, #84cc16 100%)",
  D: "linear-gradient(135deg, #ffeb3b 0%, #eab308 100%)",
  E: "linear-gradient(135deg, #ffb300 0%, #d97706 100%)",
  F: "linear-gradient(135deg, #fb8c00 0%, #ea580c 100%)",
  G: "linear-gradient(135deg, #e53935 0%, #b91c1c 100%)",
  NC: "linear-gradient(135deg, #94a3b8 0%, #475569 100%)",
};

type CoproRow = {
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
  code_insee_commune: string | null;
  section: string | null;
  numero_parcelle: string | null;
  reference_cadastrale: string | null;
  classe_finale: string | null;
  classe_reelle: string | null;
  classe_simulee: string | null;
  conso_moyenne: number | null;
  nb_dpe_individuels: number | null;
  rayon_recherche: number | null;
};

type DpeIndividuel = {
  numero_dpe: string | null;
  date: string | null;
  classe: string;
  ges: string;
  conso: number | null;
  surface: number | null;
  adresse: string | null;
  numero_voie: string | null;
  rue: string | null;
  code_postal: string | null;
  commune: string | null;
  type_dpe: string | null;
  annee_construction: number | null;
  type_batiment: string | null;
  isCollectif: boolean;
};

type DpeDetails = {
  rayonM: number;
  totalDpeFound: number;
  totalDpeMatched: number;
  matchMode: "address" | "no_match";
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
    centroidLat: number;
    centroidLon: number;
    method: "ref" | "point";
  } | null;
  ctxResolved?: {
    address: string;
    codePostal: string;
    commune: string;
    houseNumber: string | null;
    streetTokens: string[];
  };
  collectifReel: {
    classe: string;
    conso: number | null;
    ges: string;
    date: string | null;
    numero: string | null;
  } | null;
  immeubleSimule: {
    classe: string;
    consoMoyenne: number;
    nbDpeUtilises: number;
    methode: string;
  } | null;
  immeubleFinal: {
    statut: "reel" | "simule" | "aucun";
    classe: string;
    conso: number | null;
    confiance: { score: number; label: string };
  };
  banResolved?: {
    label: string;
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    score: number;
    corrected: boolean;
  } | null;
  matchedRecords?: DpeIndividuel[];
};

const QUALITY_STYLE: Record<
  string,
  { bg: string; border: string; text: string; label: string }
> = {
  verified: {
    bg: "#dcfce7",
    border: "#22c55e",
    text: "#14532d",
    label: "VÉRIFIÉ",
  },
  approximate: {
    bg: "#fef3c7",
    border: "#f59e0b",
    text: "#78350f",
    label: "APPROXIMATIF",
  },
  uncertain: {
    bg: "#fef2f2",
    border: "#f87171",
    text: "#7f1d1d",
    label: "INCERTAIN",
  },
  no_data: {
    bg: "#f1f5f9",
    border: "#94a3b8",
    text: "#1e293b",
    label: "AUCUNE DONNÉE",
  },
};

const REASON_LABELS: Record<string, string> = {
  cp_mismatch: "Code postal différent",
  commune_mismatch: "Commune différente",
  voie_type_mismatch: "Type de voie différent (rue/avenue/bd…)",
  street_mismatch: "Nom de rue différent",
  number_mismatch: "Numéro de rue différent",
  no_cp_in_record: "DPE sans code postal",
  no_commune_in_record: "DPE sans commune",
  no_voie_type_in_record: "DPE sans type de voie",
  no_street_in_record: "DPE sans rue",
  no_number_in_record: "DPE sans numéro de rue",
};

const PERIOD_LABELS: Record<string, string> = {
  AVANT_1949: "Avant 1949",
  DE_1949_A_1974: "1949 — 1974",
  DE_1975_A_1993: "1975 — 1993",
  DE_1994_A_2000: "1994 — 2000",
  DE_2001_A_2010: "2001 — 2010",
  APRES_2011: "Après 2011",
  NON_CONNUE: "Non connue",
};

export function CoproFiche({
  copro,
  prospect: prospectInit,
}: {
  copro: CoproRow;
  prospect: { id: number; stage: string } | null;
}) {
  const [details, setDetails] = useState<DpeDetails | null>(null);
  const [loading, setLoading] = useState(false);
  const [prospect, setProspect] = useState(prospectInit);
  const [ceeOpen, setCeeOpen] = useState(false);

  const loadDpe = async (refresh = false) => {
    setLoading(true);
    try {
      const r = await fetch(
        `/api/copros/${copro.id}/dpe/details${refresh ? "?refresh=1" : ""}`,
      );
      if (r.ok) setDetails(await r.json());
      else toast.error("Erreur ADEME");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDpe(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [copro.id]);

  const addToPipeline = async () => {
    const r = await fetch("/api/prospects", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ coproId: copro.id, stage: "to_contact" }),
    });
    if (r.ok) {
      const j = await r.json();
      toast.success("Ajouté au pipeline");
      setProspect({ id: j.id, stage: "to_contact" });
    } else if (r.status === 409) {
      const j = await r.json();
      setProspect({ id: j.prospectId, stage: "to_contact" });
      toast.info("Déjà dans le pipeline");
    } else toast.error("Erreur");
  };

  const finalClasse = details?.immeubleFinal.classe ?? copro.classe_finale ?? "NC";
  const periodeLabel =
    PERIOD_LABELS[copro.periode_construction ?? ""] ??
    (copro.periode_construction ?? "—").replace(/_/g, " ");

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-6 print:max-w-full print:p-0">
      {/* HEADER avec dégradé DPE */}
      <div className="grid gap-3 lg:grid-cols-[1fr_320px]">
        <Card
          className="overflow-hidden border-0 text-white shadow-lg print:shadow-none"
          style={{ background: DPE_GRADIENT[finalClasse] ?? DPE_GRADIENT.NC }}
        >
          <CardContent className="flex flex-col gap-3 p-6 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-start gap-4">
              <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-xl bg-white/20 backdrop-blur">
                <Building2 className="h-7 w-7" />
              </div>
              <div>
                <h1 className="text-2xl font-black tracking-tight drop-shadow-sm">
                  {copro.nom_copro || copro.adresse || `Copro #${copro.id}`}
                </h1>
                <p className="mt-1 flex items-center gap-1 text-sm opacity-90">
                  <MapPin className="h-3.5 w-3.5" />
                  {copro.adresse} · {copro.code_postal} {copro.commune}
                </p>
                <div className="mt-2 flex flex-wrap items-center gap-1.5">
                  <span className="rounded-full bg-white/20 px-2 py-0.5 font-mono text-[10px] font-semibold backdrop-blur">
                    {copro.numero_immatriculation}
                  </span>
                  {copro.departement ? (
                    <span className="rounded-full bg-white/20 px-2 py-0.5 text-[10px] font-semibold backdrop-blur">
                      Département {copro.departement}
                    </span>
                  ) : null}
                  {copro.nb_lots_habitation ? (
                    <span className="rounded-full bg-white/20 px-2 py-0.5 text-[10px] font-semibold backdrop-blur">
                      {copro.nb_lots_habitation} lots habitation
                    </span>
                  ) : null}
                  {(copro.periode_construction ?? "").length > 3 ? (
                    <span className="rounded-full bg-white/20 px-2 py-0.5 text-[10px] font-semibold backdrop-blur">
                      {periodeLabel}
                    </span>
                  ) : null}
                </div>
              </div>
            </div>
            <div
              className="flex h-24 w-24 shrink-0 items-center justify-center rounded-2xl bg-white text-4xl font-black shadow-lg"
              style={{ color: DPE_GRADIENT[finalClasse]?.match(/#[0-9a-f]+/i)?.[0] ?? "#475569" }}
            >
              {finalClasse === "NC" || !finalClasse ? "—" : finalClasse}
            </div>
          </CardContent>
        </Card>

        <Card className="border-primary/20 bg-gradient-to-br from-primary/5 to-secondary/30 print:hidden">
          <CardHeader>
            <CardTitle className="text-sm">Actions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {prospect ? (
              <Link
                href={`/prospects/${prospect.id}`}
                className="flex w-full items-center justify-center gap-2 rounded-lg border border-primary bg-primary/5 px-3 py-2 text-sm font-semibold text-primary hover:bg-primary/10"
              >
                <ExternalLink className="h-4 w-4" />
                Fiche prospect ({prospect.stage})
              </Link>
            ) : (
              <Button onClick={addToPipeline} className="w-full">
                <Plus className="h-4 w-4" /> Ajouter au pipeline
              </Button>
            )}
            <EditCoproSheet
              copro={{
                id: copro.id,
                nom_copro: copro.nom_copro,
                adresse: copro.adresse,
                code_postal: copro.code_postal,
                commune: copro.commune,
                syndic: copro.syndic,
                lat: copro.lat,
                lon: copro.lon,
              }}
              trigger={
                <button className="flex w-full items-center justify-center gap-2 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-xs font-semibold text-amber-900 hover:bg-amber-100">
                  <Edit3 className="h-3.5 w-3.5" /> Corriger l'adresse / position
                </button>
              }
            />
            <a
              href={`/api/copros/${copro.id}/dpe/individuals.xlsx`}
              className="flex w-full items-center justify-center gap-2 rounded-lg border border-emerald-300 bg-emerald-50 px-3 py-2 text-xs font-semibold text-emerald-900 hover:bg-emerald-100"
            >
              <FileText className="h-3.5 w-3.5" /> Exporter Excel (fiche complète)
            </a>
            <a
              href={`/api/export/copros.xlsx?ids=${copro.id}`}
              className="flex w-full items-center justify-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-xs font-medium hover:bg-secondary"
            >
              <Download className="h-3.5 w-3.5" /> Exporter synthèse Excel
            </a>
            <button
              onClick={() => window.print()}
              className="flex w-full items-center justify-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-xs font-medium hover:bg-secondary"
            >
              <Printer className="h-3.5 w-3.5" /> Imprimer / PDF
            </button>
            {copro.lat && copro.lon ? (
              <a
                href={`https://www.google.com/maps/search/?api=1&query=${copro.lat},${copro.lon}`}
                target="_blank"
                rel="noreferrer"
                className="flex w-full items-center justify-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-xs text-muted-foreground hover:bg-secondary"
              >
                <ExternalLink className="h-3 w-3" /> Voir sur Google Maps
              </a>
            ) : null}
          </CardContent>
        </Card>
      </div>

      {/* STREET VIEW */}
      <CoproStreetView
        lat={copro.lat}
        lon={copro.lon}
        label={copro.nom_copro || copro.adresse || `Copro #${copro.id}`}
      />

      {/* DPE BIG SECTION */}
      <Card className="print:shadow-none">
        <CardHeader className="flex flex-row items-center justify-between">
          <div className="flex items-center gap-2">
            <Zap className="h-4 w-4 text-primary" />
            <CardTitle className="text-sm">Diagnostic Performance Énergétique — Immeuble</CardTitle>
          </div>
          <button
            onClick={() => loadDpe(true)}
            disabled={loading}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-50 print:hidden"
          >
            {loading ? (
              <Loader2 className="h-3 w-3 animate-spin" />
            ) : (
              <RefreshCw className="h-3 w-3" />
            )}
            Recalculer depuis l'ADEME
          </button>
        </CardHeader>
        <CardContent>
          {loading && !details ? (
            <div className="flex h-32 items-center justify-center">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
            </div>
          ) : details ? (
            <>
              {details.quality ? (
                <div
                  className="mb-3 rounded-lg border-l-4 p-3"
                  style={{
                    background: QUALITY_STYLE[details.quality.level].bg,
                    borderLeftColor: QUALITY_STYLE[details.quality.level].border,
                    color: QUALITY_STYLE[details.quality.level].text,
                  }}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="flex items-center gap-2 text-xs font-bold uppercase tracking-wider">
                        Qualité prospection :{" "}
                        <span
                          className="rounded px-1.5 py-0.5 text-[10px]"
                          style={{
                            background: QUALITY_STYLE[details.quality.level].border,
                            color: "white",
                          }}
                        >
                          {QUALITY_STYLE[details.quality.level].label}
                        </span>
                      </p>
                      <p className="mt-0.5 text-xs font-semibold">
                        {details.quality.label}
                      </p>
                      <p className="mt-1 text-[11px] opacity-90">
                        {details.quality.reason}
                      </p>
                    </div>
                    {details.parcelle ? (
                      <div className="text-right text-[10px] opacity-80">
                        <p className="font-bold">Parcelle cadastrale IGN</p>
                        <p className="font-mono">{details.parcelle.idu}</p>
                        <p>
                          Section {details.parcelle.section} · n°
                          {details.parcelle.numero} · {details.parcelle.surfaceM2.toLocaleString("fr-FR")} m²
                        </p>
                        <p className="italic">
                          via{" "}
                          {details.parcelle.method === "ref"
                            ? "référence registre"
                            : "point-in-polygon"}
                        </p>
                      </div>
                    ) : null}
                  </div>
                </div>
              ) : null}
            <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
              <div className="flex flex-col items-center justify-center rounded-xl bg-secondary/40 p-6 text-center">
                <DpeBadge classe={details.immeubleFinal.classe} size="lg" className="!h-16 !min-w-[64px] !text-3xl" />
                <p className="mt-3 text-xs uppercase tracking-wider text-muted-foreground">
                  {details.immeubleFinal.statut === "reel"
                    ? "DPE collectif réel"
                    : details.immeubleFinal.statut === "simule"
                      ? "Estimation immeuble"
                      : "Aucune donnée"}
                </p>
                <p className="mt-1 text-[11px] font-semibold text-foreground/80">
                  {details.immeubleFinal.confiance.label}
                </p>
                <div className="mt-2 flex items-center gap-1 text-[10px] text-muted-foreground">
                  <div
                    className="h-1.5 w-20 overflow-hidden rounded-full bg-secondary"
                    aria-label="Score de confiance"
                  >
                    <div
                      className="h-full bg-primary"
                      style={{ width: `${details.immeubleFinal.confiance.score}%` }}
                    />
                  </div>
                  <span>{details.immeubleFinal.confiance.score}%</span>
                </div>
              </div>

              <div className="space-y-3">
                <DpeScaleBar active={details.immeubleFinal.classe} />

                <div className="grid grid-cols-2 gap-2 lg:grid-cols-3">
                  <Kpi
                    label="Consommation"
                    value={
                      details.immeubleFinal.conso != null
                        ? `${details.immeubleFinal.conso} kWhep/m²/an`
                        : "—"
                    }
                  />
                  <Kpi
                    label="GES collectif"
                    value={details.collectifReel?.ges ?? "—"}
                  />
                  <Kpi
                    label="DPE retenus"
                    value={`${details.totalDpeMatched} / ${details.totalDpeFound}`}
                  />
                  <Kpi label="Rayon recherche" value={`${details.rayonM} m`} />
                  <Kpi
                    label="Calibrage"
                    value={
                      details.matchMode === "address"
                        ? "Adresse stricte"
                        : "Aucun match"
                    }
                    tone={details.matchMode === "address" ? "success" : "danger"}
                  />
                  <Kpi
                    label="Lots utilisés pour simulation"
                    value={String(details.immeubleSimule?.nbDpeUtilises ?? "—")}
                  />
                </div>

                {details.banResolved && details.banResolved.corrected ? (
                  <div className="rounded-lg border border-blue-200 bg-blue-50 p-3">
                    <p className="text-xs font-bold uppercase tracking-wider text-blue-800">
                      Adresse corrigée par la BAN (Base Adresse Nationale)
                    </p>
                    <p className="mt-1 text-xs text-blue-900">
                      Le registre des copros indiquait{" "}
                      <em>"{copro.adresse}"</em> mais la BAN a résolu la
                      géolocalisation en :{" "}
                      <strong>{details.banResolved.label}</strong>
                      {" "}(confiance {Math.round(details.banResolved.score * 100)}%)
                    </p>
                    <p className="mt-0.5 text-[11px] text-blue-700">
                      Le matching DPE utilise donc cette adresse canonique
                      (type de voie, numéro, CP, commune) au lieu de la chaîne
                      brute du registre — c'est ce qui évite de matcher les DPE
                      d'une autre rue voisine.
                    </p>
                  </div>
                ) : null}

                {details.matchMode === "no_match" ? (
                  <div className="rounded-lg border border-red-200 bg-red-50 p-3">
                    <p className="text-xs font-bold uppercase tracking-wider text-red-800">
                      Aucun DPE strictement attribué à cette adresse
                    </p>
                    <p className="mt-1 text-xs text-red-900">
                      Le filtre exige une correspondance exacte sur{" "}
                      <strong>numéro + rue + code postal + commune</strong>. Sur les{" "}
                      {details.totalDpeFound} DPE trouvés dans un rayon de {details.rayonM}m, aucun
                      ne matche cette copro de manière certaine.
                    </p>
                    {details.matchReasons &&
                    Object.keys(details.matchReasons).length > 0 ? (
                      <ul className="mt-2 space-y-0.5 text-[11px] text-red-700">
                        {Object.entries(details.matchReasons)
                          .sort((a, b) => b[1] - a[1])
                          .map(([k, v]) => (
                            <li key={k}>
                              · {REASON_LABELS[k] ?? k} : <strong>{v}</strong> rejet(s)
                            </li>
                          ))}
                      </ul>
                    ) : null}
                  </div>
                ) : null}

                {details.collectifReel ? (
                  <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-3">
                    <p className="text-xs font-bold uppercase tracking-wider text-emerald-800">
                      DPE collectif réel
                    </p>
                    <p className="mt-1 text-xs text-emerald-900">
                      Classe <strong>{details.collectifReel.classe}</strong> · Conso{" "}
                      {details.collectifReel.conso ?? "—"} kWhep/m²/an · GES{" "}
                      {details.collectifReel.ges} · Établi{" "}
                      {details.collectifReel.date ?? "—"}
                    </p>
                    {details.collectifReel.numero ? (
                      <p className="mt-0.5 font-mono text-[10px] text-emerald-700">
                        N° DPE : {details.collectifReel.numero}
                      </p>
                    ) : null}
                  </div>
                ) : details.immeubleSimule ? (
                  <div className="rounded-lg border border-amber-200 bg-amber-50 p-3">
                    <p className="text-xs font-bold uppercase tracking-wider text-amber-800">
                      Estimation immeuble (méthode citeen / Go Renov)
                    </p>
                    <p className="mt-1 text-xs text-amber-900">
                      Moyenne pondérée des {details.immeubleSimule.nbDpeUtilises} DPE
                      individuels les plus récents → conso moyenne{" "}
                      <strong>{details.immeubleSimule.consoMoyenne} kWhep/m²/an</strong>{" "}
                      → classe <strong>{details.immeubleSimule.classe}</strong>
                    </p>
                  </div>
                ) : null}
              </div>
            </div>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">DPE non chargé.</p>
          )}
        </CardContent>
      </Card>

      {/* TRAVAUX CEE COLLECTIFS ÉLIGIBLES (collapsé par défaut) */}
      <Card className="print:shadow-none">
        <button
          type="button"
          onClick={() => setCeeOpen(!ceeOpen)}
          className="flex w-full items-center justify-between p-4 hover:bg-secondary/30"
        >
          <div className="flex items-center gap-2">
            <Zap className="h-4 w-4 text-emerald-700" />
            <CardTitle className="text-sm">Travaux CEE collectifs éligibles</CardTitle>
            <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-800">
              {ceeOpen ? "Replier" : "Voir l'estimation"}
            </span>
          </div>
          <span className="text-xs text-muted-foreground">{ceeOpen ? "▲" : "▼"}</span>
        </button>
        {ceeOpen ? (
          <CardContent className="pt-0">
            <CeeCoproPostes
              classeDpeCollective={
                details?.immeubleFinal.classe ?? copro.classe_finale ?? null
              }
              periodeConstruction={copro.periode_construction ?? null}
              nbLotsHabitation={copro.nb_lots_habitation ?? null}
              codePostal={copro.code_postal ?? null}
              matchedIndividuals={details?.matchedRecords ?? undefined}
            />
          </CardContent>
        ) : null}
      </Card>

      {/* IDENTITE / LOCALISATION */}
      <div className="grid gap-3 lg:grid-cols-2">
        <Card className="print:shadow-none">
          <CardHeader>
            <CardTitle className="text-sm">Identité copropriété</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1.5 text-xs">
            <Row label="Numéro d'immatriculation" value={copro.numero_immatriculation} mono />
            <Row label="Nom d'usage" value={copro.nom_copro ?? "—"} />
            <Row label="Syndic" value={copro.syndic ?? "—"} />
            <Row
              label="Lots totaux"
              value={copro.nb_lots != null ? String(copro.nb_lots) : "—"}
            />
            <Row
              label="Lots habitation"
              value={
                copro.nb_lots_habitation != null
                  ? String(copro.nb_lots_habitation)
                  : "—"
              }
            />
            <Row label="Période construction" value={periodeLabel} />
          </CardContent>
        </Card>

        <Card className="print:shadow-none">
          <CardHeader>
            <CardTitle className="text-sm">Localisation & cadastre</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1.5 text-xs">
            <Row label="Adresse" value={copro.adresse ?? "—"} />
            <Row label="Code postal" value={copro.code_postal ?? "—"} />
            <Row label="Commune" value={copro.commune ?? "—"} />
            <Row label="Département" value={copro.departement ?? "—"} />
            <Row
              label="Coordonnées (lat, lon)"
              value={
                copro.lat && copro.lon
                  ? `${copro.lat.toFixed(6)}, ${copro.lon.toFixed(6)}`
                  : "—"
              }
              mono
            />
            <Row
              label="Cadastre"
              value={copro.reference_cadastrale ?? "—"}
              mono
            />
            <Row
              label="Code INSEE commune"
              value={copro.code_insee_commune ?? "—"}
              mono
            />
            <Row label="Section / Parcelle" value={
              copro.section || copro.numero_parcelle
                ? `${copro.section ?? ""} ${copro.numero_parcelle ?? ""}`.trim()
                : "—"
            } mono />
          </CardContent>
        </Card>
      </div>

      {/* CONTACTS SYNDIC (Sirene + auto OSM/Google + manuel) */}
      <CoproSyndicContacts syndicName={copro.syndic ?? null} />

      {/* DPE INDIVIDUELS TABLE */}
      <DpeIndividualsCard details={details} />

      {/* SIMILAIRES (suggestion prospection) */}
      <SimilarsCard coproId={copro.id} />
    </div>
  );
}

function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="flex items-start justify-between gap-3 border-b border-border/40 py-1.5 last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span
        className={
          (mono ? "font-mono " : "") + "max-w-[60%] text-right font-medium"
        }
      >
        {value}
      </span>
    </div>
  );
}

function Kpi({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "success" | "warning" | "danger";
}) {
  const style =
    tone === "success"
      ? { borderColor: "#bbf7d0", background: "#f0fdf4" }
      : tone === "warning"
        ? { borderColor: "#fde68a", background: "#fffbeb" }
        : tone === "danger"
          ? { borderColor: "#fecaca", background: "#fef2f2" }
          : undefined;
  return (
    <div
      className="rounded-lg border border-border bg-card p-2.5"
      style={style}
    >
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-0.5 text-sm font-bold">{value}</p>
    </div>
  );
}

void Users;

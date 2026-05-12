"use client";
import { useEffect, useRef, useState } from "react";
import { Loader2, MapPin, Home, Calendar, Ruler, Zap, ExternalLink, Plus, AlertCircle, Eye } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { MaisonDetailSheet } from "@/components/crm/maison-detail-sheet";

type MaisonDpe = {
  numero_dpe: string;
  classe: string;
  ges: string;
  conso: number | null;
  emission_ges: number | null;
  surface: number | null;
  date: string | null;
  date_visite: string | null;
  date_fin_validite: string | null;
  annee_construction: number | null;
  nb_niveaux: number | null;
  nb_pieces: number | null;
  hauteur_sous_plafond: number | null;
  type_batiment: string | null;
  methode_dpe: string | null;
  energie_principale_chauffage: string | null;
  energie_n2: string | null;
  energie_n3: string | null;
  installation_chauffage: string | null;
  generateur_chauffage: string | null;
  generateur_chauffage_desc: string | null;
  emetteur_chauffage: string | null;
  generateur_ecs: string | null;
  description_ecs: string | null;
  volume_ballon_ecs: number | null;
  energie_climatisation: string | null;
  surface_climatisee: number | null;
  type_ventilation: string | null;
  ventilation_recente: boolean | null;
  zone_climatique: string | null;
  deperdition_murs: number | null;
  deperdition_baies: number | null;
  deperdition_plancher_bas: number | null;
  deperdition_plancher_haut: number | null;
  cout_total: number | null;
  cout_chauffage: number | null;
  cout_ecs: number | null;
  cout_eclairage: number | null;
  cout_refroidissement: number | null;
  isolation_enveloppe: string | null;
  isolation_murs: string | null;
  isolation_toiture: string | null;
  isolation_plancher_bas: string | null;
  isolation_menuiseries: string | null;
  address: {
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    label: string;
  };
  lat: number | null;
  lon: number | null;
  ademe_url: string;
};

type LookupResult = {
  banResolved: {
    label: string;
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    lat: number;
    lon: number;
    score: number;
  } | null;
  parcelle: {
    idu: string;
    section: string;
    numero: string;
    contenance_m2: number;
  } | null;
  totalDpeFound: number;
  matched: MaisonDpe[];
  notes: string[];
};

type Suggestion = {
  label: string;
  lat: number;
  lon: number;
  postcode?: string;
  city?: string;
  street?: string;
  housenumber?: string;
};

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

export function MaisonsAddressSearch() {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [loadingSuggest, setLoadingSuggest] = useState(false);
  const [result, setResult] = useState<LookupResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [adding, setAdding] = useState<number | null>(null);
  const tid = useRef<number | null>(null);

  useEffect(() => {
    if (tid.current) window.clearTimeout(tid.current);
    if (query.trim().length < 3) {
      setSuggestions([]);
      return;
    }
    tid.current = window.setTimeout(async () => {
      setLoadingSuggest(true);
      try {
        const r = await fetch(`/api/search/places?q=${encodeURIComponent(query)}`);
        const j = await r.json();
        setSuggestions(j.items ?? []);
        setSuggestOpen(true);
      } finally {
        setLoadingSuggest(false);
      }
    }, 220);
  }, [query]);

  const runLookup = async (q: string) => {
    setLoading(true);
    setResult(null);
    setSuggestOpen(false);
    try {
      const r = await fetch(`/api/maisons/lookup?q=${encodeURIComponent(q)}`);
      const j = await r.json();
      if (r.ok) setResult(j);
      else toast.error(j?.error || "Erreur");
    } finally {
      setLoading(false);
    }
  };

  const addToPipeline = async (m: MaisonDpe, idx: number) => {
    setAdding(idx);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          customLabel: `Maison · ${m.address.label}`,
          customAddress: m.address.label,
          customLat: m.lat ?? result?.banResolved?.lat,
          customLon: m.lon ?? result?.banResolved?.lon,
          stage: "to_contact",
          priority: 2,
          tags: ["maison", `dpe-${m.classe}`],
        }),
      });
      if (r.ok || r.status === 409) toast.success("Ajouté au pipeline");
      else toast.error("Erreur");
    } finally {
      setAdding(null);
    }
  };

  return (
    <div className="mx-auto max-w-4xl space-y-4 p-6">
      {/* Search bar */}
      <Card>
        <CardContent className="p-5">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Rechercher une maison
          </p>
          <div className="relative">
            <MapPin className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && query.trim().length >= 5) {
                  runLookup(query);
                }
              }}
              onFocus={() => suggestions.length && setSuggestOpen(true)}
              onBlur={() => setTimeout(() => setSuggestOpen(false), 180)}
              placeholder="Adresse complète : 12 rue Voltaire 75011 Paris"
              className="pl-9 pr-32"
            />
            {loadingSuggest ? (
              <Loader2 className="absolute right-24 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-muted-foreground" />
            ) : null}
            <Button
              size="sm"
              className="absolute right-1 top-1/2 -translate-y-1/2"
              onClick={() => runLookup(query)}
              disabled={loading || query.trim().length < 5}
            >
              {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : "Chercher"}
            </Button>

            {suggestOpen && suggestions.length > 0 ? (
              <div className="absolute left-0 right-0 top-full z-20 mt-1.5 overflow-hidden rounded-lg border border-border bg-popover shadow-lg">
                {suggestions.map((s, i) => (
                  <button
                    key={i}
                    type="button"
                    onMouseDown={(e) => {
                      e.preventDefault();
                      setQuery(s.label);
                      setSuggestOpen(false);
                      runLookup(s.label);
                    }}
                    className="flex w-full items-start gap-2 px-3 py-2 text-left text-sm hover:bg-secondary"
                  >
                    <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-primary" />
                    <div>
                      <div className="font-medium leading-tight">{s.label}</div>
                      <div className="text-[10px] text-muted-foreground">
                        {s.postcode} {s.city}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            ) : null}
          </div>

          <p className="mt-2 text-[11px] text-muted-foreground">
            On interroge BAN + cadastre IGN + ADEME pour trouver le DPE officiel
            de la maison à cette adresse. Filtrage strict sur type=maison.
          </p>
        </CardContent>
      </Card>

      {/* Loading state */}
      {loading ? (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-primary" />
        </div>
      ) : null}

      {/* Results */}
      {result && !loading ? (
        <>
          {/* BAN + parcelle resolved */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Adresse résolue</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-xs">
              {result.banResolved ? (
                <>
                  <Row label="Adresse canonique BAN" value={result.banResolved.label} />
                  <Row
                    label="Coordonnées GPS"
                    value={`${result.banResolved.lat.toFixed(6)}, ${result.banResolved.lon.toFixed(6)}`}
                    mono
                  />
                  <Row label="Confiance BAN" value={`${Math.round(result.banResolved.score * 100)}%`} />
                </>
              ) : (
                <p className="text-muted-foreground">BAN n'a pas trouvé cette adresse.</p>
              )}
              {result.parcelle ? (
                <>
                  <Row
                    label="Parcelle cadastrale IGN"
                    value={`${result.parcelle.idu} (${result.parcelle.contenance_m2.toLocaleString("fr-FR")} m²)`}
                    mono
                  />
                </>
              ) : null}
              <Row
                label="DPE candidats trouvés (ADEME)"
                value={`${result.totalDpeFound}`}
              />
              <Row
                label="DPE maison strictement matchés"
                value={`${result.matched.length}`}
                strong
              />
            </CardContent>
          </Card>

          {/* Matched DPE cards */}
          {result.matched.length === 0 ? (
            <Card className="border-amber-200 bg-amber-50">
              <CardContent className="flex items-start gap-3 p-5">
                <AlertCircle className="h-5 w-5 shrink-0 text-amber-700" />
                <div>
                  <p className="text-sm font-bold text-amber-900">
                    Aucun DPE maison trouvé pour cette adresse
                  </p>
                  <ul className="mt-2 space-y-1 text-[11px] text-amber-800">
                    {result.notes.map((n, i) => (
                      <li key={i}>{n}</li>
                    ))}
                  </ul>
                </div>
              </CardContent>
            </Card>
          ) : (
            result.matched.map((m, i) => (
              <MaisonResultCard key={m.numero_dpe} maison={m} index={i} onAdd={addToPipeline} loading={adding === i} />
            ))
          )}
        </>
      ) : null}

      {!result && !loading ? (
        <div className="rounded-xl border border-dashed border-border bg-card p-8 text-center text-sm text-muted-foreground">
          Saisissez une adresse pour démarrer. Exemple : <em>14 rue de la Paix 75002 Paris</em>
        </div>
      ) : null}
    </div>
  );
}

function MaisonResultCard({
  maison: m,
  index,
  onAdd,
  loading,
}: {
  maison: MaisonDpe;
  index: number;
  onAdd: (m: MaisonDpe, i: number) => void;
  loading: boolean;
}) {
  return (
    <Card
      className="overflow-hidden border-0 text-white shadow-lg"
      style={{ background: DPE_GRADIENT[m.classe] ?? DPE_GRADIENT.NC }}
    >
      <CardContent className="grid gap-4 p-6 sm:grid-cols-[1fr_120px]">
        <div>
          <div className="flex items-start gap-3">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/20 backdrop-blur">
              <Home className="h-6 w-6" />
            </div>
            <div>
              <h2 className="text-xl font-black tracking-tight">
                {m.address.housenumber} {m.address.street}
              </h2>
              <p className="text-sm opacity-90">
                {m.address.postcode} {m.address.city}
              </p>
              <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <Badge variant="secondary" className="bg-white/20 text-white">
                  {m.type_batiment ?? "maison"}
                </Badge>
                {m.annee_construction ? (
                  <Badge variant="secondary" className="bg-white/20 text-white">
                    Construit en {m.annee_construction}
                  </Badge>
                ) : null}
              </div>
            </div>
          </div>

          <div className="mt-4 grid grid-cols-2 gap-2 lg:grid-cols-4">
            <Kpi icon={<Zap className="h-3.5 w-3.5" />} label="Conso" value={m.conso ? `${m.conso}` : "—"} unit="kWhep/m²/an" />
            <Kpi icon={<Ruler className="h-3.5 w-3.5" />} label="Surface" value={m.surface ? `${m.surface}` : "—"} unit="m²" />
            <Kpi icon={<DpeBadge classe={m.ges} size="sm" />} label="GES" value={m.ges} unit="" />
            <Kpi icon={<Calendar className="h-3.5 w-3.5" />} label="Date DPE" value={m.date ? new Date(m.date).toLocaleDateString("fr-FR") : "—"} unit="" />
          </div>

          <div className="mt-4">
            <DpeScaleBar active={m.classe} />
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => onAdd(m, index)}
              disabled={loading}
              className="bg-white text-foreground hover:bg-white/90"
            >
              {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
              Ajouter au pipeline
            </Button>
            <MaisonDetailSheet
              maison={m}
              trigger={
                <button className="flex items-center gap-1 rounded-md bg-white px-3 py-1.5 text-xs font-bold text-foreground hover:bg-white/90">
                  <Eye className="h-3 w-3" /> Détail complet
                </button>
              }
            />
            <a
              href={m.ademe_url}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 rounded-md bg-white/20 px-3 py-1.5 text-xs font-semibold text-white hover:bg-white/30"
            >
              <ExternalLink className="h-3 w-3" /> Fiche DPE officielle ADEME
            </a>
            {m.lat && m.lon ? (
              <a
                href={`https://www.google.com/maps/search/?api=1&query=${m.lat},${m.lon}`}
                target="_blank"
                rel="noreferrer"
                className="flex items-center gap-1 rounded-md bg-white/20 px-3 py-1.5 text-xs font-semibold text-white hover:bg-white/30"
              >
                <ExternalLink className="h-3 w-3" /> Google Maps
              </a>
            ) : null}
          </div>
          <p className="mt-3 font-mono text-[10px] opacity-70">
            N° DPE : {m.numero_dpe} · Chauffage : {m.energie_principale_chauffage ?? "—"}
          </p>
        </div>

        <div className="flex flex-col items-center justify-center gap-1">
          <div
            className="flex h-24 w-24 items-center justify-center rounded-2xl bg-white shadow-lg"
            style={{
              color:
                DPE_GRADIENT[m.classe]?.match(/#[0-9a-f]+/i)?.[0] ?? "#475569",
            }}
          >
            <span className="text-5xl font-black">{m.classe || "—"}</span>
          </div>
          <p className="text-[10px] uppercase tracking-wider opacity-80">DPE</p>
        </div>
      </CardContent>
    </Card>
  );
}

function Row({
  label,
  value,
  mono,
  strong,
}: {
  label: string;
  value: string;
  mono?: boolean;
  strong?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-2 border-b border-border/40 py-1.5 last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span className={(mono ? "font-mono " : "") + (strong ? "font-bold " : "") + "text-right"}>
        {value}
      </span>
    </div>
  );
}

function Kpi({
  icon,
  label,
  value,
  unit,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  unit: string;
}) {
  return (
    <div className="rounded-lg bg-white/15 p-2 text-xs backdrop-blur">
      <div className="flex items-center gap-1 opacity-80">
        {icon}
        <span className="uppercase tracking-wider">{label}</span>
      </div>
      <div className="mt-0.5 text-base font-black">{value}</div>
      {unit ? <div className="text-[10px] opacity-70">{unit}</div> : null}
    </div>
  );
}

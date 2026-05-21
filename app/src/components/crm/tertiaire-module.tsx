"use client";
import { useEffect, useState, useCallback, useRef } from "react";
import {
  Search,
  Map as MapIcon,
  Building2,
  Loader2,
  Plus,
  Calculator,
  X,
  AlertTriangle,
  Users,
  MapPin,
  Zap,
  ExternalLink,
  CheckCircle2,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { TertiaireMap, type TertiairePoint, type MapBounds } from "@/components/map/tertiaire-map";
import { TertiairePanel } from "@/components/map/tertiaire-panel";
import { CeeTertiairePostes } from "@/components/crm/cee-tertiaire-postes";

type LookupResult = {
  query: string;
  geocode: { label: string; lat: number; lon: number; postcode?: string; city?: string; citycode?: string } | null;
  parcelle: { idu: string; section: string; numero: string; contenance_m2: number } | null;
  bdnb: {
    batimentGroupeId: string;
    surfaceUtileTertiaire?: number | null;
    typeUsage?: string | null;
    dpeTertiaire?: { etiquetteDpe?: string | null; etiquetteGes?: string | null; numeroDpe?: string | null } | null;
  } | null;
  dpeTertiaire: {
    numero_dpe?: string;
    etiquette_dpe?: string;
    etiquette_ges?: string;
    conso_kwhep_m2_an?: number | string;
    emission_ges_kgco2_m2_an?: number | string;
    surface_utile?: number | string;
    type_usage_principal?: string;
    annee_construction?: number | string;
    date_etablissement_dpe?: string;
  } | null;
  occupants: Array<{
    siret: string;
    siren: string;
    denomination: string | null;
    nafCode: string | null;
    nafLabel: string | null;
    trancheEffectif: string | null;
    estSiege: boolean;
    estActif: boolean;
  }>;
  diagnostics: { sourceDpe: "bdnb" | "ademe" | "none"; bdnbCandidates: number; dpeCandidates: number; occupantsCount: number };
};

type Tab = "search" | "map";

const DPE_COLORS: Record<string, string> = {
  A: "bg-emerald-500 text-white",
  B: "bg-lime-500 text-white",
  C: "bg-yellow-400 text-stone-900",
  D: "bg-amber-500 text-white",
  E: "bg-orange-500 text-white",
  F: "bg-red-500 text-white",
  G: "bg-rose-700 text-white",
};

function DpeBadge({ value }: { value?: string | null }) {
  if (!value) return <span className="text-xs text-muted-foreground">—</span>;
  const color = DPE_COLORS[value.toUpperCase()] ?? "bg-stone-300";
  return (
    <span className={`inline-flex h-7 w-7 items-center justify-center rounded-md text-sm font-bold ${color}`}>
      {value.toUpperCase()}
    </span>
  );
}

const SECTORS = ["Bureaux", "Commerces", "Hotellerie / Restauration", "Sante", "Enseignement", "Autres secteurs"];
const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G"];

export function TertiaireModule() {
  const [tab, setTab] = useState<Tab>("search");
  // Simulateur CEE overlay
  const [ceeOverlay, setCeeOverlay] = useState<{
    sector?: string | null;
    postalCode?: string | null;
    surface?: number | null;
    year?: number | null;
    label?: string;
  } | null>(null);

  return (
    <div className="flex h-full flex-col">
      {/* Tabs header */}
      <div className="border-b border-border bg-card/40 backdrop-blur">
        <div className="flex gap-1 px-4">
          <TabButton active={tab === "search"} onClick={() => setTab("search")} icon={<Search className="h-3.5 w-3.5" />}>
            Recherche par adresse
          </TabButton>
          <TabButton active={tab === "map"} onClick={() => setTab("map")} icon={<MapIcon className="h-3.5 w-3.5" />}>
            Carte IDF
          </TabButton>
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        {tab === "search" ? (
          <SearchView onSimulerCee={setCeeOverlay} />
        ) : (
          <MapView onSimulerCee={setCeeOverlay} />
        )}
      </div>

      {ceeOverlay ? (
        <CeeOverlay
          context={ceeOverlay}
          onClose={() => setCeeOverlay(null)}
        />
      ) : null}
    </div>
  );
}

function TabButton({ active, onClick, icon, children }: { active: boolean; onClick: () => void; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className={`relative flex items-center gap-1.5 px-4 py-3 text-xs font-semibold uppercase tracking-wider transition-colors ${
        active ? "text-primary" : "text-muted-foreground hover:text-foreground"
      }`}
    >
      {icon}
      {children}
      {active ? <span className="absolute inset-x-0 bottom-0 h-0.5 bg-primary" /> : null}
    </button>
  );
}

/* =============================== SEARCH VIEW =============================== */

function SearchView({ onSimulerCee }: { onSimulerCee: (ctx: { sector?: string | null; postalCode?: string | null; surface?: number | null; year?: number | null; label?: string }) => void }) {
  const [query, setQuery] = useState("31 rue de Mogador, 75009 Paris");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<LookupResult | null>(null);
  const [savedBuildingId, setSavedBuildingId] = useState<number | null>(null);
  const [creatingProspect, setCreatingProspect] = useState(false);

  const onSubmit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    const q = query.trim();
    if (q.length < 5) { setError("Adresse trop courte"); return; }
    setLoading(true); setError(null); setResult(null); setSavedBuildingId(null);
    try {
      const res = await fetch(`/api/tertiaire/lookup?q=${encodeURIComponent(q)}`);
      const json = await res.json();
      if (!res.ok) setError(json?.error ?? `Erreur ${res.status}`);
      else setResult(json as LookupResult);
    } catch (err) { setError((err as Error).message); }
    finally { setLoading(false); }
  };

  const saveAndCreateProspect = async () => {
    if (!result) return;
    setCreatingProspect(true);
    try {
      const saveRes = await fetch("/api/tertiaire/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result),
      });
      if (!saveRes.ok) throw new Error(`save ${saveRes.status}`);
      const { buildingId } = await saveRes.json();
      setSavedBuildingId(buildingId);

      const prospectRes = await fetch(`/api/tertiaire/${buildingId}/create-prospect`, { method: "POST" });
      if (!prospectRes.ok) throw new Error(`create-prospect ${prospectRes.status}`);
      const prospectJson = await prospectRes.json() as { prospect: { id: number }; created: boolean };
      toast.success(prospectJson.created ? "Prospect créé" : "Prospect existant rouvert");
    } catch (err) {
      toast.error((err as Error).message);
    } finally {
      setCreatingProspect(false);
    }
  };

  const simulerCee = () => {
    if (!result) return;
    const surface = result.bdnb?.surfaceUtileTertiaire ?? Number(result.dpeTertiaire?.surface_utile ?? 0) || null;
    const year = Number(result.dpeTertiaire?.annee_construction ?? 0) || null;
    onSimulerCee({
      sector: mapTypeToSecteur(result.bdnb?.typeUsage ?? result.dpeTertiaire?.type_usage_principal),
      postalCode: result.geocode?.postcode ?? null,
      surface,
      year,
      label: result.geocode?.label,
    });
  };

  return (
    <div className="mx-auto h-full max-w-6xl space-y-5 overflow-auto p-6">
      <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
        <div className="mb-3 flex items-center gap-2">
          <Building2 className="h-5 w-5 text-primary" />
          <h2 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground">Recherche d'un bâtiment tertiaire</h2>
        </div>
        <form onSubmit={onSubmit} className="flex flex-col gap-2 sm:flex-row">
          <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Adresse complète" className="flex-1" disabled={loading} />
          <Button type="submit" disabled={loading} className="gap-2">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            Rechercher
          </Button>
        </form>
        <p className="mt-2 text-xs text-muted-foreground">
          BAN → Cadastre IGN → BDNB → DPE tertiaire ADEME → Recherche d'entreprises (occupants)
        </p>
      </div>

      {error ? (
        <div className="flex items-start gap-3 rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-900">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <div><p className="font-semibold">Erreur</p><p>{error}</p></div>
        </div>
      ) : null}

      {result ? (
        <>
          {/* Actions principales */}
          <div className="flex flex-wrap items-center gap-2 rounded-xl border border-border bg-card p-3">
            <span className="text-xs text-muted-foreground">Actions :</span>
            <Button size="sm" onClick={saveAndCreateProspect} disabled={creatingProspect} className="gap-1.5">
              {creatingProspect ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
              Sauvegarder + créer prospect
            </Button>
            <Button size="sm" variant="secondary" onClick={simulerCee} className="gap-1.5">
              <Calculator className="h-3.5 w-3.5" />
              Simuler CEE
            </Button>
            {savedBuildingId ? (
              <Badge className="ml-auto gap-1.5 bg-emerald-100 text-emerald-900">
                <CheckCircle2 className="h-3 w-3" /> Bâtiment #{savedBuildingId} sauvegardé
              </Badge>
            ) : null}
          </div>

          {/* 4 cartes */}
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <ResultCard icon={<MapPin className="h-4 w-4 text-primary" />} title="Adresse & parcelle">
              {result.geocode ? (
                <dl className="space-y-1 text-sm">
                  <Row label="Adresse géocodée" value={result.geocode.label} />
                  <Row label="Coordonnées" value={`${result.geocode.lat.toFixed(6)}, ${result.geocode.lon.toFixed(6)}`} />
                  <Row label="Commune INSEE" value={result.geocode.citycode ?? "—"} />
                  <Row label="Code postal" value={result.geocode.postcode ?? "—"} />
                  {result.parcelle ? (
                    <>
                      <Row label="Parcelle" value={`${result.parcelle.section} / ${result.parcelle.numero}`} />
                      <Row label="IDU" value={result.parcelle.idu} />
                      <Row label="Surface" value={`${result.parcelle.contenance_m2.toLocaleString("fr-FR")} m²`} />
                    </>
                  ) : <p className="pt-2 text-xs text-muted-foreground">Aucune parcelle.</p>}
                </dl>
              ) : <p className="text-sm text-muted-foreground">Adresse non géocodée.</p>}
            </ResultCard>

            <ResultCard
              icon={<Building2 className="h-4 w-4 text-primary" />}
              title="Fiche bâtiment BDNB"
              right={<span className="text-[10px] uppercase tracking-wider text-muted-foreground">{result.diagnostics.bdnbCandidates} candidat(s)</span>}
            >
              {result.bdnb ? (
                <dl className="space-y-1 text-sm">
                  <Row label="ID bâtiment groupe" value={result.bdnb.batimentGroupeId} />
                  <Row label="Surface utile tertiaire" value={result.bdnb.surfaceUtileTertiaire ? `${result.bdnb.surfaceUtileTertiaire.toLocaleString("fr-FR")} m²` : "—"} />
                  <Row label="Type d'usage" value={result.bdnb.typeUsage ?? "—"} />
                  {result.bdnb.dpeTertiaire?.etiquetteDpe ? (
                    <div className="flex items-center gap-2 pt-2">
                      <span className="text-xs text-muted-foreground">DPE :</span>
                      <DpeBadge value={result.bdnb.dpeTertiaire.etiquetteDpe} />
                      <span className="text-xs text-muted-foreground">GES :</span>
                      <DpeBadge value={result.bdnb.dpeTertiaire.etiquetteGes} />
                    </div>
                  ) : null}
                </dl>
              ) : <p className="text-sm text-muted-foreground">Aucun bâtiment BDNB trouvé.</p>}
            </ResultCard>

            <ResultCard
              icon={<Zap className="h-4 w-4 text-primary" />}
              title="DPE tertiaire ADEME"
              right={<span className="rounded-full bg-secondary px-2 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">source: {result.diagnostics.sourceDpe}</span>}
            >
              {result.dpeTertiaire ? (
                <dl className="space-y-1 text-sm">
                  <Row label="N° DPE" value={result.dpeTertiaire.numero_dpe ?? "—"} />
                  <div className="flex items-center gap-2 py-1">
                    <span className="w-44 text-xs text-muted-foreground">Étiquettes</span>
                    <DpeBadge value={String(result.dpeTertiaire.etiquette_dpe ?? "")} />
                    <span className="text-xs text-muted-foreground">DPE</span>
                    <DpeBadge value={String(result.dpeTertiaire.etiquette_ges ?? "")} />
                    <span className="text-xs text-muted-foreground">GES</span>
                  </div>
                  <Row label="Conso EP" value={result.dpeTertiaire.conso_kwhep_m2_an ? `${Number(result.dpeTertiaire.conso_kwhep_m2_an).toLocaleString("fr-FR")} kWhEP/m²/an` : "—"} />
                  <Row label="Émissions GES" value={result.dpeTertiaire.emission_ges_kgco2_m2_an ? `${Number(result.dpeTertiaire.emission_ges_kgco2_m2_an).toLocaleString("fr-FR")} kgCO₂/m²/an` : "—"} />
                  <Row label="Surface utile" value={result.dpeTertiaire.surface_utile ? `${Number(result.dpeTertiaire.surface_utile).toLocaleString("fr-FR")} m²` : "—"} />
                  <Row label="Usage" value={result.dpeTertiaire.type_usage_principal ?? "—"} />
                  <Row label="Année construction" value={result.dpeTertiaire.annee_construction ?? "—"} />
                  <Row label="Date DPE" value={result.dpeTertiaire.date_etablissement_dpe ?? "—"} />
                </dl>
              ) : <p className="text-sm text-muted-foreground">Aucun DPE tertiaire trouvé.</p>}
            </ResultCard>

            <ResultCard
              icon={<Users className="h-4 w-4 text-primary" />}
              title="Sociétés occupantes"
              right={<span className="text-[10px] uppercase tracking-wider text-muted-foreground">{result.diagnostics.occupantsCount} actives</span>}
            >
              {result.occupants.length === 0 ? (
                <p className="text-sm text-muted-foreground">Aucune société active.</p>
              ) : (
                <ul className="space-y-2">
                  {result.occupants.slice(0, 12).map((o) => (
                    <li key={o.siret} className="rounded-lg border border-border/50 bg-secondary/30 p-3 text-sm">
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0 flex-1">
                          <p className="truncate font-semibold">{o.denomination ?? "(sans dénomination)"}</p>
                          <p className="truncate text-[11px] text-muted-foreground">
                            SIRET {o.siret}{o.estSiege ? " · siège" : ""}{o.trancheEffectif ? ` · ${o.trancheEffectif}` : ""}
                          </p>
                          {o.nafLabel ? <p className="truncate text-[11px] text-muted-foreground">{o.nafCode} · {o.nafLabel}</p> : null}
                        </div>
                        {o.siren ? (
                          <a href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${o.siren}`} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-primary" title="Voir sur Annuaire Entreprises">
                            <ExternalLink className="h-3 w-3" />
                          </a>
                        ) : null}
                      </div>
                    </li>
                  ))}
                  {result.occupants.length > 12 ? <p className="text-xs text-muted-foreground">+ {result.occupants.length - 12} autres…</p> : null}
                </ul>
              )}
              <p className="mt-3 border-t border-border/50 pt-2 text-[11px] italic text-muted-foreground">
                Propriétaire foncier : non disponible (DV3F sous convention).
              </p>
            </ResultCard>
          </div>
        </>
      ) : null}
    </div>
  );
}

function ResultCard({ icon, title, right, children }: { icon: React.ReactNode; title: string; right?: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-3 flex items-center gap-2">
        {icon}
        <h3 className="text-sm font-semibold">{title}</h3>
        {right ? <div className="ml-auto">{right}</div> : null}
      </div>
      {children}
    </section>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex gap-2 py-0.5">
      <dt className="w-44 shrink-0 text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 flex-1 break-words">{value}</dd>
    </div>
  );
}

/* =============================== MAP VIEW =============================== */

function MapView({ onSimulerCee }: { onSimulerCee: (ctx: { sector?: string | null; postalCode?: string | null; surface?: number | null; year?: number | null; label?: string }) => void }) {
  const [points, setPoints] = useState<TertiairePoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [filterSecteur, setFilterSecteur] = useState<string>("");
  const [filterDpe, setFilterDpe] = useState<string>("");
  const boundsRef = useRef<MapBounds | null>(null);
  const reloadTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchPoints = useCallback(async () => {
    if (!boundsRef.current) return;
    if (boundsRef.current.zoom < 10) { setPoints([]); return; }
    setLoading(true);
    try {
      const params = new URLSearchParams();
      const b = boundsRef.current;
      params.set("bbox", `${b.minLon},${b.minLat},${b.maxLon},${b.maxLat}`);
      if (filterSecteur) params.set("secteur", filterSecteur);
      if (filterDpe) params.set("dpe", filterDpe);
      params.set("limit", "3000");
      const res = await fetch(`/api/tertiaire/list?${params.toString()}`);
      if (!res.ok) throw new Error(`list ${res.status}`);
      const json = await res.json() as { items: TertiairePoint[] };
      setPoints(json.items);
    } catch (err) {
      toast.error((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, [filterSecteur, filterDpe]);

  const onBoundsChange = useCallback((b: MapBounds) => {
    boundsRef.current = b;
    if (reloadTimer.current) clearTimeout(reloadTimer.current);
    reloadTimer.current = setTimeout(fetchPoints, 350);
  }, [fetchPoints]);

  // Re-fetch quand filtres changent
  useEffect(() => { fetchPoints(); }, [filterSecteur, filterDpe]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="flex h-full">
      <div className="relative flex-1">
        {/* Toolbar filtres */}
        <div className="absolute left-3 top-3 z-10 flex items-center gap-2 rounded-lg border border-border bg-card/95 p-2 shadow-md backdrop-blur">
          <select
            value={filterSecteur}
            onChange={(e) => setFilterSecteur(e.target.value)}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs"
          >
            <option value="">Tous secteurs</option>
            {SECTORS.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
          <select
            value={filterDpe}
            onChange={(e) => setFilterDpe(e.target.value)}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs"
          >
            <option value="">Toutes classes DPE</option>
            {DPE_CLASSES.map((c) => <option key={c} value={c}>Classe {c}</option>)}
            <option value="FG">F + G (passoires)</option>
            <option value="DEFG">D à G</option>
          </select>
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            {loading ? "Chargement…" : `${points.length} bâtiments`}
          </span>
        </div>

        <TertiaireMap
          points={points}
          onBoundsChange={onBoundsChange}
          onSelectBuilding={setSelectedId}
          selectedId={selectedId}
        />

        {points.length === 0 && !loading && boundsRef.current && boundsRef.current.zoom >= 10 ? (
          <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded-lg border border-border bg-card/95 p-3 text-xs text-muted-foreground shadow-md">
            Aucun bâtiment importé dans cette zone. Lance le script <code className="rounded bg-secondary px-1">npx tsx scripts/import-dpe-tertiaire-idf.ts</code> ou utilise la recherche par adresse.
          </div>
        ) : null}

        {boundsRef.current && boundsRef.current.zoom < 10 ? (
          <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded-lg border border-border bg-card/95 p-3 text-xs text-muted-foreground shadow-md">
            Zoome (zoom ≥ 10) pour afficher les bâtiments tertiaires.
          </div>
        ) : null}
      </div>

      {/* Panneau latéral */}
      {selectedId ? (
        <div className="w-[400px] shrink-0 border-l border-border">
          <TertiairePanel
            buildingId={selectedId}
            onClose={() => setSelectedId(null)}
            onSimulerCee={(d) => onSimulerCee({
              sector: d.building.secteur,
              postalCode: d.building.code_postal,
              surface: d.building.surface_m2,
              year: d.building.annee_construction,
              label: d.building.label ?? d.building.adresse ?? `#${d.building.id}`,
            })}
          />
        </div>
      ) : null}
    </div>
  );
}

/* =============================== CEE OVERLAY =============================== */

function CeeOverlay({
  context,
  onClose,
}: {
  context: { sector?: string | null; postalCode?: string | null; surface?: number | null; year?: number | null; label?: string };
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-stone-900/40 p-4">
      <div className="flex h-[90vh] w-full max-w-5xl flex-col overflow-hidden rounded-2xl border border-border bg-card shadow-2xl">
        <div className="flex items-center justify-between border-b border-border bg-card px-5 py-4">
          <div>
            <div className="flex items-center gap-2">
              <Calculator className="h-4 w-4 text-primary" />
              <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Simulateur CEE tertiaire</span>
            </div>
            <h3 className="mt-1 text-base font-semibold">{context.label ?? "Simulation libre"}</h3>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>
        <div className="flex-1 overflow-auto p-5">
          <CeeTertiairePostes
            defaultSector={context.sector ?? null}
            defaultPostalCode={context.postalCode ?? null}
            defaultSurface={context.surface ?? null}
            defaultYear={context.year ?? null}
          />
        </div>
      </div>
    </div>
  );
}

/* =============================== UTILS =============================== */

function mapTypeToSecteur(typeUsage: string | null | undefined): string | null {
  if (!typeUsage) return null;
  const s = typeUsage.toLowerCase();
  if (s.includes("bureau")) return "Bureaux";
  if (s.includes("commerce") || s.includes("magasin")) return "Commerces";
  if (s.includes("hotel") || s.includes("hôtel") || s.includes("restaur")) return "Hotellerie / Restauration";
  if (s.includes("sante") || s.includes("santé") || s.includes("hopit") || s.includes("clinique")) return "Sante";
  if (s.includes("enseign") || s.includes("ecole") || s.includes("école") || s.includes("scolaire")) return "Enseignement";
  return "Autres secteurs";
}

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
  Database,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { TertiaireMap, type TertiairePoint, type MapBounds } from "@/components/map/tertiaire-map";
import { TertiairePanel } from "@/components/map/tertiaire-panel";
import { CeeTertiairePostes } from "@/components/crm/cee-tertiaire-postes";
import { AddressAutocomplete } from "@/components/crm/address-autocomplete";

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
    classe_consommation_energie?: string;
    classe_estimation_ges?: string;
    consommation_energie?: number | string;
    estimation_ges?: number | string;
    surface_utile?: number | string;
    secteur_activite?: string;
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
  buildingId: number | null;
  nearbyBuildings: Array<{
    id: number;
    label: string | null;
    adresse: string | null;
    secteur: string | null;
    lat: number;
    lon: number;
    distanceM: number;
    etiquette_dpe: string | null;
  }>;
  diagnostics: { sourceDpe: "db" | "bdnb" | "ademe" | "none"; bdnbCandidates: number; dpeCandidates: number; occupantsCount: number; dbBuildings: number };
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
      // Si le bâtiment est déjà en DB (trouvé par lookup), on évite le save.
      let buildingId = result.buildingId;
      if (!buildingId) {
        const saveRes = await fetch("/api/tertiaire/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(result),
        });
        if (!saveRes.ok) throw new Error(`save ${saveRes.status}`);
        const saveJson = await saveRes.json() as { buildingId: number };
        buildingId = saveJson.buildingId;
      }
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
    const surface = result.bdnb?.surfaceUtileTertiaire ?? (Number(result.dpeTertiaire?.surface_utile ?? 0) || null);
    const year = Number(result.dpeTertiaire?.annee_construction ?? 0) || null;
    onSimulerCee({
      sector: mapTypeToSecteur(result.bdnb?.typeUsage ?? result.dpeTertiaire?.secteur_activite),
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
          <AddressAutocomplete
            value={query}
            onChange={setQuery}
            onSelect={(s) => {
              setQuery(s.label);
              // déclenche aussitôt la recherche sur sélection
              setTimeout(() => onSubmit(), 0);
            }}
            onSubmit={() => onSubmit()}
            placeholder="Tape une adresse (suggestions automatiques BAN)"
            disabled={loading}
          />
          <Button type="submit" disabled={loading} className="gap-2">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            Rechercher
          </Button>
        </form>
        <p className="mt-2 text-xs text-muted-foreground">
          BAN → Cadastre IGN → DB locale (80k bâtiments IDF) → DPE ADEME live → Recherche d'entreprises (occupants)
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
              {result.buildingId ? "Créer prospect" : "Sauvegarder + créer prospect"}
            </Button>
            <Button size="sm" variant="secondary" onClick={simulerCee} className="gap-1.5">
              <Calculator className="h-3.5 w-3.5" />
              Simuler CEE
            </Button>
            {result.buildingId ? (
              <Badge variant="secondary" className="gap-1.5 text-[10px]">
                <Database className="h-3 w-3" /> En base #{result.buildingId}
              </Badge>
            ) : null}
            {savedBuildingId ? (
              <Badge className="ml-auto gap-1.5 bg-emerald-100 text-emerald-900">
                <CheckCircle2 className="h-3 w-3" /> Sauvegardé #{savedBuildingId}
              </Badge>
            ) : null}
          </div>

          {/* Bâtiments proches en DB (si l'adresse exacte n'a pas de DPE) */}
          {result.nearbyBuildings.length > 0 && result.diagnostics.sourceDpe !== "db" ? (
            <div className="rounded-xl border border-amber-200 bg-amber-50 p-4">
              <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-amber-900">
                <Database className="h-4 w-4" />
                {result.nearbyBuildings.length} bâtiment(s) tertiaire(s) en base à proximité
              </div>
              <p className="mb-3 text-xs text-amber-800">
                L'adresse exacte n'a pas de DPE tertiaire référencé mais ces bâtiments DB sont à moins de 150m :
              </p>
              <ul className="space-y-1">
                {result.nearbyBuildings.slice(0, 8).map((b) => (
                  <li key={b.id} className="flex items-center justify-between gap-2 rounded border border-amber-200 bg-white p-2 text-xs">
                    <div className="min-w-0 flex-1">
                      <p className="truncate font-medium">
                        {b.etiquette_dpe ? <DpeBadge value={b.etiquette_dpe} /> : null} {b.label ?? b.adresse}
                      </p>
                      <p className="truncate text-[10px] text-muted-foreground">
                        {b.secteur ?? "—"} · à {Math.round(b.distanceM)}m
                      </p>
                    </div>
                    <Badge variant="outline" className="shrink-0 text-[10px]">#{b.id}</Badge>
                  </li>
                ))}
              </ul>
            </div>
          ) : null}

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
                    <DpeBadge value={String(result.dpeTertiaire.classe_consommation_energie ?? "")} />
                    <span className="text-xs text-muted-foreground">DPE</span>
                    <DpeBadge value={String(result.dpeTertiaire.classe_estimation_ges ?? "")} />
                    <span className="text-xs text-muted-foreground">GES</span>
                  </div>
                  <Row label="Conso EP" value={result.dpeTertiaire.consommation_energie ? `${Number(result.dpeTertiaire.consommation_energie).toLocaleString("fr-FR")} kWhEP/m²/an` : "—"} />
                  <Row label="Émissions GES" value={result.dpeTertiaire.estimation_ges ? `${Number(result.dpeTertiaire.estimation_ges).toLocaleString("fr-FR")} kgCO₂/m²/an` : "—"} />
                  <Row label="Surface utile" value={result.dpeTertiaire.surface_utile ? `${Number(result.dpeTertiaire.surface_utile).toLocaleString("fr-FR")} m²` : "—"} />
                  <Row label="Usage" value={result.dpeTertiaire.secteur_activite ?? "—"} />
                  <Row label="Année construction" value={result.dpeTertiaire.annee_construction ?? "—"} />
                  <Row label="Date DPE" value={result.dpeTertiaire.date_etablissement_dpe ?? "—"} />
                </dl>
              ) : <p className="text-sm text-muted-foreground">Aucun DPE tertiaire trouvé dans un rayon de 80m. {result.diagnostics.dbBuildings > 0 ? `${result.diagnostics.dbBuildings} bâtiment(s) DB proche(s) — voir liste ci-dessous.` : ""}</p>}
            </ResultCard>

            <ResultCard
              icon={<Users className="h-4 w-4 text-primary" />}
              title="Sociétés occupantes"
              right={<span className="text-[10px] uppercase tracking-wider text-muted-foreground">{result.diagnostics.occupantsCount} actives</span>}
            >
              {result.occupants.length === 0 ? (
                <div className="space-y-1 rounded border border-border/50 bg-secondary/20 p-3">
                  <p className="text-sm font-medium">Aucune société SIRENE à cette adresse exacte.</p>
                  <p className="text-xs text-muted-foreground">
                    Causes possibles : adresse résidentielle, récente, ou nom de rue historique
                    différent. Relance la recherche (Ctrl+F5) si tu viens d'un état caché.
                  </p>
                </div>
              ) : (
                <SortedOccupantsList occupants={result.occupants} />
              )}
              <p className="mt-3 border-t border-border/50 pt-2 text-[11px] italic text-muted-foreground">
                Propriétaire foncier : DV3F (Cerema sous convention publique).
              </p>
            </ResultCard>
          </div>
        </>
      ) : null}
    </div>
  );
}

// === Tri par effectif + détection domiciliations ===
type OccupantSearch = {
  siret: string;
  siren: string;
  denomination: string | null;
  nafCode: string | null;
  nafLabel: string | null;
  trancheEffectif: string | null;
  estSiege: boolean;
  estActif: boolean;
};

const EFFECTIF_ORDER: Record<string, number> = {
  "53": 14, "52": 13, "51": 12, "42": 11, "41": 10, "32": 9, "31": 8,
  "22": 7, "21": 6, "12": 5, "11": 4, "03": 3, "02": 2, "01": 1, "00": 0,
};
function effRank(c: string | null): number {
  if (!c) return -1;
  return EFFECTIF_ORDER[c] ?? -1;
}
function effLabel(c: string | null): string {
  if (!c) return "Non renseigné";
  const m: Record<string, string> = {
    "00": "0 salarié", "01": "1-2", "02": "3-5", "03": "6-9",
    "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
    "31": "200-249", "32": "250-499", "41": "500-999", "42": "1000-1999",
    "51": "2000-4999", "52": "5000-9999", "53": "10000+", "NN": "Non renseigné",
  };
  return m[c] ?? c;
}
function isDomic(o: OccupantSearch): boolean {
  return !o.trancheEffectif || o.trancheEffectif === "NN" || o.trancheEffectif === "00";
}

function OccupantRow({ o, accent }: { o: OccupantSearch; accent: "real" | "domic" }) {
  return (
    <li
      className={`rounded-lg border p-3 text-sm ${
        accent === "real" ? "border-emerald-200 bg-emerald-50/30" : "border-amber-200 bg-amber-50/30"
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="truncate font-semibold">{o.denomination ?? "(sans dénomination)"}</p>
          <p className="truncate text-[11px] text-muted-foreground">
            SIRET {o.siret}
            {o.estSiege ? " · siège" : ""}
            {" · "}
            <span className={accent === "real" ? "font-semibold text-emerald-700" : "text-amber-700"}>
              {effLabel(o.trancheEffectif)}
            </span>
          </p>
          {o.nafLabel ? (
            <p className="truncate text-[11px] text-muted-foreground">
              {o.nafCode} · {o.nafLabel}
            </p>
          ) : null}
        </div>
        {o.siren ? (
          <a
            href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${o.siren}`}
            target="_blank"
            rel="noopener noreferrer"
            className="text-muted-foreground hover:text-primary"
            title="Voir sur Annuaire Entreprises"
          >
            <ExternalLink className="h-3 w-3" />
          </a>
        ) : null}
      </div>
    </li>
  );
}

function SortedOccupantsList({ occupants }: { occupants: OccupantSearch[] }) {
  const [showDomic, setShowDomic] = useState(false);
  const sorted = [...occupants].sort((a, b) => {
    const r = effRank(b.trancheEffectif) - effRank(a.trancheEffectif);
    if (r !== 0) return r;
    return (a.denomination ?? "").localeCompare(b.denomination ?? "");
  });
  const reals = sorted.filter((o) => !isDomic(o));
  const domics = sorted.filter(isDomic);

  return (
    <div className="space-y-4">
      {/* Section 1 : présence physique réelle (employeurs avec salariés) */}
      <div>
        <div className="mb-2 flex items-center gap-2">
          <span className="inline-flex h-5 items-center gap-1 rounded-full bg-emerald-100 px-2 text-[11px] font-semibold text-emerald-900">
            ● {reals.length} société(s) présente(s)
          </span>
          <span className="text-[10px] text-muted-foreground">avec salariés / activité réelle</span>
        </div>
        {reals.length === 0 ? (
          <p className="rounded border border-border/50 bg-secondary/20 p-2 text-xs text-muted-foreground">
            Aucune société avec salariés enregistrée. Toutes les sociétés à cette adresse semblent
            être domiciliées (boîte aux lettres).
          </p>
        ) : (
          <ul className="space-y-2">
            {reals.slice(0, 30).map((o) => <OccupantRow key={o.siret} o={o} accent="real" />)}
            {reals.length > 30 ? <p className="text-xs text-muted-foreground">+ {reals.length - 30} autres employeurs…</p> : null}
          </ul>
        )}
      </div>

      {/* Section 2 : domiciliations probables (collapsable) */}
      {domics.length > 0 ? (
        <div>
          <button
            onClick={() => setShowDomic((v) => !v)}
            className="mb-2 flex w-full items-center justify-between rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-xs hover:bg-amber-100"
          >
            <span className="font-semibold text-amber-900">
              ○ {domics.length} domiciliation(s) probable(s)
            </span>
            <span className="text-[11px] text-amber-900">
              {showDomic ? "Masquer ▲" : "Afficher ▼"}
            </span>
          </button>
          {showDomic ? (
            <ul className="space-y-2">
              {domics.slice(0, 30).map((o) => <OccupantRow key={o.siret} o={o} accent="domic" />)}
              {domics.length > 30 ? <p className="text-xs text-muted-foreground">+ {domics.length - 30} autres…</p> : null}
            </ul>
          ) : (
            <p className="text-[11px] italic text-muted-foreground">
              SCI, holdings, sociétés sans salariés enregistrées à l'adresse mais probablement
              sans présence physique. Cliquez pour voir.
            </p>
          )}
        </div>
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

// Bbox initial par défaut centré sur Paris pour amorcer la carte avant le
// premier `moveend` de MapLibre.
const DEFAULT_BBOX: MapBounds = {
  minLon: 2.20, maxLon: 2.50,
  minLat: 48.80, maxLat: 48.95,
  zoom: 11,
};

const ZOOM_MIN = 9;

type Stats = {
  totalBuildings: number;
  withCoords: number;
  parisBboxCount: number;
  db: { tursoUrlSet: boolean; tursoUrlPrefix: string | null; authTokenSet: boolean };
};

function MapView({ onSimulerCee }: { onSimulerCee: (ctx: { sector?: string | null; postalCode?: string | null; surface?: number | null; year?: number | null; label?: string }) => void }) {
  const [points, setPoints] = useState<TertiairePoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [filterSecteur, setFilterSecteur] = useState<string>("");
  const [filterDpe, setFilterDpe] = useState<string>("");
  const [filterCp, setFilterCp] = useState<string>("");
  const [filterCommune, setFilterCommune] = useState<string>("");
  const [filterSurfaceMin, setFilterSurfaceMin] = useState<string>("");
  const [filterSurfaceMax, setFilterSurfaceMax] = useState<string>("");
  const [filterYearMin, setFilterYearMin] = useState<string>("");
  const [filterYearMax, setFilterYearMax] = useState<string>("");
  const [filterProprio, setFilterProprio] = useState<string>("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  // Pré-initialisé pour que le 1er fetch fonctionne même si MapLibre tarde
  const boundsRef = useRef<MapBounds>(DEFAULT_BBOX);
  const reloadTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Diagnostic au mount : compte global en DB pour comprendre si la prod
  // voit bien les bâtiments importés.
  useEffect(() => {
    fetch("/api/tertiaire/stats")
      .then((r) => (r.ok ? r.json() : null))
      .then((j) => setStats(j))
      .catch(() => {});
  }, []);

  const fetchPoints = useCallback(async () => {
    const b = boundsRef.current;
    // Si filtres CP/commune actifs → on ignore le bbox (recherche globale)
    const hasZoneFilter = Boolean(filterCp || filterCommune || filterProprio);
    if (!hasZoneFilter && b.zoom < ZOOM_MIN) { setPoints([]); return; }
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (!hasZoneFilter) {
        params.set("bbox", `${b.minLon},${b.minLat},${b.maxLon},${b.maxLat}`);
      }
      if (filterSecteur) params.set("secteur", filterSecteur);
      if (filterDpe) params.set("dpe", filterDpe);
      if (filterCp) params.set("cp", filterCp);
      if (filterCommune) params.set("commune", filterCommune);
      if (filterSurfaceMin) params.set("surfaceMin", filterSurfaceMin);
      if (filterSurfaceMax) params.set("surfaceMax", filterSurfaceMax);
      if (filterYearMin) params.set("yearMin", filterYearMin);
      if (filterYearMax) params.set("yearMax", filterYearMax);
      if (filterProprio) params.set("proprietaire", filterProprio);
      params.set("limit", "5000");
      const res = await fetch(`/api/tertiaire/list?${params.toString()}`);
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        const msg = (json as { error?: string }).error ?? `HTTP ${res.status}`;
        setError(msg);
        return;
      }
      setPoints((json as { items: TertiairePoint[] }).items ?? []);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, [filterSecteur, filterDpe, filterCp, filterCommune, filterSurfaceMin, filterSurfaceMax, filterYearMin, filterYearMax, filterProprio]);

  // La carte ne fetch PLUS automatiquement quand on bouge / zoom — uniquement
  // sur clic du bouton "Rechercher". On garde la bbox à jour pour le prochain
  // submit, mais on ne déclenche aucune requête.
  const onBoundsChange = useCallback((b: MapBounds) => {
    boundsRef.current = b;
  }, []);

  // Fetch initial UNE FOIS au montage (avec DEFAULT_BBOX Paris) pour amorcer.
  const hasMounted = useRef(false);
  useEffect(() => {
    if (hasMounted.current) return;
    hasMounted.current = true;
    fetchPoints();
  }, [fetchPoints]);

  return (
    <div className="flex h-full">
      <div className="relative flex-1">
        {/* Toolbar filtres (avancés affichables) */}
        <div className="absolute left-3 top-3 z-10 w-[400px] max-w-[calc(100vw-2rem)] rounded-lg border border-border bg-card/95 p-3 shadow-md backdrop-blur">
          <div className="mb-2 flex items-center gap-2">
            <select
              value={filterSecteur}
              onChange={(e) => setFilterSecteur(e.target.value)}
              className="flex-1 rounded-md border border-input bg-background px-2 py-1 text-xs"
            >
              <option value="">Tous secteurs</option>
              {SECTORS.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <select
              value={filterDpe}
              onChange={(e) => setFilterDpe(e.target.value)}
              className="flex-1 rounded-md border border-input bg-background px-2 py-1 text-xs"
            >
              <option value="">Toutes classes DPE</option>
              {DPE_CLASSES.map((c) => <option key={c} value={c}>Classe {c}</option>)}
              <option value="FG">F + G (passoires)</option>
              <option value="DEFG">D à G</option>
            </select>
            <button
              onClick={() => setShowAdvanced((v) => !v)}
              className="rounded-md border border-input bg-background px-2 py-1 text-[10px] hover:bg-secondary"
            >
              {showAdvanced ? "Moins" : "Filtres avancés"}
            </button>
          </div>

          {showAdvanced ? (
            <div className="space-y-2 border-t border-border/50 pt-2">
              <div className="grid grid-cols-2 gap-2">
                <input
                  value={filterCp}
                  onChange={(e) => setFilterCp(e.target.value)}
                  placeholder="Code postal"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
                <input
                  value={filterCommune}
                  onChange={(e) => setFilterCommune(e.target.value)}
                  placeholder="Commune"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <input
                  value={filterSurfaceMin}
                  onChange={(e) => setFilterSurfaceMin(e.target.value)}
                  placeholder="Surface min m²"
                  type="number"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
                <input
                  value={filterSurfaceMax}
                  onChange={(e) => setFilterSurfaceMax(e.target.value)}
                  placeholder="Surface max m²"
                  type="number"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <input
                  value={filterYearMin}
                  onChange={(e) => setFilterYearMin(e.target.value)}
                  placeholder="Année min"
                  type="number"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
                <input
                  value={filterYearMax}
                  onChange={(e) => setFilterYearMax(e.target.value)}
                  placeholder="Année max"
                  type="number"
                  className="rounded-md border border-input bg-background px-2 py-1 text-xs"
                />
              </div>
              <input
                value={filterProprio}
                onChange={(e) => setFilterProprio(e.target.value)}
                placeholder="Occupant / société (nom)"
                className="w-full rounded-md border border-input bg-background px-2 py-1 text-xs"
              />
            </div>
          ) : null}

          <div className="mt-2 flex items-center gap-2">
            <Button
              size="sm"
              onClick={() => fetchPoints()}
              disabled={loading}
              className="h-7 gap-1.5 text-xs"
            >
              {loading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Search className="h-3 w-3" />}
              Rechercher
            </Button>
            <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {points.length} bâtiments
            </span>
            {stats ? (
              <span
                className={`ml-auto rounded px-1.5 py-0.5 text-[10px] font-semibold ${
                  stats.totalBuildings > 0
                    ? "bg-emerald-100 text-emerald-900"
                    : "bg-red-100 text-red-900"
                }`}
                title={`Turso URL ${stats.db.tursoUrlSet ? "set" : "MISSING"}${stats.db.tursoUrlPrefix ? ` (${stats.db.tursoUrlPrefix}…)` : ""}`}
              >
                DB: {stats.totalBuildings.toLocaleString("fr-FR")}
              </span>
            ) : null}
          </div>
          <p className="mt-1 text-[10px] italic text-muted-foreground">
            La carte ne se met à jour qu'au clic sur Rechercher (pas en bougeant/zoomant).
          </p>
        </div>

        <TertiaireMap
          points={points}
          onBoundsChange={onBoundsChange}
          onSelectBuilding={setSelectedId}
          selectedId={selectedId}
        />

        {error ? (
          <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-900 shadow-md">
            Erreur API /api/tertiaire/list : {error}
          </div>
        ) : null}

        {!error && points.length === 0 && !loading && boundsRef.current.zoom >= ZOOM_MIN ? (
          <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded-lg border border-border bg-card/95 p-3 text-xs text-muted-foreground shadow-md">
            Aucun bâtiment dans cette zone (vérifie la DB Turso, ou déplace/zoome).
          </div>
        ) : null}

        {boundsRef.current.zoom < ZOOM_MIN ? (
          <div className="absolute left-1/2 top-20 z-10 -translate-x-1/2 rounded-lg border border-border bg-card/95 p-3 text-xs text-muted-foreground shadow-md">
            Zoome davantage (zoom ≥ {ZOOM_MIN}) pour afficher les bâtiments.
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

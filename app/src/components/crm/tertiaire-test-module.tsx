"use client";
import { useState } from "react";
import {
  Search,
  Building2,
  Users,
  Zap,
  MapPin,
  AlertTriangle,
  Loader2,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

type LookupResult = {
  query: string;
  geocode: {
    label: string;
    lat: number;
    lon: number;
    postcode?: string;
    city?: string;
    citycode?: string;
  } | null;
  parcelle: {
    idu: string;
    section: string;
    numero: string;
    contenance_m2: number;
  } | null;
  bdnb: {
    batimentGroupeId: string;
    rnbId?: string | null;
    adresse?: string | null;
    surfaceUtileTertiaire?: number | null;
    typeUsage?: string | null;
    dpeTertiaire?: {
      etiquetteDpe?: string | null;
      etiquetteGes?: string | null;
      numeroDpe?: string | null;
    } | null;
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
    adresseEnregistree: string | null;
    estSiege: boolean;
    estActif: boolean;
  }>;
  diagnostics: {
    sourceDpe: "bdnb" | "ademe" | "none";
    bdnbCandidates: number;
    dpeCandidates: number;
    occupantsCount: number;
  };
};

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

export function TertiaireTestModule() {
  const [query, setQuery] = useState("31 rue de Mogador, 75009 Paris");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<LookupResult | null>(null);

  const onSubmit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    const q = query.trim();
    if (q.length < 5) {
      setError("Merci de saisir une adresse complète (min. 5 caractères)");
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch(`/api/tertiaire/lookup?q=${encodeURIComponent(q)}`);
      const json = await res.json();
      if (!res.ok) {
        setError(json?.error ?? `Erreur ${res.status}`);
      } else {
        setResult(json as LookupResult);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mx-auto flex h-full max-w-6xl flex-col gap-6 overflow-auto p-6">
      <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
        <div className="mb-3 flex items-center gap-2">
          <Building2 className="h-5 w-5 text-primary" />
          <h2 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground">
            Recherche bâtiment tertiaire
          </h2>
        </div>
        <form onSubmit={onSubmit} className="flex flex-col gap-2 sm:flex-row">
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Adresse complète (ex: 31 rue de Mogador, 75009 Paris)"
            className="flex-1"
            disabled={loading}
          />
          <Button type="submit" disabled={loading} className="gap-2">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            Rechercher
          </Button>
        </form>
        <p className="mt-2 text-xs text-muted-foreground">
          Chaîne data : BAN → Cadastre IGN → BDNB → DPE tertiaire ADEME → Recherche d'entreprises
        </p>
      </div>

      {error ? (
        <div className="flex items-start gap-3 rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-900">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>
            <p className="font-semibold">Erreur</p>
            <p>{error}</p>
          </div>
        </div>
      ) : null}

      {result ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Adresse + parcelle */}
          <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <MapPin className="h-4 w-4 text-primary" />
              <h3 className="text-sm font-semibold">Adresse & parcelle</h3>
            </div>
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
                    <Row label="Surface parcelle" value={`${result.parcelle.contenance_m2.toLocaleString("fr-FR")} m²`} />
                  </>
                ) : (
                  <p className="pt-2 text-xs text-muted-foreground">Aucune parcelle trouvée à ce point.</p>
                )}
              </dl>
            ) : (
              <p className="text-sm text-muted-foreground">Adresse non géocodée.</p>
            )}
          </section>

          {/* BDNB */}
          <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <Building2 className="h-4 w-4 text-primary" />
              <h3 className="text-sm font-semibold">Fiche bâtiment BDNB</h3>
              <span className="ml-auto text-[10px] uppercase tracking-wider text-muted-foreground">
                {result.diagnostics.bdnbCandidates} candidat(s)
              </span>
            </div>
            {result.bdnb ? (
              <dl className="space-y-1 text-sm">
                <Row label="ID bâtiment groupe" value={result.bdnb.batimentGroupeId} />
                <Row label="RNB" value={result.bdnb.rnbId ?? "—"} />
                <Row label="Adresse" value={result.bdnb.adresse ?? "—"} />
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
            ) : (
              <p className="text-sm text-muted-foreground">
                Aucun bâtiment BDNB trouvé. L'API BDNB Open peut ne pas couvrir cet endroit ou
                avoir atteint son quota mensuel.
              </p>
            )}
          </section>

          {/* DPE Tertiaire */}
          <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <Zap className="h-4 w-4 text-primary" />
              <h3 className="text-sm font-semibold">DPE tertiaire ADEME</h3>
              <span className="ml-auto rounded-full bg-secondary px-2 py-0.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                source: {result.diagnostics.sourceDpe}
              </span>
            </div>
            {result.dpeTertiaire ? (
              <dl className="space-y-1 text-sm">
                <Row label="Numéro DPE" value={result.dpeTertiaire.numero_dpe ?? "—"} />
                <div className="flex items-center gap-2 py-1">
                  <span className="w-44 text-xs text-muted-foreground">Étiquettes</span>
                  <DpeBadge value={String(result.dpeTertiaire.etiquette_dpe ?? "")} />
                  <span className="text-xs text-muted-foreground">DPE</span>
                  <DpeBadge value={String(result.dpeTertiaire.etiquette_ges ?? "")} />
                  <span className="text-xs text-muted-foreground">GES</span>
                </div>
                <Row label="Conso énergie primaire" value={result.dpeTertiaire.conso_kwhep_m2_an ? `${Number(result.dpeTertiaire.conso_kwhep_m2_an).toLocaleString("fr-FR")} kWhEP/m²/an` : "—"} />
                <Row label="Émissions GES" value={result.dpeTertiaire.emission_ges_kgco2_m2_an ? `${Number(result.dpeTertiaire.emission_ges_kgco2_m2_an).toLocaleString("fr-FR")} kgCO₂/m²/an` : "—"} />
                <Row label="Surface utile" value={result.dpeTertiaire.surface_utile ? `${Number(result.dpeTertiaire.surface_utile).toLocaleString("fr-FR")} m²` : "—"} />
                <Row label="Type d'usage" value={result.dpeTertiaire.type_usage_principal ?? "—"} />
                <Row label="Année construction" value={result.dpeTertiaire.annee_construction ?? "—"} />
                <Row label="Date DPE" value={result.dpeTertiaire.date_etablissement_dpe ?? "—"} />
              </dl>
            ) : (
              <p className="text-sm text-muted-foreground">
                Aucun DPE tertiaire trouvé dans un rayon de 80m. {result.diagnostics.dpeCandidates > 0
                  ? `(${result.diagnostics.dpeCandidates} candidat(s) écartés car trop éloignés)`
                  : ""}
              </p>
            )}
          </section>

          {/* Occupants */}
          <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <Users className="h-4 w-4 text-primary" />
              <h3 className="text-sm font-semibold">Sociétés occupantes</h3>
              <span className="ml-auto text-[10px] uppercase tracking-wider text-muted-foreground">
                {result.diagnostics.occupantsCount} actives
              </span>
            </div>
            {result.occupants.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                Aucune société active trouvée à cette adresse via Recherche d'entreprises.
              </p>
            ) : (
              <ul className="space-y-2">
                {result.occupants.slice(0, 12).map((o) => (
                  <li key={o.siret} className="rounded-lg border border-border/50 bg-secondary/30 p-3 text-sm">
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0 flex-1">
                        <p className="truncate font-semibold">{o.denomination ?? "(sans dénomination)"}</p>
                        <p className="truncate text-[11px] text-muted-foreground">
                          SIRET {o.siret}
                          {o.estSiege ? " · siège" : ""}
                          {o.trancheEffectif ? ` · ${o.trancheEffectif}` : ""}
                        </p>
                        {o.nafLabel ? (
                          <p className="truncate text-[11px] text-muted-foreground">
                            {o.nafCode} · {o.nafLabel}
                          </p>
                        ) : null}
                      </div>
                    </div>
                  </li>
                ))}
                {result.occupants.length > 12 ? (
                  <p className="text-xs text-muted-foreground">
                    + {result.occupants.length - 12} autres…
                  </p>
                ) : null}
              </ul>
            )}
            <p className="mt-3 border-t border-border/50 pt-2 text-[11px] italic text-muted-foreground">
              Propriétaire foncier : non disponible (nécessite DV3F / Fichiers fonciers Cerema, sous convention).
            </p>
          </section>
        </div>
      ) : null}
    </div>
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

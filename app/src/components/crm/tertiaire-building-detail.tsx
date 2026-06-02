"use client";
/**
 * Fiche détaillée d'un bâtiment tertiaire sauvegardé (/tertiaire/[id]).
 *
 * Charge depuis /api/tertiaire/[id] et affiche :
 *   - Adresse + coordonnées
 *   - DPE tertiaire (badge + détails)
 *   - Occupants SIRENE avec leurs contacts enrichis
 *   - Section "Recherche contacts à cette adresse"
 *   - Lien vers /dpe?q=adresse pour voir TOUS les DPE ADEME
 */
import { useQuery } from "@tanstack/react-query";
import {
  MapPin, Building2, Phone, Globe, Mail, ExternalLink, Loader2,
  Sparkles, FileText, Users, Zap, ScanSearch,
} from "lucide-react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { jsonFetcher } from "@/lib/fetcher";
import { ExternalContactLinks } from "@/components/address/external-contact-links";

type Building = {
  id: number;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  code_insee_commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  secteur: string | null;
  type_usage: string | null;
  surface_m2: number | null;
  annee_construction: number | null;
};

type Dpe = {
  numero_dpe: string | null;
  etiquette_dpe: string | null;
  etiquette_ges: string | null;
  conso_energie_primaire: number | null;
  conso_energie_finale: number | null;
  emissions_ges: number | null;
  surface_utile: number | null;
  type_usage_dpe: string | null;
  date_etablissement: number | null;
};

type Occupant = {
  id: number;
  siret: string | null;
  siren: string | null;
  denomination: string | null;
  naf_code: string | null;
  naf_label: string | null;
  tranche_effectif: string | null;
  est_siege: number | null;
  phone: string | null;
  website: string | null;
  email: string | null;
  contact_source: string | null;
};

type DetailResp = {
  building: Building;
  dpe: Dpe | null;
  occupants: Occupant[];
  occupantsCount: number;
  prospect: { id: number; stage: string } | null;
};

export function TertiaireBuildingDetail({
  buildingId,
  initialBuilding,
}: {
  buildingId: number;
  initialBuilding: Building;
}) {
  const { data, isPending } = useQuery({
    queryKey: ["tertiary-detail", buildingId],
    queryFn: ({ signal }) =>
      jsonFetcher<DetailResp>(`/api/tertiaire/${buildingId}`, signal),
    staleTime: 60 * 1000,
  });

  const building = data?.building ?? initialBuilding;
  const dpe = data?.dpe ?? null;
  const occupants = data?.occupants ?? [];

  return (
    <div className="space-y-4">
      {/* HEADER */}
      <section className="overflow-hidden rounded-2xl border border-border bg-gradient-to-br from-primary/5 via-card to-secondary/30 shadow-sm">
        <div className="p-6">
          <div className="mb-2 flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
            <Building2 className="h-3 w-3" />
            Bâtiment tertiaire
            {building.secteur ? (
              <span className="rounded bg-primary/10 px-2 py-0.5 text-primary">
                {building.secteur}
              </span>
            ) : null}
          </div>
          <h1 className="flex items-center gap-2 text-xl font-black tracking-tight">
            <MapPin className="h-5 w-5 text-primary" />
            {building.adresse ?? `Bâtiment #${building.id}`}
          </h1>
          <p className="text-sm text-muted-foreground">
            {building.code_postal} {building.commune}
            {building.lat != null && building.lon != null
              ? ` · ${building.lat.toFixed(5)}, ${building.lon.toFixed(5)}`
              : ""}
          </p>
        </div>
      </section>

      {/* DPE TERTIAIRE + BÂTIMENT */}
      <section className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Zap className="h-4 w-4 text-primary" />
            DPE tertiaire
          </h2>
          {isPending ? (
            <Skeleton className="h-24 w-full" />
          ) : dpe?.numero_dpe ? (
            <div className="space-y-2">
              <div className="flex items-center gap-3">
                <DpeBadge classe={dpe.etiquette_dpe ?? "NC"} size="lg" />
                <span className="text-sm text-muted-foreground">DPE</span>
                <DpeBadge classe={dpe.etiquette_ges ?? "NC"} size="lg" />
                <span className="text-sm text-muted-foreground">GES</span>
              </div>
              <dl className="space-y-1 text-xs">
                <KvRow label="Conso EP" v={dpe.conso_energie_primaire != null ? `${dpe.conso_energie_primaire.toFixed(0)} kWhEP/m²/an` : null} />
                <KvRow label="Conso EF" v={dpe.conso_energie_finale != null ? `${dpe.conso_energie_finale.toFixed(0)} kWhEF/m²/an` : null} />
                <KvRow label="Émissions GES" v={dpe.emissions_ges != null ? `${dpe.emissions_ges.toFixed(0)} kgCO₂/m²/an` : null} />
                <KvRow label="Surface DPE" v={dpe.surface_utile ? `${dpe.surface_utile.toFixed(0)} m²` : null} />
                <KvRow label="Type usage" v={dpe.type_usage_dpe} />
                <KvRow label="N° DPE" v={dpe.numero_dpe} mono />
              </dl>
              <Link
                href={`/dpe/${encodeURIComponent(dpe.numero_dpe)}`}
                className="mt-2 inline-flex items-center gap-1 text-xs font-semibold text-primary hover:underline"
              >
                Voir la fiche DPE complète <ExternalLink className="h-3 w-3" />
              </Link>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              Aucun DPE tertiaire enregistré pour ce bâtiment.
            </p>
          )}
        </div>

        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Building2 className="h-4 w-4 text-primary" />
            Bâtiment
          </h2>
          <dl className="space-y-1.5 text-xs">
            <KvRow label="Secteur" v={building.secteur} />
            <KvRow label="Type d'usage" v={building.type_usage} />
            <KvRow label="Surface" v={building.surface_m2 ? `${building.surface_m2.toFixed(0)} m²` : null} />
            <KvRow label="Année construction" v={building.annee_construction} />
            <KvRow label="Code postal" v={building.code_postal} />
            <KvRow label="Commune" v={building.commune} />
            <KvRow label="Département" v={building.departement} />
            <KvRow label="Code INSEE" v={building.code_insee_commune} mono />
          </dl>
        </div>
      </section>

      {/* OCCUPANTS SIRENE */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <Users className="h-4 w-4 text-primary" />
          Occupants ({occupants.length})
        </h2>
        {isPending ? (
          <Skeleton className="h-32 w-full" />
        ) : occupants.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            Aucun occupant enregistré. Cliquez sur &quot;Rafraîchir&quot; depuis la
            carte pour interroger SIRENE.
          </p>
        ) : (
          <ul className="space-y-1.5">
            {occupants.slice(0, 50).map((o) => (
              <OccupantRow key={o.id} occupant={o} />
            ))}
          </ul>
        )}
      </section>

      {/* RECHERCHE CONTACTS À L'ADRESSE */}
      {building.adresse ? (
        <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Sparkles className="h-4 w-4 text-primary" />
            Rechercher des contacts à cette adresse
          </h2>
          <ExternalContactLinks
            address={building.adresse}
            cp={building.code_postal}
            city={building.commune}
          />
        </section>
      ) : null}

      {/* LIEN VERS /dpe pour TOUS les DPE ADEME */}
      {building.adresse ? (
        <section className="rounded-xl border border-primary/30 bg-primary/5 p-4 shadow-sm">
          <Link
            href={`/dpe?q=${encodeURIComponent(`${building.adresse} ${building.code_postal ?? ""} ${building.commune ?? ""}`.trim())}`}
            className="flex items-center justify-between gap-3 text-sm font-semibold text-primary hover:underline"
          >
            <span className="flex items-center gap-2">
              <ScanSearch className="h-4 w-4" />
              Voir TOUS les DPE ADEME à cette adresse (résidentiel + tertiaire)
            </span>
            <ExternalLink className="h-4 w-4" />
          </Link>
        </section>
      ) : null}
    </div>
  );
}

function OccupantRow({ occupant: o }: { occupant: Occupant }) {
  const hasContact = Boolean(o.phone || o.website || o.email);
  return (
    <li className="rounded-md border border-border bg-secondary/30 p-2 text-xs">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="truncate font-semibold">
            {o.denomination ?? "(sans dénomination)"}
            {o.est_siege ? (
              <span className="ml-1 rounded bg-primary/15 px-1 text-[9px] uppercase text-primary">
                Siège
              </span>
            ) : null}
          </p>
          <p className="truncate text-[10px] text-muted-foreground">
            SIRET {o.siret ?? "—"}
            {o.naf_code ? ` · NAF ${o.naf_code}` : ""}
            {o.naf_label ? ` · ${o.naf_label}` : ""}
            {o.tranche_effectif ? ` · eff. ${o.tranche_effectif}` : ""}
          </p>
          {hasContact ? (
            <div className="mt-1 flex flex-wrap gap-2 text-[11px]">
              {o.phone ? (
                <a href={`tel:${o.phone}`} className="flex items-center gap-1 text-primary hover:underline">
                  <Phone className="h-3 w-3" /> {o.phone}
                </a>
              ) : null}
              {o.website ? (
                <a href={o.website.startsWith("http") ? o.website : `https://${o.website}`} target="_blank" rel="noreferrer" className="flex items-center gap-1 text-primary hover:underline">
                  <Globe className="h-3 w-3" /> Site
                </a>
              ) : null}
              {o.email ? (
                <a href={`mailto:${o.email}`} className="flex items-center gap-1 text-primary hover:underline">
                  <Mail className="h-3 w-3" /> {o.email}
                </a>
              ) : null}
              {o.contact_source ? (
                <span className="rounded bg-secondary/60 px-1 text-[9px] uppercase text-muted-foreground">
                  via {o.contact_source}
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
        {o.siren ? (
          <a
            href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${o.siren}`}
            target="_blank"
            rel="noreferrer"
            className="text-muted-foreground hover:text-primary"
            title="Annuaire Entreprises"
          >
            <ExternalLink className="h-3 w-3" />
          </a>
        ) : null}
      </div>
    </li>
  );
}

function KvRow({ label, v, mono }: { label: string; v: unknown; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-border/40 py-0.5 last:border-b-0">
      <span className="text-muted-foreground">{label}</span>
      <span className={`text-right text-foreground ${mono ? "font-mono text-[10px]" : "font-medium"}`}>
        {v != null && v !== "" ? String(v) : "—"}
      </span>
    </div>
  );
}

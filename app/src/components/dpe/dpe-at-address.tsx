"use client";
/**
 * Widget de recherche DPE par adresse — reflet exhaustif de l'ADEME.
 *
 * Affiche distinctement les 4 catégories ADEME canoniques :
 *   - 🏢 DPE collectif RÉEL d'immeuble (methode_application_dpe="dpe immeuble collectif")
 *   - 🏠 DPE individuel d'appartement (réel diagnostic)
 *   - 📋 DPE d'appartement dérivé d'un DPE immeuble
 *   - 🏡 DPE maison individuelle
 *
 * Utilisé sur /dpe (page recherche) et embeddable sur la fiche copro,
 * fiche tertiaire, etc.
 */
import { useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
  Loader2, MapPin, Building2, Home, FileText, Info,
  AlertTriangle, CheckCircle2, Sparkles, Users2, Briefcase,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { useDebouncedValue } from "@/lib/hooks/use-debounced-value";
import { jsonFetcher } from "@/lib/fetcher";
import { AddressAutocomplete } from "@/components/address/address-autocomplete";
import { ExternalContactLinks } from "@/components/address/external-contact-links";

type DpeKind =
  | "collectif_reel"
  | "appartement_individuel"
  | "appartement_derive_immeuble"
  | "maison_individuelle"
  | "tertiaire"
  | "autre";

type DpeItem = {
  kind: DpeKind;
  numero_dpe: string | null;
  numero_dpe_immeuble: string | null;
  etiquette_dpe: string | null;
  etiquette_ges: string | null;
  date_etablissement: string | null;
  type_batiment: string | null;
  methode_application_dpe: string | null;
  numero_voie_ban: string | null;
  nom_rue_ban: string | null;
  code_postal_ban: string | null;
  nom_commune_ban: string | null;
  surface_habitable: number | null;
  conso_5_usages_par_m2_ep: number | null;
};

type Result = {
  banResolved: {
    label: string;
    lat: number;
    lon: number;
    score: number;
  } | null;
  parcelle: { idu: string } | null;
  rayonMetres: number;
  totalAdeme: number;
  matchedCount: number;
  collectifsReels: DpeItem[];
  appartementsIndividuels: DpeItem[];
  appartementsDerivesImmeuble: DpeItem[];
  maisonsIndividuelles: DpeItem[];
  tertiaires: DpeItem[];
  autres: DpeItem[];
  notes: string[];
};

export function DpeAtAddress({
  initialQuery,
  autoSearch = true,
}: {
  initialQuery?: string;
  autoSearch?: boolean;
}) {
  const [input, setInput] = useState(initialQuery ?? "");
  const [active, setActive] = useState(autoSearch ? (initialQuery ?? "") : "");
  const debounced = useDebouncedValue(active, 300);

  const { data, isFetching, isPending, error } = useQuery({
    queryKey: ["dpe-at-address", debounced],
    queryFn: ({ signal }) =>
      jsonFetcher<Result>(
        `/api/dpe/at-address?q=${encodeURIComponent(debounced)}`,
        signal,
      ),
    enabled: debounced.length >= 5,
    staleTime: 5 * 60 * 1000,
  });

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setActive(input.trim());
  };

  return (
    <div className="space-y-4">
      <form
        onSubmit={onSubmit}
        className="rounded-xl border border-border bg-card p-3 shadow-sm"
      >
        <div className="flex items-center gap-2">
          <div className="flex-1">
            <AddressAutocomplete
              value={input}
              onChange={setInput}
              onSelect={(s) => setActive(s.label)}
              placeholder="Adresse (ex: 2 avenue lenine romainville)"
            />
          </div>
          <Button type="submit" size="sm" disabled={input.trim().length < 5}>
            Chercher
          </Button>
        </div>
        <p className="mt-1.5 px-1 text-[10px] text-muted-foreground">
          Source : ADEME (data.gouv.fr) — base nationale des DPE existants
          + BAN pour le géocodage.
        </p>
      </form>

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-900">
          Erreur : {(error as Error).message}
        </div>
      ) : null}

      {active.length >= 5 && isPending ? (
        <div className="space-y-2">
          <Skeleton className="h-14 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      ) : null}

      {data && active.length >= 5 ? (
        <ResultsView data={data} fetching={isFetching} />
      ) : null}
    </div>
  );
}

function ResultsView({ data, fetching }: { data: Result; fetching: boolean }) {
  if (!data.banResolved) {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
        <AlertTriangle className="mr-1 inline h-3.5 w-3.5" />
        {data.notes[0] ?? "Adresse non résolue par la BAN."}
      </div>
    );
  }

  const noResults = data.matchedCount === 0;
  const resolvedLabel = data.banResolved.label;
  // Extract CP + city pour les liens externes (parse simple du label BAN)
  const parsed = resolvedLabel.match(/(\d{5})\s+(.+)$/);
  const cp = parsed?.[1] ?? null;
  const city = parsed?.[2] ?? null;
  const streetOnly = parsed
    ? resolvedLabel.replace(parsed[0], "").trim()
    : resolvedLabel;

  return (
    <div className="space-y-4">
      {/* Bandeau résolution adresse */}
      <div className="flex items-start gap-2 rounded-lg border border-border bg-card p-3 text-xs">
        <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-primary" />
        <div className="flex-1">
          <p className="font-semibold">{data.banResolved.label}</p>
          <p className="text-muted-foreground">
            BAN score {Math.round(data.banResolved.score * 100)} %
            {data.parcelle ? ` · Parcelle ${data.parcelle.idu}` : ""}
            {" · "}rayon {data.rayonMetres} m
            {" · "}
            {data.matchedCount}/{data.totalAdeme} DPE
            {fetching ? <Loader2 className="ml-1 inline h-3 w-3 animate-spin" /> : null}
          </p>
        </div>
      </div>

      {noResults ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-xs text-amber-900">
          <p className="font-semibold">Aucun DPE à cette adresse exacte.</p>
          <p className="mt-1 opacity-80">
            ADEME a {data.totalAdeme} DPE dans un rayon de {data.rayonMetres} m
            autour du point BAN, mais aucun ne correspond strictement au CP +
            commune + nom de rue + numéro recherchés. Vérifiez l&apos;orthographe
            ou tentez une variante.
          </p>
        </div>
      ) : null}

      {/* Section 1 : DPE collectif RÉEL */}
      {data.collectifsReels.length > 0 ? (
        <Section
          title="DPE collectif d'immeuble — RÉEL ADEME"
          subtitle={`${data.collectifsReels.length} DPE collectif(s) trouvé(s) à cette adresse`}
          icon={Building2}
          accent="emerald"
          highlight={
            <span className="inline-flex items-center gap-1 rounded-full bg-emerald-600 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-white">
              <CheckCircle2 className="h-3 w-3" /> Valeur officielle
            </span>
          }
        >
          <p className="mb-2 text-[11px] text-emerald-900">
            Ces DPE sont des diagnostics <strong>collectifs réalisés sur l&apos;immeuble entier</strong> (méthode ADEME &quot;dpe immeuble collectif&quot;). C&apos;est la valeur de référence — ne pas la confondre avec une estimation simulée depuis des DPE individuels.
          </p>
          {data.collectifsReels.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section 2 : appartements individuels */}
      {data.appartementsIndividuels.length > 0 ? (
        <Section
          title="DPE individuels d'appartement"
          subtitle={`${data.appartementsIndividuels.length} appartement(s) avec DPE individuel réel`}
          icon={Home}
          accent="primary"
        >
          {data.appartementsIndividuels.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section 3 : appartements dérivés */}
      {data.appartementsDerivesImmeuble.length > 0 ? (
        <Section
          title="DPE d'appartement dérivés d'un DPE immeuble"
          subtitle={`${data.appartementsDerivesImmeuble.length} appartement(s) — issu d'un DPE collectif parent`}
          icon={FileText}
          accent="slate"
        >
          <p className="mb-2 flex items-start gap-1 text-[11px] text-muted-foreground">
            <Info className="mt-0.5 h-3 w-3 shrink-0" />
            Ces DPE ne sont pas des diagnostics individuels réalisés sur place, mais des dérivations automatiques du DPE collectif de l&apos;immeuble parent.
          </p>
          {data.appartementsDerivesImmeuble.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section 4 : maisons individuelles */}
      {data.maisonsIndividuelles.length > 0 ? (
        <Section
          title="DPE de maison individuelle"
          subtitle={`${data.maisonsIndividuelles.length} maison(s) à cette adresse`}
          icon={Home}
          accent="primary"
        >
          {data.maisonsIndividuelles.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section 5 : DPE TERTIAIRE (dataset différent ADEME) */}
      {data.tertiaires.length > 0 ? (
        <Section
          title="DPE tertiaire (bureaux, commerces, hôtels, etc.)"
          subtitle={`${data.tertiaires.length} DPE tertiaire(s) trouvé(s) à cette adresse — dataset dpe-tertiaire ADEME`}
          icon={Briefcase}
          accent="primary"
        >
          {data.tertiaires.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section 6 : autres */}
      {data.autres.length > 0 ? (
        <Section
          title="Autres DPE"
          subtitle="Type ADEME inhabituel"
          icon={Sparkles}
          accent="slate"
        >
          {data.autres.map((it) => (
            <DpeRow key={it.numero_dpe ?? Math.random()} item={it} />
          ))}
        </Section>
      ) : null}

      {/* Section : liens directs vers les annuaires officiels */}
      <Section
        title="Rechercher des contacts à cette adresse"
        subtitle="Liens vers les annuaires publics — tu consultes directement chaque source"
        icon={Users2}
        accent="slate"
      >
        <ExternalContactLinks
          address={streetOnly}
          cp={cp}
          city={city}
        />
      </Section>

      {/* Notes diagnostiques */}
      {data.notes.length > 0 ? (
        <details className="rounded-lg border border-border bg-secondary/30 p-2 text-[11px] text-muted-foreground">
          <summary className="cursor-pointer font-semibold">
            Notes diagnostiques ({data.notes.length})
          </summary>
          <ul className="mt-2 space-y-0.5 pl-4">
            {data.notes.map((n, i) => (
              <li key={i} className="list-disc">{n}</li>
            ))}
          </ul>
        </details>
      ) : null}
    </div>
  );
}

function Section({
  title,
  subtitle,
  icon: Icon,
  accent,
  highlight,
  children,
}: {
  title: string;
  subtitle: string;
  icon: React.ComponentType<{ className?: string }>;
  accent: "emerald" | "primary" | "slate";
  highlight?: React.ReactNode;
  children: React.ReactNode;
}) {
  const borderClass =
    accent === "emerald"
      ? "border-emerald-300 bg-emerald-50/40"
      : accent === "primary"
        ? "border-primary/30 bg-primary/5"
        : "border-border bg-card";
  const iconColor =
    accent === "emerald"
      ? "text-emerald-700"
      : accent === "primary"
        ? "text-primary"
        : "text-muted-foreground";
  return (
    <section className={`rounded-xl border ${borderClass} p-4 shadow-sm`}>
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="flex items-start gap-2">
          <Icon className={`mt-0.5 h-4 w-4 shrink-0 ${iconColor}`} />
          <div>
            <h3 className="text-sm font-semibold">{title}</h3>
            <p className="text-[11px] text-muted-foreground">{subtitle}</p>
          </div>
        </div>
        {highlight}
      </div>
      <div className="space-y-1.5">{children}</div>
    </section>
  );
}

function DpeRow({ item }: { item: DpeItem }) {
  const addr = [
    item.numero_voie_ban,
    item.nom_rue_ban,
    item.code_postal_ban,
    item.nom_commune_ban,
  ]
    .filter(Boolean)
    .join(" ");
  const date = item.date_etablissement
    ? new Date(item.date_etablissement).toLocaleDateString("fr-FR")
    : "—";
  const content = (
    <>
      <DpeBadge classe={item.etiquette_dpe ?? "NC"} />
      <div className="min-w-0 flex-1">
        <p className="truncate font-medium">{addr || "Adresse non précisée"}</p>
        <p className="text-[10px] text-muted-foreground">
          DPE {item.numero_dpe ?? "—"} · {date}
          {item.surface_habitable ? ` · ${item.surface_habitable.toFixed(0)} m²` : ""}
          {item.conso_5_usages_par_m2_ep
            ? ` · ${item.conso_5_usages_par_m2_ep.toFixed(0)} kWh/m²/an`
            : ""}
        </p>
      </div>
      {item.etiquette_ges ? (
        <div className="shrink-0 text-[10px] text-muted-foreground">
          GES <strong className="text-foreground">{item.etiquette_ges}</strong>
        </div>
      ) : null}
      {item.numero_dpe ? (
        <span className="ml-1 shrink-0 text-[10px] font-bold text-primary">
          Détail →
        </span>
      ) : null}
    </>
  );
  if (item.numero_dpe) {
    return (
      <Link
        href={`/dpe/${encodeURIComponent(item.numero_dpe)}`}
        className="flex items-center gap-3 rounded-md border border-border bg-card p-2 text-xs transition-colors hover:border-primary/40 hover:bg-primary/5"
      >
        {content}
      </Link>
    );
  }
  return (
    <div className="flex items-center gap-3 rounded-md border border-border bg-card p-2 text-xs">
      {content}
    </div>
  );
}

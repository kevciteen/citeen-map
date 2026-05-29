"use client";
/**
 * Liens externes "Rechercher des contacts à cette adresse".
 *
 * Approche legal-friendly : on génère des deep-links vers les moteurs de
 * recherche publics avec l'adresse pré-remplie. L'utilisateur consulte
 * directement le site source. Pas de scraping → pas de violation ToS,
 * pas de stockage de données personnelles RGPD.
 *
 * Sources :
 *  - Pages Blanches (particuliers, nom + tél)
 *  - Pages Jaunes (commerçants, pros)
 *  - Annuaire Inversé (téléphone → nom et adresse)
 *  - Annuaire Entreprises (SIRET, SIREN)
 *  - Google Maps (vue satellite + Street View)
 *  - Cadastre (parcelles + occupants pro)
 *  - LinkedIn (recherche sur une adresse)
 *
 * Utilisé sur les pages adresse/copro/maison/appartement/tertiaire.
 */
import {
  Phone, BookOpen, Briefcase, Map as MapIcon, ExternalLink,
  Building, Linkedin, Search,
} from "lucide-react";

type LinkDef = {
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  href: string;
  note?: string;
};

function buildLinks(opts: {
  address: string;
  cp?: string | null;
  city?: string | null;
}): LinkDef[] {
  const adresseFull = [opts.address, opts.cp, opts.city].filter(Boolean).join(" ");
  const cityQ = opts.city ? opts.city : "";
  const enc = encodeURIComponent;

  return [
    {
      label: "Pages Blanches",
      icon: BookOpen,
      // PB nécessite un nom dans quoiqui, sinon page d'accueil vide
      href: `https://www.pagesjaunes.fr/pagesblanches/recherche?quoiqui=particulier&ou=${enc(adresseFull)}`,
      note: "Particuliers — saisir un nom dans la barre",
    },
    {
      label: "Pages Jaunes",
      icon: Briefcase,
      // PJ exige quoiqui non-vide sinon 404 / formulaire vide
      href: `https://www.pagesjaunes.fr/recherche/?quoiqui=entreprise&ou=${enc(adresseFull)}`,
      note: "Entreprises présentes à l'adresse",
    },
    {
      label: "Annuaire Inversé",
      icon: Phone,
      href: `https://annuaireinverse.118712.fr/${enc(cityQ)}`,
      note: "Téléphone → nom & adresse",
    },
    {
      label: "Annuaire Entreprises",
      icon: Building,
      href: `https://annuaire-entreprises.data.gouv.fr/rechercher?terme=${enc(adresseFull)}`,
      note: "Sirene + dirigeants (data.gouv.fr)",
    },
    {
      label: "Google Maps",
      icon: MapIcon,
      href: `https://www.google.com/maps/search/${enc(adresseFull)}`,
      note: "Vue satellite + Street View",
    },
    {
      label: "Cadastre",
      icon: MapIcon,
      href: `https://www.cadastre.gouv.fr/scpc/rechercherPlan.do?numeroVoie=&indiceRepetition=&nomVoie=${enc(opts.address)}&lieuDit=&codePostal=${enc(opts.cp ?? "")}&ville=${enc(cityQ)}`,
      note: "Parcelles + références cadastrales",
    },
    {
      label: "LinkedIn",
      icon: Linkedin,
      href: `https://www.google.com/search?q=site%3Alinkedin.com+%22${enc(adresseFull)}%22`,
      note: "Profils à cette adresse (via Google)",
    },
    {
      label: "Recherche Google",
      icon: Search,
      href: `https://www.google.com/search?q=${enc(`"${adresseFull}" contact OR téléphone OR email`)}`,
      note: "Toutes sources — résultats organiques",
    },
  ];
}

export function ExternalContactLinks({
  address,
  cp,
  city,
  compact = false,
}: {
  address: string;
  cp?: string | null;
  city?: string | null;
  compact?: boolean;
}) {
  if (!address || address.trim().length < 3) return null;
  const links = buildLinks({ address, cp, city });

  if (compact) {
    return (
      <div className="flex flex-wrap gap-1.5">
        {links.map((l) => {
          const Icon = l.icon;
          return (
            <a
              key={l.label}
              href={l.href}
              target="_blank"
              rel="noopener noreferrer"
              title={l.note}
              className="inline-flex items-center gap-1 rounded-md border border-border bg-card px-2 py-1 text-[11px] font-medium text-foreground hover:bg-secondary"
            >
              <Icon className="h-3 w-3 text-muted-foreground" />
              {l.label}
              <ExternalLink className="h-2.5 w-2.5 text-muted-foreground" />
            </a>
          );
        })}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <p className="text-[11px] text-muted-foreground">
        Cherche des contacts (occupants, commerces, professionnels) à cette
        adresse sur des sources publiques. Tu consultes directement le site
        — aucune donnée n&apos;est stockée par Citeen.
      </p>
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {links.map((l) => {
          const Icon = l.icon;
          return (
            <a
              key={l.label}
              href={l.href}
              target="_blank"
              rel="noopener noreferrer"
              className="group flex items-start gap-2 rounded-lg border border-border bg-card p-2.5 transition-colors hover:border-primary/30 hover:bg-primary/5"
            >
              <div className="rounded-md bg-secondary/60 p-1.5 text-primary group-hover:bg-primary/20">
                <Icon className="h-3.5 w-3.5" />
              </div>
              <div className="min-w-0 flex-1">
                <p className="flex items-center gap-1 text-xs font-semibold">
                  {l.label}
                  <ExternalLink className="h-2.5 w-2.5 text-muted-foreground" />
                </p>
                {l.note ? (
                  <p className="text-[10px] text-muted-foreground">{l.note}</p>
                ) : null}
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
}

"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  Copy,
  ExternalLink,
  IdCard,
  Loader2,
  Mail,
  MapPin,
  Search,
  Users,
} from "lucide-react";
import { toast } from "sonner";

function slugify(name: string): string {
  return name
    .trim()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

type SyndicContact = {
  source: string;
  nomComplet: string;
  siren: string;
  siretSiege: string | null;
  adresse: string | null;
  codePostal: string | null;
  commune: string | null;
  departement: string | null;
  codeApe: string | null;
  libelleApe: string | null;
  dirigeant: string | null;
  trancheEffectif: string | null;
  matchScore: number;
};

export function SyndicContactCard({
  syndicName,
  coproContext,
}: {
  syndicName: string;
  coproContext?: { nom: string | null; adresse: string | null };
}) {
  const [contact, setContact] = useState<SyndicContact | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!syndicName || syndicName.trim().length < 3) return;
    let aborted = false;
    setLoading(true);
    setError(null);
    setContact(null);
    fetch(`/api/syndics/lookup?name=${encodeURIComponent(syndicName)}`)
      .then((r) => r.json())
      .then((j: { contact: SyndicContact | null; error?: string }) => {
        if (aborted) return;
        if (j.contact) setContact(j.contact);
        else setError(j.error ?? "Pas de match trouvé");
      })
      .catch((e) => !aborted && setError((e as Error).message))
      .finally(() => !aborted && setLoading(false));
    return () => {
      aborted = true;
    };
  }, [syndicName]);

  const copy = (label: string, text: string) => {
    void navigator.clipboard.writeText(text);
    toast.success(`${label} copié`);
  };

  const fullAddress = contact
    ? [contact.adresse, contact.codePostal, contact.commune]
        .filter(Boolean)
        .join(" ")
    : "";

  const mailtoBody = encodeURIComponent(
    `Bonjour,\n\n` +
      `Nous accompagnons les copropriétés ${
        coproContext?.adresse ? `de ${coproContext.adresse} ` : ""
      }dans leur projet de rénovation énergétique (Audit + MaPrimeRénov' Copro + financement).\n\n` +
      `${
        coproContext?.nom
          ? `Concernant la copro ${coproContext.nom}, n`
          : "N"
      }ous aimerions échanger sur leurs travaux de rénovation à venir.\n\n` +
      `Cordialement,\n` +
      `[Votre nom]`,
  );
  const mailtoSubject = encodeURIComponent(
    `Rénovation énergétique copropriété${
      coproContext?.nom ? ` — ${coproContext.nom}` : ""
    }`,
  );

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Recherche des coordonnées du syndic via Sirene…
        </div>
      </div>
    );
  }

  if (error || !contact) {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
        <p className="font-semibold">Pas de fiche entreprise trouvée</p>
        <p className="mt-1 opacity-80">
          {error ?? "Le nom du syndic ne match aucune entreprise active."}
        </p>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <Link
            href={`/syndics/${slugify(syndicName)}?name=${encodeURIComponent(syndicName)}`}
            className="inline-flex items-center gap-1 rounded-md border border-amber-300 bg-white px-2 py-1 text-[11px] font-medium text-amber-900 hover:bg-amber-100"
          >
            <IdCard className="h-3 w-3" /> Ouvrir la fiche
          </Link>
          <a
            href={`https://www.google.com/search?q=${encodeURIComponent(
              `${syndicName} syndic adresse contact`,
            )}`}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 font-medium text-amber-800 underline hover:text-amber-700"
          >
            <Search className="h-3 w-3" /> Rechercher sur Google
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2 rounded-lg border border-emerald-200 bg-emerald-50/50 p-3">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-bold text-emerald-900">
            {contact.nomComplet}
          </p>
          <p className="font-mono text-[10px] text-emerald-700">
            SIREN {contact.siren} · {contact.codeApe}
            {contact.libelleApe ? ` · ${contact.libelleApe}` : ""}
          </p>
        </div>
        <span
          className="shrink-0 rounded-full bg-emerald-200 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider text-emerald-900"
          title={`Score de match heuristique ${Math.round(contact.matchScore * 100)}%`}
        >
          {Math.round(contact.matchScore * 100)}%
        </span>
      </div>

      {fullAddress ? (
        <div className="flex items-start gap-2 text-xs text-emerald-950">
          <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-700" />
          <span className="flex-1 leading-snug">{fullAddress}</span>
          <button
            onClick={() => copy("Adresse", fullAddress)}
            className="shrink-0 rounded p-1 text-emerald-700 hover:bg-emerald-100"
            title="Copier l'adresse"
          >
            <Copy className="h-3 w-3" />
          </button>
        </div>
      ) : null}

      {contact.dirigeant ? (
        <div className="flex items-center gap-2 text-xs text-emerald-950">
          <Users className="h-3.5 w-3.5 text-emerald-700" />
          <span>{contact.dirigeant}</span>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-1.5 pt-1">
        <Link
          href={`/syndics/${slugify(syndicName)}?name=${encodeURIComponent(syndicName)}`}
          className="flex items-center gap-1 rounded-md bg-emerald-700 px-2 py-1 text-[11px] font-bold text-white hover:bg-emerald-800"
        >
          <IdCard className="h-3 w-3" /> Ouvrir la fiche
        </Link>
        <a
          href={`mailto:?subject=${mailtoSubject}&body=${mailtoBody}`}
          className="flex items-center gap-1 rounded-md border border-emerald-300 bg-white px-2 py-1 text-[11px] font-medium text-emerald-900 hover:bg-emerald-100"
        >
          <Mail className="h-3 w-3" /> Composer un email
        </a>
        <a
          href={`https://www.google.com/search?q=${encodeURIComponent(
            `${contact.nomComplet} email contact`,
          )}`}
          target="_blank"
          rel="noreferrer"
          className="flex items-center gap-1 rounded-md border border-emerald-300 bg-white px-2 py-1 text-[11px] font-medium text-emerald-900 hover:bg-emerald-100"
        >
          <Search className="h-3 w-3" /> Trouver l&apos;email
        </a>
        {contact.siren ? (
          <a
            href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${contact.siren}`}
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1 rounded-md border border-emerald-300 bg-white px-2 py-1 text-[11px] font-medium text-emerald-900 hover:bg-emerald-100"
            title="Fiche officielle annuaire-entreprises"
          >
            <ExternalLink className="h-3 w-3" /> Annuaire
          </a>
        ) : null}
      </div>
    </div>
  );
}

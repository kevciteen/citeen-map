"use client";
/**
 * Bloc "Contacts syndic" sur la fiche copro.
 *
 * Affiche la vision complète du syndic : Sirene (siège social), champs
 * édités manuellement, et le bloc auto-enrichi (téléphone/site/mail
 * via OSM + Google Places, étape 3 du chantier coordonnées).
 *
 * Bouton "Enrichir auto" déclenche POST /api/syndics/[slug]/enrich-contacts
 * et rafraîchit la fiche.
 */
import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import {
  Loader2, Mail, Phone, Globe, MapPin, Users, IdCard, RefreshCw,
  Copy, ExternalLink, Sparkles,
} from "lucide-react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

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

type SyndicDetail = {
  slug: string;
  name: string;
  sirene: {
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
    matchScore: number;
  } | null;
  editable: {
    email: string | null;
    phone: string | null;
    contact_person: string | null;
    website: string | null;
    address_override: string | null;
    notes: string | null;
  };
  auto: {
    phone: string | null;
    website: string | null;
    email: string | null;
    hours: string | null;
    source: string | null;
    fetched_at: number | null;
  };
};

export function CoproSyndicContacts({
  syndicName,
}: {
  syndicName: string | null;
}) {
  const [detail, setDetail] = useState<SyndicDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [enriching, setEnriching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const slug = syndicName ? slugify(syndicName) : null;

  const load = useCallback(async () => {
    if (!syndicName || !slug) return;
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(
        `/api/syndics/${slug}?name=${encodeURIComponent(syndicName)}`,
      );
      const j = await r.json();
      if (!r.ok) {
        setError(j.error ?? "Syndic introuvable");
        setDetail(null);
        return;
      }
      setDetail(j);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [slug, syndicName]);

  useEffect(() => {
    void load();
  }, [load]);

  const enrich = async () => {
    if (!slug || !syndicName) return;
    setEnriching(true);
    try {
      const r = await fetch(`/api/syndics/${slug}/enrich-contacts`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: syndicName }),
      });
      const j = await r.json();
      if (!r.ok) {
        toast.error(j.error ?? "Erreur enrichissement");
        return;
      }
      if (j.resolved) {
        toast.success(`Enrichi via ${j.source.toUpperCase()}`);
      } else {
        toast.warning(j.reason ?? "Aucun contact trouvé");
      }
      await load();
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setEnriching(false);
    }
  };

  if (!syndicName) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Contacts syndic</CardTitle>
        </CardHeader>
        <CardContent className="text-xs text-muted-foreground">
          Aucun syndic référencé pour cette copropriété.
        </CardContent>
      </Card>
    );
  }

  // Manuel prioritaire, auto en fallback
  const phone = detail?.editable.phone || detail?.auto.phone;
  const phoneSrc = detail?.editable.phone ? "manuel" : detail?.auto.source;
  const email = detail?.editable.email || detail?.auto.email;
  const emailSrc = detail?.editable.email ? "manuel" : detail?.auto.source;
  const website = detail?.editable.website || detail?.auto.website;
  const websiteSrc = detail?.editable.website ? "manuel" : detail?.auto.source;
  const hasAnyContact = phone || email || website;
  const lastEnriched = detail?.auto.fetched_at
    ? new Date(detail.auto.fetched_at * 1000)
    : null;

  return (
    <Card className="print:shadow-none">
      <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0">
        <CardTitle className="text-sm">Contacts syndic</CardTitle>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={enrich}
            disabled={enriching || loading}
            title="Récupère téléphone / site web via OSM + Google Places (cache 7j)"
          >
            {enriching ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Sparkles className="h-3.5 w-3.5" />
            )}
            {hasAnyContact ? "Ré-enrichir" : "Enrichir auto"}
          </Button>
          {slug ? (
            <Link
              href={`/syndics/${slug}?name=${encodeURIComponent(syndicName)}`}
              className="inline-flex items-center gap-1 rounded-md border border-border bg-secondary/40 px-2 py-1 text-[11px] font-medium text-foreground hover:bg-secondary/70"
            >
              <IdCard className="h-3 w-3" /> Fiche complète
            </Link>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="space-y-3 text-xs">
        {loading && !detail ? (
          <div className="flex items-center gap-2 text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            Chargement…
          </div>
        ) : null}

        {error ? (
          <p className="rounded-md border border-amber-200 bg-amber-50 p-2 text-amber-900">
            {error}
          </p>
        ) : null}

        {/* Identité Sirene */}
        {detail?.sirene ? (
          <div className="space-y-1 rounded-md border border-border bg-secondary/30 p-2">
            <p className="font-semibold text-foreground">
              {detail.sirene.nomComplet}
              <span className="ml-2 rounded-full bg-primary/15 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider text-primary">
                {Math.round(detail.sirene.matchScore * 100)}% match
              </span>
            </p>
            <p className="font-mono text-[10px] text-muted-foreground">
              SIREN {detail.sirene.siren}
              {detail.sirene.codeApe ? ` · NAF ${detail.sirene.codeApe}` : ""}
              {detail.sirene.libelleApe ? ` · ${detail.sirene.libelleApe}` : ""}
            </p>
            {detail.sirene.adresse ? (
              <p className="flex items-start gap-1 text-foreground/80">
                <MapPin className="mt-0.5 h-3 w-3 shrink-0 text-muted-foreground" />
                <span>
                  {detail.sirene.adresse}
                  {detail.sirene.codePostal ? `, ${detail.sirene.codePostal}` : ""}
                  {detail.sirene.commune ? ` ${detail.sirene.commune}` : ""}
                </span>
              </p>
            ) : null}
            {detail.sirene.dirigeant ? (
              <p className="flex items-center gap-1 text-foreground/80">
                <Users className="h-3 w-3 text-muted-foreground" />
                {detail.sirene.dirigeant}
              </p>
            ) : null}
          </div>
        ) : null}

        {/* Canaux : manuel + auto fusionnés (manuel prioritaire) */}
        {hasAnyContact ? (
          <div className="space-y-1.5">
            {phone ? (
              <ContactRow icon={Phone} value={phone} source={phoneSrc ?? null} href={`tel:${phone}`} />
            ) : null}
            {email ? (
              <ContactRow icon={Mail} value={email} source={emailSrc ?? null} href={`mailto:${email}`} />
            ) : null}
            {website ? (
              <ContactRow
                icon={Globe}
                value={website}
                source={websiteSrc ?? null}
                href={website.startsWith("http") ? website : `https://${website}`}
                external
              />
            ) : null}
            {detail?.auto.hours && !detail.editable.phone ? (
              <p className="text-[11px] text-muted-foreground">
                Horaires : {detail.auto.hours}
              </p>
            ) : null}
          </div>
        ) : detail && !loading ? (
          <p className="text-muted-foreground">
            Aucun contact enregistré.{" "}
            <button
              onClick={enrich}
              className="text-primary hover:underline"
              disabled={enriching}
            >
              Lancer l&apos;enrichissement automatique
            </button>{" "}
            (OSM + Google Places).
          </p>
        ) : null}

        {/* Footer méta : sources */}
        {lastEnriched ? (
          <p className="border-t border-border/60 pt-2 text-[10px] text-muted-foreground">
            Auto-enrichissement : {detail?.auto.source}
            {" · "}
            {lastEnriched.toLocaleString("fr-FR", { dateStyle: "short", timeStyle: "short" })}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function ContactRow({
  icon: Icon,
  value,
  source,
  href,
  external,
}: {
  icon: React.ComponentType<{ className?: string }>;
  value: string;
  source: string | null;
  href: string;
  external?: boolean;
}) {
  const copy = () => {
    void navigator.clipboard.writeText(value);
    toast.success("Copié");
  };
  return (
    <div className="flex items-center gap-2 text-foreground">
      <Icon className="h-3.5 w-3.5 shrink-0 text-primary" />
      <a
        href={href}
        target={external ? "_blank" : undefined}
        rel={external ? "noreferrer" : undefined}
        className="min-w-0 flex-1 truncate hover:underline"
      >
        {value}
      </a>
      {source ? (
        <span className="shrink-0 rounded bg-secondary/60 px-1.5 py-0.5 text-[9px] font-medium uppercase text-muted-foreground">
          {source}
        </span>
      ) : null}
      <button
        onClick={copy}
        className="shrink-0 rounded p-1 text-muted-foreground hover:bg-secondary/50"
        title="Copier"
      >
        <Copy className="h-3 w-3" />
      </button>
      {external ? (
        <ExternalLink className="h-3 w-3 text-muted-foreground" />
      ) : null}
    </div>
  );
}

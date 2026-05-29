"use client";
/**
 * UI scraper Pages Jaunes / Pages Blanches.
 *
 * Onglets : "Recherche externe" (deep-links — fallback légal) +
 * "Scrap auto" (API /api/contacts/scrape — résultats parsés).
 *
 * Utilisé sur les fiches copro / DPE pour trouver les contacts à
 * une adresse en quelques clics.
 */
import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import {
  Search, Loader2, Phone, Mail, Globe, MapPin, AlertTriangle,
  Building, Users2, BookOpen, ExternalLink, Copy, ChevronDown,
  ChevronUp, RefreshCw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import { ExternalContactLinks } from "./external-contact-links";

type ScrapedContact = {
  name: string | null;
  phone: string | null;
  email: string | null;
  website: string | null;
  category: string | null;
  address: string | null;
  source: "pj" | "pb";
};

type ScrapeResult = {
  source: "pj" | "pb";
  cached: boolean;
  fetcher: "direct" | "scrapingbee";
  url: string;
  htmlLength: number;
  items: ScrapedContact[];
  error: string | null;
};

export function ContactsScraper({
  address,
  cp,
  city,
}: {
  address: string;
  cp?: string | null;
  city?: string | null;
}) {
  const [tab, setTab] = useState<"external" | "scrape">("external");
  const [pjResult, setPjResult] = useState<ScrapeResult | null>(null);
  const [pbResult, setPbResult] = useState<ScrapeResult | null>(null);

  const scrapeMutation = useMutation({
    mutationFn: async (source: "pj" | "pb"): Promise<ScrapeResult> => {
      const r = await fetch("/api/contacts/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source, address, cp, city }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error ?? "Erreur scraper");
      return j;
    },
    onSuccess: (data) => {
      if (data.source === "pj") setPjResult(data);
      else setPbResult(data);
      if (data.error) {
        toast.warning(data.error);
      } else if (data.items.length > 0) {
        toast.success(`${data.items.length} contact(s) trouvé(s)`);
      } else {
        toast.info("0 résultat parsé");
      }
    },
    onError: (e) => toast.error((e as Error).message),
  });

  return (
    <div className="space-y-3">
      {/* Tabs */}
      <div className="flex gap-1 border-b border-border">
        <TabButton
          active={tab === "external"}
          onClick={() => setTab("external")}
          icon={ExternalLink}
        >
          Recherche externe (liens)
        </TabButton>
        <TabButton
          active={tab === "scrape"}
          onClick={() => setTab("scrape")}
          icon={Search}
        >
          Scrap auto
        </TabButton>
      </div>

      {tab === "external" ? (
        <ExternalContactLinks address={address} cp={cp} city={city} />
      ) : (
        <div className="space-y-3">
          {/* Warning bandeau */}
          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-[11px] text-amber-900">
            <AlertTriangle className="mr-1 inline h-3 w-3" />
            <strong>Usage à tes risques.</strong> Pages Jaunes interdit le
            scraping automatisé dans ses ToS. Sans
            <code className="mx-1 rounded bg-amber-100 px-1">SCRAPINGBEE_API_KEY</code>
            configuré, la plupart des requêtes seront bloquées par Cloudflare
            (IP datacenter Vercel). Avec ScrapingBee/équivalent : ~50€/mois,
            bypass Cloudflare + render JS.
          </div>

          {/* Boutons scraper */}
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => scrapeMutation.mutate("pj")}
              disabled={scrapeMutation.isPending}
            >
              {scrapeMutation.isPending && scrapeMutation.variables === "pj" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Building className="h-3.5 w-3.5" />
              )}
              Scraper Pages Jaunes (B2B)
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => scrapeMutation.mutate("pb")}
              disabled={scrapeMutation.isPending}
            >
              {scrapeMutation.isPending && scrapeMutation.variables === "pb" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Users2 className="h-3.5 w-3.5" />
              )}
              Scraper Pages Blanches (B2C — RGPD ⚠)
            </Button>
          </div>

          {/* Résultats PJ */}
          {pjResult ? (
            <ResultBlock
              title="Pages Jaunes (entreprises / commerces)"
              icon={Building}
              result={pjResult}
              onRefresh={() => scrapeMutation.mutate("pj")}
            />
          ) : null}

          {/* Résultats PB */}
          {pbResult ? (
            <ResultBlock
              title="Pages Blanches (particuliers)"
              icon={Users2}
              result={pbResult}
              onRefresh={() => scrapeMutation.mutate("pb")}
            />
          ) : null}
        </div>
      )}
    </div>
  );
}

function TabButton({
  active,
  onClick,
  icon: Icon,
  children,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ComponentType<{ className?: string }>;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={
        "flex items-center gap-1.5 border-b-2 px-3 py-1.5 text-xs font-semibold transition-colors " +
        (active
          ? "border-primary text-primary"
          : "border-transparent text-muted-foreground hover:text-foreground")
      }
    >
      <Icon className="h-3.5 w-3.5" />
      {children}
    </button>
  );
}

function ResultBlock({
  title,
  icon: Icon,
  result,
  onRefresh,
}: {
  title: string;
  icon: React.ComponentType<{ className?: string }>;
  result: ScrapeResult;
  onRefresh: () => void;
}) {
  const [showRaw, setShowRaw] = useState(false);
  return (
    <section className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <h3 className="flex items-center gap-1.5 text-xs font-semibold">
          <Icon className="h-3.5 w-3.5 text-primary" />
          {title}
          <span className="text-[10px] font-normal text-muted-foreground">
            ({result.items.length} contact(s) · {result.fetcher}
            {result.cached ? " · cache" : ""})
          </span>
        </h3>
        <button
          onClick={onRefresh}
          title="Forcer un nouveau scrape (ignore le cache)"
          className="rounded p-1 text-muted-foreground hover:bg-secondary"
        >
          <RefreshCw className="h-3.5 w-3.5" />
        </button>
      </div>

      {result.error ? (
        <div className="mb-2 rounded-md border border-amber-200 bg-amber-50 p-2 text-[11px] text-amber-900">
          ⚠ {result.error}
        </div>
      ) : null}

      {result.items.length > 0 ? (
        <ul className="space-y-1">
          {result.items.map((c, i) => (
            <ContactRow key={i} contact={c} />
          ))}
        </ul>
      ) : (
        <p className="text-[11px] text-muted-foreground">
          Aucun contact extrait.{" "}
          <a
            href={result.url}
            target="_blank"
            rel="noreferrer"
            className="text-primary hover:underline"
          >
            Voir la page source →
          </a>
        </p>
      )}

      {/* Diagnostic */}
      <button
        onClick={() => setShowRaw((v) => !v)}
        className="mt-2 flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
      >
        {showRaw ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        Diagnostic (URL + HTML length)
      </button>
      {showRaw ? (
        <div className="mt-1 rounded-md border border-border bg-secondary/30 p-2 text-[10px] text-muted-foreground">
          <p>HTML reçu : {result.htmlLength.toLocaleString("fr-FR")} caractères</p>
          <p className="truncate">
            URL :{" "}
            <a
              href={result.url}
              target="_blank"
              rel="noreferrer"
              className="text-primary hover:underline"
            >
              {result.url}
            </a>
          </p>
        </div>
      ) : null}
    </section>
  );
}

function ContactRow({ contact }: { contact: ScrapedContact }) {
  const copy = (v: string) => {
    void navigator.clipboard.writeText(v);
    toast.success("Copié");
  };
  return (
    <li className="flex items-start gap-2 rounded-md border border-border bg-secondary/20 p-2 text-xs">
      <div className="min-w-0 flex-1">
        <p className="truncate font-semibold text-foreground">
          {contact.name ?? "—"}
          {contact.category ? (
            <span className="ml-1 rounded bg-secondary/60 px-1 text-[9px] font-normal text-muted-foreground">
              {contact.category}
            </span>
          ) : null}
        </p>
        {contact.address ? (
          <p className="flex items-center gap-1 text-[10px] text-muted-foreground">
            <MapPin className="h-3 w-3" /> {contact.address}
          </p>
        ) : null}
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px]">
          {contact.phone ? (
            <a
              href={`tel:${contact.phone}`}
              className="flex items-center gap-1 text-foreground hover:text-primary"
            >
              <Phone className="h-3 w-3" /> {contact.phone}
              <button onClick={(e) => { e.preventDefault(); copy(contact.phone!); }}>
                <Copy className="h-2.5 w-2.5 text-muted-foreground" />
              </button>
            </a>
          ) : null}
          {contact.email ? (
            <a
              href={`mailto:${contact.email}`}
              className="flex items-center gap-1 text-foreground hover:text-primary"
            >
              <Mail className="h-3 w-3" /> {contact.email}
            </a>
          ) : null}
          {contact.website ? (
            <a
              href={contact.website}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 text-foreground hover:text-primary"
            >
              <Globe className="h-3 w-3" /> Site
            </a>
          ) : null}
        </div>
      </div>
    </li>
  );
}

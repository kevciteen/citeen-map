/**
 * Scraper Pages Jaunes / Pages Blanches — récupère noms, téléphones et
 * activités à une adresse.
 *
 * ⚠️ AVERTISSEMENTS IMPORTANTS À LIRE :
 *
 * 1. Pages Jaunes ToS interdisent le scraping automatisé. En cas de volume,
 *    risque de mise en demeure. Usage à tes risques.
 * 2. Le HTML est en JS-heavy + Cloudflare devant. Un fetch direct sans
 *    rendering renvoie souvent une page de challenge vide. Pour fiabilité
 *    production, configure SCRAPINGBEE_API_KEY (ou autre service similaire)
 *    qui gère le JS + Cloudflare.
 * 3. Les sélecteurs HTML peuvent changer à toute mise à jour du site. Surveille
 *    le log d'erreur et adapte au besoin.
 * 4. RGPD : Pages Blanches contient des données personnelles. Pour B2C :
 *    base légale = intérêt légitime (B2B OK, B2C contesté en prospection).
 *
 * Architecture :
 *   - fetchHtml(url) : direct fetch ou via ScrapingBee selon env
 *   - parsePagesJaunes(html) / parsePagesBlanches(html) : extraction regex
 *   - scrapeContacts(opts) : orchestrateur avec cache DB persistant (7j)
 */
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary"; // pour contact_cache

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
];

function pickUA(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

const CACHE_TTL_MS = 7 * 24 * 3600 * 1000;
const NEGATIVE_TTL_MS = 60 * 60 * 1000; // erreur: 1h

export type ScrapeSource = "pj" | "pb";

export type ScrapedContact = {
  name: string | null;
  phone: string | null;
  email: string | null;
  website: string | null;
  category: string | null;
  address: string | null;
  source: ScrapeSource;
};

export type ScrapeResult = {
  source: ScrapeSource;
  cached: boolean;
  fetcher: "direct" | "scrapingbee";
  url: string;
  htmlLength: number;
  items: ScrapedContact[];
  error: string | null;
};

/* ============================== FETCH ============================== */

async function fetchHtml(url: string): Promise<{ html: string; via: "direct" | "scrapingbee" }> {
  const sbKey = process.env.SCRAPINGBEE_API_KEY;
  if (sbKey) {
    // ScrapingBee : render_js=true + premium_proxy=true bypass Cloudflare
    const sbUrl = new URL("https://app.scrapingbee.com/api/v1/");
    sbUrl.searchParams.set("api_key", sbKey);
    sbUrl.searchParams.set("url", url);
    sbUrl.searchParams.set("render_js", "true");
    sbUrl.searchParams.set("premium_proxy", "true");
    sbUrl.searchParams.set("country_code", "fr");
    const r = await fetch(sbUrl, { headers: { accept: "text/html" } });
    if (!r.ok) {
      const body = await r.text().catch(() => "");
      throw new Error(`ScrapingBee HTTP ${r.status} — ${body.slice(0, 200)}`);
    }
    return { html: await r.text(), via: "scrapingbee" };
  }

  // Fallback fetch direct (risque blocage IP datacenter + Cloudflare)
  const r = await fetch(url, {
    headers: {
      "User-Agent": pickUA(),
      "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
      "Cache-Control": "no-cache",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
    },
  });
  if (!r.ok) {
    throw new Error(`HTTP ${r.status} (sans ScrapingBee — probablement blocage)`);
  }
  return { html: await r.text(), via: "direct" };
}

/* ============================== URL BUILDERS ============================== */

function buildPagesJaunesUrl(opts: {
  address?: string | null;
  cp?: string | null;
  city?: string | null;
  name?: string | null;
}): string {
  const u = new URL("https://www.pagesjaunes.fr/recherche/");
  const quoiqui = opts.name?.trim() || "";
  const ou = [opts.address, opts.cp, opts.city].filter(Boolean).join(" ").trim();
  if (quoiqui) u.searchParams.set("quoiqui", quoiqui);
  if (ou) u.searchParams.set("ou", ou);
  return u.toString();
}

function buildPagesBlanchesUrl(opts: {
  name?: string | null;
  cp?: string | null;
  city?: string | null;
}): string {
  const u = new URL("https://www.pagesjaunes.fr/pagesblanches/recherche");
  const quoiqui = opts.name?.trim() || "";
  const ou = [opts.cp, opts.city].filter(Boolean).join(" ").trim();
  if (quoiqui) u.searchParams.set("quoiqui", quoiqui);
  if (ou) u.searchParams.set("ou", ou);
  return u.toString();
}

/* ============================== PARSERS ============================== */

/** Extrait le contenu textuel d'un fragment HTML brut. */
function stripTags(s: string): string {
  return s.replace(/<[^>]*>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ").trim();
}

function decodeHtml(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&eacute;/g, "é")
    .replace(/&egrave;/g, "è")
    .replace(/&ecirc;/g, "ê")
    .replace(/&agrave;/g, "à")
    .replace(/&acirc;/g, "â")
    .replace(/&ccedil;/g, "ç")
    .replace(/&ocirc;/g, "ô")
    .replace(/&ucirc;/g, "û")
    .replace(/&icirc;/g, "î")
    .replace(/&Eacute;/g, "É");
}

/**
 * Parse les résultats Pages Jaunes (pro + assimilés B2B).
 * PJ utilise des classes CSS stables `bi-bloc`, `denomination-links`, `nb-text`.
 */
function parsePagesJaunes(html: string): ScrapedContact[] {
  const items: ScrapedContact[] = [];
  // Détecte les cards de résultats
  const cardRegex = /<article\b[^>]*class="[^"]*bi[-_]bloc[^"]*"[^>]*>([\s\S]*?)<\/article>/gi;
  let m: RegExpExecArray | null;
  while ((m = cardRegex.exec(html)) !== null) {
    const block = m[1];

    const nameMatch =
      block.match(/<h3[^>]*class="[^"]*denomination[^"]*"[^>]*>([\s\S]*?)<\/h3>/i) ||
      block.match(/<a[^>]*class="[^"]*denomination[^"]*"[^>]*>([\s\S]*?)<\/a>/i);
    const name = nameMatch ? decodeHtml(stripTags(nameMatch[1])) : null;

    const phoneMatch = block.match(/class="[^"]*nb[-_]text[^"]*"[^>]*>([\s\S]*?)</i);
    const phone = phoneMatch ? decodeHtml(stripTags(phoneMatch[1])) : null;

    const categoryMatch = block.match(/class="[^"]*activite[^"]*"[^>]*>([\s\S]*?)</i);
    const category = categoryMatch ? decodeHtml(stripTags(categoryMatch[1])) : null;

    const addressMatch = block.match(/class="[^"]*adresse[^"]*"[^>]*>([\s\S]*?)<\/(?:div|a|span)/i);
    const address = addressMatch ? decodeHtml(stripTags(addressMatch[1])) : null;

    const websiteMatch = block.match(/href="(https?:\/\/[^"]+)"[^>]*class="[^"]*site[^"]*"/i);
    const website = websiteMatch ? websiteMatch[1] : null;

    if (name || phone) {
      items.push({
        name,
        phone,
        email: null,
        website,
        category,
        address,
        source: "pj",
      });
    }
  }
  return items;
}

/**
 * Parse les résultats Pages Blanches (particuliers).
 * Format proche de PJ mais sans "activité" — on a name + tel + adresse.
 */
function parsePagesBlanches(html: string): ScrapedContact[] {
  const items: ScrapedContact[] = parsePagesJaunes(html); // même framework PJ
  return items.map((it) => ({ ...it, source: "pb" as const }));
}

/* ============================== CACHE ============================== */

async function getCached(key: string): Promise<ScrapeResult | null> {
  await ensureTertiary();
  const row = await db.get<{ payload_json: string; expires_at: number }>(
    `SELECT payload_json, expires_at FROM contact_cache WHERE cache_key = ?`,
    [key],
  );
  if (!row) return null;
  if (row.expires_at * 1000 < Date.now()) {
    void db.run(`DELETE FROM contact_cache WHERE cache_key = ?`, [key]).catch(() => {});
    return null;
  }
  try {
    return JSON.parse(row.payload_json) as ScrapeResult;
  } catch {
    return null;
  }
}

async function setCached(key: string, payload: ScrapeResult, ttlMs: number): Promise<void> {
  await ensureTertiary();
  const expSec = Math.floor((Date.now() + ttlMs) / 1000);
  await db.run(
    `INSERT INTO contact_cache (cache_key, payload_json, source, expires_at, created_at)
     VALUES (?, ?, ?, ?, unixepoch())
     ON CONFLICT(cache_key) DO UPDATE SET
       payload_json = excluded.payload_json,
       source = excluded.source,
       expires_at = excluded.expires_at,
       created_at = unixepoch()`,
    [key, JSON.stringify(payload), `scrape-${payload.source}`, expSec],
  );
}

/* ============================== API PUBLIQUE ============================== */

export async function scrapeContacts(opts: {
  source: ScrapeSource;
  address?: string | null;
  cp?: string | null;
  city?: string | null;
  name?: string | null;
}): Promise<ScrapeResult> {
  const url =
    opts.source === "pj"
      ? buildPagesJaunesUrl(opts)
      : buildPagesBlanchesUrl({ name: opts.name, cp: opts.cp, city: opts.city });

  const cacheKey = `scrape:${opts.source}:${url}`;
  const cached = await getCached(cacheKey);
  if (cached) return { ...cached, cached: true };

  try {
    const { html, via } = await fetchHtml(url);
    const items =
      opts.source === "pj" ? parsePagesJaunes(html) : parsePagesBlanches(html);
    const result: ScrapeResult = {
      source: opts.source,
      cached: false,
      fetcher: via,
      url,
      htmlLength: html.length,
      items,
      error: items.length === 0
        ? "0 résultats — Cloudflare bloque probablement (set SCRAPINGBEE_API_KEY) ou sélecteurs HTML ont changé"
        : null,
    };
    await setCached(cacheKey, result, items.length > 0 ? CACHE_TTL_MS : NEGATIVE_TTL_MS);
    return result;
  } catch (err) {
    const result: ScrapeResult = {
      source: opts.source,
      cached: false,
      fetcher: process.env.SCRAPINGBEE_API_KEY ? "scrapingbee" : "direct",
      url,
      htmlLength: 0,
      items: [],
      error: (err as Error).message,
    };
    await setCached(cacheKey, result, NEGATIVE_TTL_MS);
    return result;
  }
}

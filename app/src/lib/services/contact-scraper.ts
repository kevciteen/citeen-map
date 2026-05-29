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

export type ScrapeSource = "pj" | "pb" | "118";

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

async function fetchHtml(url: string): Promise<{ html: string; via: "direct" | "scrapingbee"; statusCode: number }> {
  const sbKey = process.env.SCRAPINGBEE_API_KEY;
  if (sbKey) {
    const sbUrl = new URL("https://app.scrapingbee.com/api/v1/");
    sbUrl.searchParams.set("api_key", sbKey);
    sbUrl.searchParams.set("url", url);
    sbUrl.searchParams.set("render_js", "true");
    sbUrl.searchParams.set("premium_proxy", "true");
    sbUrl.searchParams.set("country_code", "fr");
    // ★ Accepte les status codes non-2xx du site cible (pour parser
    //   le HTML quand PJ retourne 404 avec page "aucun résultat")
    sbUrl.searchParams.set("transparent_status_code", "true");
    const r = await fetch(sbUrl, { headers: { accept: "text/html" } });
    const body = await r.text();
    // Vraies erreurs ScrapingBee (5xx, 401, quota) → throw
    if (r.status >= 500 || r.status === 401 || r.status === 429) {
      throw new Error(`ScrapingBee HTTP ${r.status} — ${body.slice(0, 200)}`);
    }
    // 4xx avec HTML body = réponse du site cible → on essaie quand même de parser
    return { html: body, via: "scrapingbee", statusCode: r.status };
  }

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
  const body = await r.text();
  // En direct, 5xx = vraie erreur, mais on garde 4xx avec body
  if (r.status >= 500) {
    throw new Error(`HTTP ${r.status} (sans ScrapingBee — probablement blocage)`);
  }
  return { html: body, via: "direct", statusCode: r.status };
}

/* ============================== URL BUILDERS ============================== */

function buildPagesJaunesUrl(opts: {
  address?: string | null;
  cp?: string | null;
  city?: string | null;
  name?: string | null;
}): string {
  const u = new URL("https://www.pagesjaunes.fr/recherche/");
  // ★ PJ exige `quoiqui` non-vide (sinon 404). Si l'user ne précise pas
  //   d'activité, on cherche toutes les entreprises à l'adresse via
  //   un terme générique large.
  const quoiqui = opts.name?.trim() || "entreprise";
  const ou = [opts.address, opts.cp, opts.city].filter(Boolean).join(" ").trim();
  u.searchParams.set("quoiqui", quoiqui);
  if (ou) u.searchParams.set("ou", ou);
  return u.toString();
}

function buildPagesBlanchesUrl(opts: {
  name?: string | null;
  address?: string | null;
  cp?: string | null;
  city?: string | null;
}): string {
  const u = new URL("https://www.pagesjaunes.fr/pagesblanches/recherche");
  const quoiqui = opts.name?.trim() || "";
  // ★ Inclut l'adresse dans `ou=` — PB est plus précis qu'avec CP+ville seul
  //   (note : sans `quoiqui`, les résultats restent limités car PB est
  //   designed name-first)
  const ou = [opts.address, opts.cp, opts.city].filter(Boolean).join(" ").trim();
  if (quoiqui) u.searchParams.set("quoiqui", quoiqui);
  if (ou) u.searchParams.set("ou", ou);
  return u.toString();
}

function build118000Url(opts: {
  name?: string | null;
  address?: string | null;
  cp?: string | null;
  city?: string | null;
}): string {
  // 118000.fr supporte vraiment la recherche par adresse via label=
  // (concatène nom + adresse complète dans un seul champ)
  const u = new URL("https://www.118000.fr/search");
  const label = [opts.name, opts.address, opts.cp, opts.city]
    .filter(Boolean)
    .join(" ")
    .trim();
  if (label) u.searchParams.set("label", label);
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

/**
 * Parse les résultats 118000.fr (particuliers + pros à une adresse).
 * 118000 a une structure HTML différente de PJ : utilise des classes
 * `card`, `card-title`, `phone-number`, `address`.
 */
function parse118000(html: string): ScrapedContact[] {
  const items: ScrapedContact[] = [];
  // Pattern 1 : cards modernes (post-redesign 2023)
  const cardRegex = /<(?:article|div)\b[^>]*class="[^"]*(?:result|card|listing)[^"]*"[^>]*>([\s\S]*?)<\/(?:article|div)>/gi;
  let m: RegExpExecArray | null;
  while ((m = cardRegex.exec(html)) !== null) {
    const block = m[1];

    const nameMatch =
      block.match(/<(?:h2|h3)[^>]*>([\s\S]*?)<\/(?:h2|h3)>/i) ||
      block.match(/class="[^"]*(?:name|title)[^"]*"[^>]*>([\s\S]*?)</i);
    const name = nameMatch ? decodeHtml(stripTags(nameMatch[1])) : null;

    const phoneMatch =
      block.match(/class="[^"]*(?:phone|tel|tel-number)[^"]*"[^>]*>([\s\S]*?)</i) ||
      block.match(/href="tel:([^"]+)"/i);
    const phone = phoneMatch ? decodeHtml(stripTags(phoneMatch[1])).replace(/[^\d+\s.()-]/g, "").trim() : null;

    const addressMatch = block.match(/class="[^"]*(?:address|adresse|location)[^"]*"[^>]*>([\s\S]*?)<\//i);
    const address = addressMatch ? decodeHtml(stripTags(addressMatch[1])) : null;

    if (name && (phone || address)) {
      items.push({
        name, phone, email: null, website: null,
        category: null, address, source: "118",
      });
    }
  }
  // Déduplication par nom (118000 a parfois des doublons cross-section)
  const seen = new Set<string>();
  return items.filter((c) => {
    const key = `${c.name}|${c.phone}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
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
      : opts.source === "pb"
        ? buildPagesBlanchesUrl({
            name: opts.name,
            address: opts.address,
            cp: opts.cp,
            city: opts.city,
          })
        : build118000Url({
            name: opts.name,
            address: opts.address,
            cp: opts.cp,
            city: opts.city,
          });

  const cacheKey = `scrape:${opts.source}:${url}`;
  const cached = await getCached(cacheKey);
  if (cached) return { ...cached, cached: true };

  try {
    const { html, via, statusCode } = await fetchHtml(url);
    const items =
      opts.source === "pj"
        ? parsePagesJaunes(html)
        : opts.source === "pb"
          ? parsePagesBlanches(html)
          : parse118000(html);
    let errorMsg: string | null = null;
    if (items.length === 0) {
      if (statusCode === 404) {
        errorMsg = `Site cible renvoie 404 (aucun résultat trouvé pour cette requête sur ${opts.source === "pj" ? "Pages Jaunes" : opts.source === "pb" ? "Pages Blanches" : "118000"})`;
      } else if (statusCode >= 400) {
        errorMsg = `Site cible HTTP ${statusCode} — probable blocage ou page d'erreur`;
      } else if (html.length < 5000) {
        errorMsg = `Réponse vide ou très courte (${html.length} caractères) — probable challenge Cloudflare`;
      } else {
        errorMsg = "0 contacts parsés — sélecteurs HTML ont peut-être changé, voir l'URL source";
      }
    }
    const result: ScrapeResult = {
      source: opts.source,
      cached: false,
      fetcher: via,
      url,
      htmlLength: html.length,
      items,
      error: errorMsg,
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

/**
 * Coordonnées d'occupants (téléphone, site web, email, horaires).
 *
 * Stratégie 2 niveaux :
 *   1. OpenStreetMap (Overpass API, gratuit sans clé) — POI taggés par la
 *      communauté avec `phone`, `website`, `email`, `opening_hours`.
 *      Couverture variable (~30-50% des commerces/bureaux en zone urbaine).
 *   2. Fallback Google Places (clé GOOGLE_MAPS_API_KEY, free tier 1000/mois) —
 *      couverture quasi exhaustive mais consomme du quota.
 *
 * Cache TTL : 7 jours (les coordonnées changent rarement).
 */
import { cacheGet, cacheSet } from "./cache";

const OVERPASS_URL = "https://overpass-api.de/api/interpreter";
const GOOGLE_FIND_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json";
const GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json";
const CACHE_TTL_MS = 7 * 24 * 3600 * 1000;
const TIMEOUT_MS = 12000;

export type ContactInfo = {
  phone: string | null;
  website: string | null;
  email: string | null;
  hours: string | null;
  source: "osm" | "google" | "none";
};

function normName(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function nameSimilarity(a: string, b: string): number {
  // Score de similarité simple : nb de tokens communs / nb tokens A
  const ta = new Set(normName(a).split(" ").filter((t) => t.length >= 3));
  const tb = new Set(normName(b).split(" ").filter((t) => t.length >= 3));
  if (ta.size === 0) return 0;
  let common = 0;
  for (const t of ta) if (tb.has(t)) common++;
  return common / ta.size;
}

/* ============================== OSM Overpass ============================== */

type OsmElement = {
  type: "node" | "way" | "relation";
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  tags?: Record<string, string>;
};

async function findOsmContact(opts: {
  lat: number;
  lon: number;
  denomination: string;
  radiusM?: number;
}): Promise<Partial<ContactInfo> | null> {
  const { lat, lon, denomination } = opts;
  const radius = opts.radiusM ?? 40;
  const key = `osm-contact:${lat.toFixed(5)}:${lon.toFixed(5)}:${normName(denomination)}`;
  const hit = cacheGet<Partial<ContactInfo> | null>(key);
  if (hit !== null) return hit;

  // Overpass query : POI taggés avec phone, website, email, dans le rayon
  const query = `[out:json][timeout:12];
(
  node(around:${radius},${lat},${lon})[~"^(phone|contact:phone|website|contact:website|email|contact:email|opening_hours)$"~"."];
  way(around:${radius},${lat},${lon})[~"^(phone|contact:phone|website|contact:website|email|contact:email|opening_hours)$"~"."];
);
out tags center;`;

  try {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), TIMEOUT_MS);
    const res = await fetch(OVERPASS_URL, {
      method: "POST",
      body: `data=${encodeURIComponent(query)}`,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      signal: controller.signal,
    });
    clearTimeout(t);
    if (!res.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const json = (await res.json()) as { elements?: OsmElement[] };
    const elements = json.elements ?? [];

    // Cherche le meilleur match par similarité de nom
    let best: { el: OsmElement; score: number } | null = null;
    for (const el of elements) {
      const tags = el.tags ?? {};
      const name = tags.name ?? tags["name:fr"] ?? "";
      const score = name ? nameSimilarity(denomination, name) : 0;
      if (!best || score > best.score) best = { el, score };
    }

    let pick: OsmElement | null = null;
    if (best && best.score >= 0.4) {
      pick = best.el;
    } else if (elements.length === 1) {
      // Un seul POI → on prend (le nom peut juste manquer)
      pick = elements[0];
    } else if (elements.length > 0 && best && best.score < 0.4) {
      // Plusieurs POI mais aucun match nom → on retourne quand même le plus proche
      // (en pratique le premier de la liste Overpass)
      pick = elements[0];
    }

    if (!pick) {
      cacheSet(key, null, CACHE_TTL_MS);
      return null;
    }
    const t2 = pick.tags ?? {};
    const result: Partial<ContactInfo> = {
      phone: t2.phone ?? t2["contact:phone"] ?? null,
      website: t2.website ?? t2["contact:website"] ?? null,
      email: t2.email ?? t2["contact:email"] ?? null,
      hours: t2.opening_hours ?? null,
    };
    cacheSet(key, result, CACHE_TTL_MS);
    return result;
  } catch {
    return null;
  }
}

/* ============================== Google Places ============================== */

async function findGoogleContact(opts: {
  denomination: string;
  address: string;
}): Promise<Partial<ContactInfo> | null> {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  if (!apiKey) return null;
  const key = `google-contact:${normName(opts.denomination)}|${normName(opts.address)}`;
  const hit = cacheGet<Partial<ContactInfo> | null>(key);
  if (hit !== null) return hit;

  try {
    // 1. Find Place from Text → place_id
    const findUrl = new URL(GOOGLE_FIND_URL);
    findUrl.searchParams.set("input", `${opts.denomination} ${opts.address}`);
    findUrl.searchParams.set("inputtype", "textquery");
    findUrl.searchParams.set("fields", "place_id");
    findUrl.searchParams.set("language", "fr");
    findUrl.searchParams.set("key", apiKey);
    const findRes = await fetch(findUrl, { next: { revalidate: 7 * 24 * 3600 } });
    if (!findRes.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const findJson = (await findRes.json()) as { candidates?: Array<{ place_id: string }>; status?: string };
    const placeId = findJson.candidates?.[0]?.place_id;
    if (!placeId) {
      cacheSet(key, null, CACHE_TTL_MS);
      return null;
    }

    // 2. Place Details → coordonnées
    const detailsUrl = new URL(GOOGLE_DETAILS_URL);
    detailsUrl.searchParams.set("place_id", placeId);
    detailsUrl.searchParams.set("fields", "formatted_phone_number,international_phone_number,website,opening_hours,name");
    detailsUrl.searchParams.set("language", "fr");
    detailsUrl.searchParams.set("key", apiKey);
    const detRes = await fetch(detailsUrl, { next: { revalidate: 7 * 24 * 3600 } });
    if (!detRes.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const detJson = (await detRes.json()) as {
      result?: {
        formatted_phone_number?: string;
        international_phone_number?: string;
        website?: string;
        opening_hours?: { weekday_text?: string[] };
      };
    };
    const r = detJson.result;
    if (!r) {
      cacheSet(key, null, CACHE_TTL_MS);
      return null;
    }
    const result: Partial<ContactInfo> = {
      phone: r.formatted_phone_number ?? r.international_phone_number ?? null,
      website: r.website ?? null,
      email: null, // Google Places ne renvoie pas l'email
      hours: r.opening_hours?.weekday_text?.join(" · ") ?? null,
    };
    cacheSet(key, result, CACHE_TTL_MS);
    return result;
  } catch {
    return null;
  }
}

/* ============================== API publique ============================== */

/**
 * Récupère les coordonnées d'une société.
 * OSM en premier (gratuit) ; Google Places en fallback si OSM rien ou
 * coordonnées incomplètes.
 */
export async function findContactInfo(opts: {
  denomination: string;
  address: string;
  lat?: number;
  lon?: number;
}): Promise<ContactInfo> {
  const empty: ContactInfo = { phone: null, website: null, email: null, hours: null, source: "none" };
  if (!opts.denomination || opts.denomination.trim().length === 0) return empty;

  let osmResult: Partial<ContactInfo> | null = null;
  if (opts.lat && opts.lon && Number.isFinite(opts.lat) && Number.isFinite(opts.lon)) {
    osmResult = await findOsmContact({
      lat: opts.lat,
      lon: opts.lon,
      denomination: opts.denomination,
    });
  }

  // Si OSM a tout ce qu'on veut (phone OU website), on s'arrête
  if (osmResult && (osmResult.phone || osmResult.website)) {
    return {
      phone: osmResult.phone ?? null,
      website: osmResult.website ?? null,
      email: osmResult.email ?? null,
      hours: osmResult.hours ?? null,
      source: "osm",
    };
  }

  // Sinon fallback Google (consomme du quota)
  const googleResult = await findGoogleContact({
    denomination: opts.denomination,
    address: opts.address,
  });
  if (googleResult && (googleResult.phone || googleResult.website)) {
    return {
      phone: googleResult.phone ?? osmResult?.phone ?? null,
      website: googleResult.website ?? osmResult?.website ?? null,
      email: osmResult?.email ?? null, // Google ne donne pas d'email
      hours: googleResult.hours ?? osmResult?.hours ?? null,
      source: "google",
    };
  }

  // Rien trouvé
  return osmResult
    ? {
        phone: osmResult.phone ?? null,
        website: osmResult.website ?? null,
        email: osmResult.email ?? null,
        hours: osmResult.hours ?? null,
        source: osmResult.phone || osmResult.website || osmResult.email ? "osm" : "none",
      }
    : empty;
}

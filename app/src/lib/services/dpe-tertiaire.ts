/**
 * DPE Tertiaire ADEME — dataset distinct du DPE résidentiel.
 *
 * Endpoint : https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines
 * Couverture : DPE des bâtiments tertiaires depuis le 01/07/2021.
 * Pas de clé API, format DataFair (mêmes filtres geo_distance / qs que le résidentiel).
 *
 * Note : la BDNB consolide ces données dans `batiment_groupe_dpe_tertiaire`.
 * On garde une voie ADEME directe (1) en fallback quand BDNB rate, (2) pour
 * la précharge IDF en bulk (pas de quota côté ADEME).
 */
import { cacheGet, cacheSet } from "./cache";

const DPE_TERT_BASE = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines";
const REQ_TIMEOUT_MS = 15000;

export type DpeTertiaireRecord = {
  numero_dpe?: string;
  etiquette_dpe?: string;             // A..G
  etiquette_ges?: string;             // A..G
  conso_kwhep_m2_an?: number | string;
  conso_kwhef_m2_an?: number | string;
  emission_ges_kgco2_m2_an?: number | string;
  surface_utile?: number | string;
  type_usage_principal?: string;       // Bureaux, Commerces, etc.
  annee_construction?: number | string;
  date_etablissement_dpe?: string;
  date_derniere_modification_dpe?: string;
  adresse?: string;
  adresse_brut?: string;
  code_postal?: string;
  nom_commune?: string;
  code_insee_commune?: string;
  // Coordonnées (varient selon les versions du dataset)
  _geopoint?: string | [number, number] | { lat: number; lon: number };
  latitude?: number | string;
  longitude?: number | string;
  [key: string]: unknown;
};

function parseDate(d: unknown): number {
  if (!d) return 0;
  const t = Date.parse(String(d));
  return Number.isFinite(t) ? t : 0;
}

function dedupeLatest(list: DpeTertiaireRecord[]): DpeTertiaireRecord[] {
  const best = new Map<string, DpeTertiaireRecord>();
  for (const d of list) {
    const numero = d.numero_dpe;
    if (!numero) continue;
    const t1 =
      parseDate(d.date_derniere_modification_dpe) ||
      parseDate(d.date_etablissement_dpe);
    const prev = best.get(numero);
    const t0 = prev
      ? parseDate(prev.date_derniere_modification_dpe) ||
        parseDate(prev.date_etablissement_dpe)
      : -1;
    if (!prev || t1 >= t0) best.set(numero, d);
  }
  return [...best.values()];
}

async function ademeFetch(params: URLSearchParams): Promise<DpeTertiaireRecord[]> {
  const url = new URL(DPE_TERT_BASE);
  for (const [k, v] of params.entries()) url.searchParams.set(k, v);

  let lastErr: unknown = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), REQ_TIMEOUT_MS);
      const res = await fetch(url, {
        headers: { accept: "application/json" },
        next: { revalidate: 600 },
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`ADEME DPE tertiaire HTTP ${res.status} ${txt}`);
      }
      const json = (await res.json()) as { results?: DpeTertiaireRecord[] };
      return Array.isArray(json?.results) ? json.results : [];
    } catch (err) {
      lastErr = err;
      if (attempt < 2) {
        await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
      }
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error("ADEME DPE tertiaire fetch failed");
}

/**
 * DPE tertiaires dans un rayon autour d'un point (recherche fiche bâtiment).
 */
export async function fetchDpeTertiaireAround(opts: {
  lat: number;
  lon: number;
  radiusM?: number;
  size?: number;
}): Promise<DpeTertiaireRecord[]> {
  const { lat, lon, radiusM = 50, size = 100 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error("lat/lon invalides");
  }
  const key = `dpe-tert:around:${lat.toFixed(6)}|${lon.toFixed(6)}|${radiusM}|${size}`;
  const hit = cacheGet<DpeTertiaireRecord[]>(key);
  if (hit) return hit;

  const params = new URLSearchParams();
  params.set("size", String(size));
  params.set("geo_distance", `${lon},${lat},${radiusM}`);
  const raw = await ademeFetch(params);
  const list = dedupeLatest(raw);
  cacheSet(key, list, 24 * 3600 * 1000);
  return list;
}

/**
 * Récupère un DPE tertiaire par numéro.
 */
export async function fetchDpeTertiaireByNumero(numero: string): Promise<DpeTertiaireRecord | null> {
  if (!numero || numero.length < 5) return null;
  const key = `dpe-tert:numero:${numero}`;
  const hit = cacheGet<DpeTertiaireRecord | null>(key);
  if (hit !== null) return hit;

  const params = new URLSearchParams();
  params.set("size", "5");
  params.set("qs", `numero_dpe:"${numero}"`);
  const raw = await ademeFetch(params);
  const sorted = [...raw].sort(
    (a, b) =>
      (parseDate(b.date_derniere_modification_dpe) || parseDate(b.date_etablissement_dpe)) -
      (parseDate(a.date_derniere_modification_dpe) || parseDate(a.date_etablissement_dpe)),
  );
  const found = sorted[0] ?? null;
  cacheSet(key, found, 24 * 3600 * 1000);
  return found;
}

/**
 * Récupère un lot de DPE tertiaires pour un département (utilisé par le
 * script d'import IDF). Paginé via le paramètre `after` de DataFair.
 *
 * @param departement code département (75, 77, 78, 91, 92, 93, 94, 95)
 * @param size taille de page (max 10000 pour DataFair)
 * @param after curseur de pagination (renvoyé par next.href)
 */
export async function fetchDpeTertiaireByDepartement(opts: {
  departement: string;
  size?: number;
  after?: string;
}): Promise<{ records: DpeTertiaireRecord[]; nextAfter: string | null }> {
  const { departement, size = 1000, after } = opts;
  const params = new URLSearchParams();
  params.set("size", String(size));
  params.set("qs", `code_postal:${departement}*`);
  if (after) params.set("after", after);

  const url = new URL(DPE_TERT_BASE);
  for (const [k, v] of params.entries()) url.searchParams.set(k, v);

  const res = await fetch(url, {
    headers: { accept: "application/json" },
    next: { revalidate: 0 },
  });
  if (!res.ok) throw new Error(`ADEME DPE tertiaire HTTP ${res.status}`);
  const json = (await res.json()) as {
    results?: DpeTertiaireRecord[];
    next?: string;
  };
  const records = json?.results ?? [];
  let nextAfter: string | null = null;
  if (json.next) {
    try {
      const u = new URL(json.next);
      nextAfter = u.searchParams.get("after");
    } catch {
      nextAfter = null;
    }
  }
  return { records, nextAfter };
}

/**
 * Extrait lat/lon d'un record (champ `_geopoint` ou `latitude/longitude`).
 */
export function extractLatLon(r: DpeTertiaireRecord): { lat: number; lon: number } | null {
  if (r._geopoint) {
    if (typeof r._geopoint === "string") {
      const [latS, lonS] = r._geopoint.split(",");
      const lat = Number(latS);
      const lon = Number(lonS);
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
    } else if (Array.isArray(r._geopoint)) {
      const [a, b] = r._geopoint;
      if (Number.isFinite(a) && Number.isFinite(b)) return { lat: a, lon: b };
    } else if (typeof r._geopoint === "object") {
      const { lat, lon } = r._geopoint;
      if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
    }
  }
  const lat = Number(r.latitude);
  const lon = Number(r.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
  return null;
}

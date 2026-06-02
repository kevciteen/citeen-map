/**
 * DPE Logements existants — méthode pré-2021 (dataset ADEME `dpe-france`).
 *
 * Endpoint : https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines
 *
 * Ce dataset contient ~10,7M de DPE résidentiels antérieurs à juillet 2021
 * (ancienne méthode 3CL ou autres). Toujours valides 10 ans après leur
 * date d'établissement.
 *
 * Structure proche du dataset `dpe-tertiaire` (classe_consommation_energie,
 * classe_estimation_ges, consommation_energie, secteur_activite, etc.).
 */
import { cacheGet, cacheSet } from "./cache";

const DPE_LEGACY_BASE = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines";
const REQ_TIMEOUT_MS = 15000;

export type DpeLegacyRecord = {
  numero_dpe?: string;
  classe_consommation_energie?: string;
  classe_estimation_ges?: string;
  consommation_energie?: number | string;
  estimation_ges?: number | string;
  surface_thermique_lot?: number | string;
  surface_habitable?: number | string;
  shon?: number | string;
  // Adresse
  geo_adresse?: string;
  nom_rue?: string;
  commune?: string;
  code_postal?: string;
  code_insee_commune?: string;
  // Coords
  latitude?: number | string;
  longitude?: number | string;
  _geopoint?: string;
  // Méta
  annee_construction?: number | string;
  date_etablissement_dpe?: string;
  date_reception_dpe?: string;
  tr001_modele_dpe_type_libelle?: string; // "Vente" | "Location" | etc.
  tr002_type_batiment_libelle?: string;   // "Maison Individuelle" | "Logement" | ...
  [key: string]: unknown;
};

function parseDate(d: unknown): number {
  if (!d) return 0;
  const t = Date.parse(String(d));
  return Number.isFinite(t) ? t : 0;
}

function dedupeLatest(list: DpeLegacyRecord[]): DpeLegacyRecord[] {
  const best = new Map<string, DpeLegacyRecord>();
  for (const d of list) {
    const numero = d.numero_dpe;
    if (!numero) continue;
    const t1 = parseDate(d.date_etablissement_dpe);
    const prev = best.get(numero);
    const t0 = prev ? parseDate(prev.date_etablissement_dpe) : -1;
    if (!prev || t1 >= t0) best.set(numero, d);
  }
  return [...best.values()];
}

async function ademeFetch(params: URLSearchParams): Promise<DpeLegacyRecord[]> {
  const url = new URL(DPE_LEGACY_BASE);
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
      if (!res.ok) throw new Error(`ADEME DPE legacy HTTP ${res.status}`);
      const json = (await res.json()) as { results?: DpeLegacyRecord[] };
      return Array.isArray(json?.results) ? json.results : [];
    } catch (err) {
      lastErr = err;
      if (attempt < 2) await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error("ADEME DPE legacy fetch failed");
}

export async function fetchDpeLegacyAround(opts: {
  lat: number;
  lon: number;
  radiusM?: number;
  size?: number;
}): Promise<DpeLegacyRecord[]> {
  const { lat, lon, radiusM = 80, size = 200 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return [];
  const key = `dpe-legacy:around:${lat.toFixed(6)}|${lon.toFixed(6)}|${radiusM}|${size}`;
  const hit = cacheGet<DpeLegacyRecord[]>(key);
  if (hit) return hit;

  const params = new URLSearchParams();
  params.set("size", String(size));
  params.set("geo_distance", `${lon},${lat},${radiusM}`);
  const raw = await ademeFetch(params);
  const list = dedupeLatest(raw);
  cacheSet(key, list, 24 * 3600 * 1000);
  return list;
}

export async function fetchDpeLegacyByNumero(numero: string): Promise<DpeLegacyRecord | null> {
  if (!numero || numero.length < 5) return null;
  const key = `dpe-legacy:numero:${numero}`;
  const hit = cacheGet<DpeLegacyRecord | null>(key);
  if (hit !== null) return hit;

  const params = new URLSearchParams();
  params.set("size", "5");
  params.set("qs", `numero_dpe:"${numero}"`);
  const raw = await ademeFetch(params);
  const sorted = [...raw].sort(
    (a, b) => parseDate(b.date_etablissement_dpe) - parseDate(a.date_etablissement_dpe),
  );
  const found = sorted[0] ?? null;
  cacheSet(key, found, 24 * 3600 * 1000);
  return found;
}

/** Extract lat/lon depuis le record (différentes formes ADEME) */
export function extractLegacyLatLon(r: DpeLegacyRecord): { lat: number; lon: number } | null {
  const lat = Number(r.latitude);
  const lon = Number(r.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lon) && (lat !== 0 || lon !== 0)) {
    return { lat, lon };
  }
  if (typeof r._geopoint === "string") {
    const [latS, lonS] = r._geopoint.split(",");
    const la = Number(latS);
    const lo = Number(lonS);
    if (Number.isFinite(la) && Number.isFinite(lo)) return { lat: la, lon: lo };
  }
  return null;
}

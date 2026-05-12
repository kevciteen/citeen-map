import { cacheGet, cacheSet } from "./cache";
import { ADEME_DPE_LINES_URL } from "./ademe-constants";

const DPE_API_BASE = ADEME_DPE_LINES_URL;

export type AdemeRecord = {
  numero_dpe?: string;
  numero_dpe_immeuble?: string;
  numero_dpe_immeuble_associe?: string;
  numero_immatriculation_copropriete?: string;
  type_dpe?: string;
  type_dpe_batiment?: string;
  date_etablissement_dpe?: string;
  date_derniere_modification_dpe?: string;
  etiquette_dpe?: string;
  etiquette_ges?: string;
  conso_5_usages_par_m2_ep?: number | string;
  surface_habitable_logement?: number | string;
  surface_habitable_immeuble?: number | string;
  annee_construction?: number | string;
  nom_commune_ban?: string;
  nom_commune_brut?: string;
  code_postal_ban?: string;
  code_postal_brut?: string;
  nom_rue_ban?: string;
  numero_voie_ban?: string;
  adresse_ban?: string;
  adresse_complete_brut?: string;
  _geopoint?: string | [number, number] | { lat: number; lon: number };
  [key: string]: unknown;
};

function parseDate(d: unknown): number {
  if (!d) return 0;
  const t = Date.parse(String(d));
  return Number.isFinite(t) ? t : 0;
}

// On ne garde que le "dernier état" par numero_dpe / numero_dpe_immeuble
function dedupeLatestByNumeroDpe(list: AdemeRecord[]): AdemeRecord[] {
  const best = new Map<string, AdemeRecord>();
  for (const d of list) {
    const numero = d?.numero_dpe || d?.numero_dpe_immeuble;
    if (!numero) continue;
    const t1 =
      parseDate(d?.date_derniere_modification_dpe) ||
      parseDate(d?.date_etablissement_dpe);
    const prev = best.get(numero);
    const t0 = prev
      ? parseDate(prev?.date_derniere_modification_dpe) ||
        parseDate(prev?.date_etablissement_dpe)
      : -1;
    if (!prev || t1 >= t0) best.set(numero, d);
  }
  return [...best.values()];
}

export async function fetchAdemeDpeAround(opts: {
  lat: number;
  lon: number;
  r?: number;
  size?: number;
}): Promise<AdemeRecord[]> {
  const { lat, lon, r = 50, size = 2000 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error("lat/lon invalides");
  }
  if (!Number.isFinite(r) || r <= 0) throw new Error("r invalide");

  const key = `ademe:${lat.toFixed(6)}|${lon.toFixed(6)}|${r}|${size}`;
  const hit = cacheGet<AdemeRecord[]>(key);
  if (hit) return hit;

  const url = new URL(DPE_API_BASE);
  url.searchParams.set("size", String(size));
  // DataFair geo_distance attend "lon,lat,r" (r en mètres)
  url.searchParams.set("geo_distance", `${lon},${lat},${r}`);

  // Retry avec back-off léger : ADEME ferme parfois la socket sous charge.
  let lastErr: unknown = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);
      const res = await fetch(url, {
        headers: { accept: "application/json" },
        next: { revalidate: 600 },
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`ADEME HTTP ${res.status} ${txt}`);
      }
      const json = (await res.json()) as { results?: AdemeRecord[] };
      const raw = Array.isArray(json?.results) ? json.results : [];
      const list = dedupeLatestByNumeroDpe(raw);
      cacheSet(key, list);
      return list;
    } catch (err) {
      lastErr = err;
      if (attempt < 2) {
        await new Promise((resolve) => setTimeout(resolve, 500 * (attempt + 1)));
      }
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error("ADEME fetch failed");
}

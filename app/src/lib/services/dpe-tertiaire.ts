/**
 * DPE Tertiaire ADEME — dataset `dpe-tertiaire` (ancienne méthode 2007-2021).
 *
 * Endpoint : https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines
 *
 * **Attention** : la structure de ce dataset n'a RIEN à voir avec le DPE
 * résidentiel `dpe03existant`. Les champs principaux sont :
 *   - numero_dpe
 *   - classe_consommation_energie (DPE A..G)
 *   - classe_estimation_ges (GES A..G)
 *   - consommation_energie (kWhEP/m²/an)
 *   - estimation_ges (kgCO2/m²/an)
 *   - secteur_activite (TEXTE LIBRE — bureaux, commerce, hotel, etc.)
 *   - tr002_type_batiment_libelle (Résidentiel | Non résidentiel)
 *   - latitude / longitude (≠ _geopoint)
 *   - geo_adresse, nom_rue, commune, code_postal, code_insee_commune
 *   - annee_construction, surface_utile, surface_habitable, shon
 *
 * **Le dataset contient aussi du résidentiel collectif** (parties communes
 * d'immeubles d'habitation) — il faut filtrer sur tr002_type_batiment_libelle.
 */
import { cacheGet, cacheSet } from "./cache";

const DPE_TERT_BASE = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-tertiaire/lines";
const REQ_TIMEOUT_MS = 15000;

export type DpeTertiaireRecord = {
  numero_dpe?: string;
  // Classifications (≠ "etiquette_dpe" du résidentiel)
  classe_consommation_energie?: string;       // A..G
  classe_estimation_ges?: string;             // A..G
  consommation_energie?: number | string;     // kWhEP/m²/an
  estimation_ges?: number | string;           // kgCO2/m²/an
  // Surfaces (préférer surface_utile)
  surface_utile?: number | string;
  surface_habitable?: number | string;
  shon?: number | string;
  surface_thermique_lot?: number | string;
  // Usage (texte libre)
  secteur_activite?: string;
  tr002_type_batiment_libelle?: string;       // "Non résidentiel" | "Résidentiel"
  // Adresse
  geo_adresse?: string;
  nom_rue?: string;
  commune?: string;
  code_postal?: string;
  code_insee_commune?: string;
  code_insee_commune_actualise?: string;
  // Coordonnées
  latitude?: number | string;
  longitude?: number | string;
  _geopoint?: string;
  // Métadonnées
  annee_construction?: number | string;
  date_etablissement_dpe?: string;
  date_reception_dpe?: string;
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
    const t1 = parseDate(d.date_etablissement_dpe);
    const prev = best.get(numero);
    const t0 = prev ? parseDate(prev.date_etablissement_dpe) : -1;
    if (!prev || t1 >= t0) best.set(numero, d);
  }
  return [...best.values()];
}

/**
 * Filtre les records pour ne garder que le vrai tertiaire (exclut le
 * résidentiel collectif "Habitation Parties communes/privatives").
 *
 * Attention : "Non résidentiel" contient "résidentiel" — on ne peut pas
 * juste faire includes("résidentiel"). On match les libellés exacts ADEME.
 */
export function isReallyTertiary(r: DpeTertiaireRecord): boolean {
  const typeBat = String(r.tr002_type_batiment_libelle ?? "").trim().toLowerCase();
  // Libellés ADEME : "Résidentiel", "Non résidentiel", "Centres commerciaux"
  if (typeBat === "résidentiel" || typeBat === "residentiel") return false;
  const secteur = String(r.secteur_activite ?? "").trim().toLowerCase();
  if (secteur.startsWith("habitation")) return false;
  return true;
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
      if (attempt < 2) await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error("ADEME DPE tertiaire fetch failed");
}

export async function fetchDpeTertiaireAround(opts: {
  lat: number;
  lon: number;
  radiusM?: number;
  size?: number;
}): Promise<DpeTertiaireRecord[]> {
  const { lat, lon, radiusM = 50, size = 100 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) throw new Error("lat/lon invalides");

  const key = `dpe-tert:around:${lat.toFixed(6)}|${lon.toFixed(6)}|${radiusM}|${size}`;
  const hit = cacheGet<DpeTertiaireRecord[]>(key);
  if (hit) return hit;

  const params = new URLSearchParams();
  params.set("size", String(size));
  params.set("geo_distance", `${lon},${lat},${radiusM}`);
  const raw = await ademeFetch(params);
  const list = dedupeLatest(raw).filter(isReallyTertiary);
  cacheSet(key, list, 24 * 3600 * 1000);
  return list;
}

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
    (a, b) => parseDate(b.date_etablissement_dpe) - parseDate(a.date_etablissement_dpe),
  );
  const found = sorted[0] ?? null;
  cacheSet(key, found, 24 * 3600 * 1000);
  return found;
}

/**
 * Bulk par département pour l'import — pagine via `after` (curseur DataFair).
 * Ne filtre PAS isReallyTertiary ici : le script d'import doit décider.
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
  const json = (await res.json()) as { results?: DpeTertiaireRecord[]; next?: string };
  const records = json?.results ?? [];
  let nextAfter: string | null = null;
  if (json.next) {
    try {
      nextAfter = new URL(json.next).searchParams.get("after");
    } catch {
      nextAfter = null;
    }
  }
  return { records, nextAfter };
}

/**
 * Extrait lat/lon (préfère latitude/longitude directs, fallback _geopoint).
 */
export function extractLatLon(r: DpeTertiaireRecord): { lat: number; lon: number } | null {
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

/**
 * Normalisation libérale du secteur_activite (texte libre ADEME) vers le
 * Sector enum utilisé par le moteur CEE.
 */
export type Sector =
  | "Bureaux"
  | "Commerces"
  | "Hotellerie / Restauration"
  | "Sante"
  | "Enseignement"
  | "Autres secteurs";

export function normalizeSector(secteurActivite: unknown): Sector {
  const raw = String(secteurActivite ?? "").trim();
  if (!raw || raw === "\\N" || raw === "null") return "Autres secteurs";
  const s = raw.toLowerCase();
  // Mapping libéral — l'ADEME a beaucoup de variations orthographiques
  if (s.includes("hopit") || s.includes("hôpit") || s.includes("clinique") || s.includes("ehpad") || s.includes("medic") || s.includes("médic") || s.includes("pharmac") || s.includes("santé") || s.includes("sante") || s.includes("cabinet")) return "Sante";
  if (s.includes("ecole") || s.includes("école") || s.includes("scolaire") || s.includes("creche") || s.includes("crèche") || s.includes("univers") || (s.includes("enseignement") && !s.includes("bureau"))) return "Enseignement";
  if (s.includes("hotel") || s.includes("hôtel") || s.includes("restaur") || s.includes("brasserie") || s.includes("bar") || s.includes("café") || s.includes("cafe")) return "Hotellerie / Restauration";
  if (s.includes("commerce") || s.includes("magasin") || s.includes("boutique") || s.includes("local commercial") || s.includes("local  a usage de commerce") || s.includes("local commercial") || s.includes("local d'activ") || s.includes("centre commercial") || s.includes("boucherie") || s.includes("brocant") || s.includes("pressing") || s.includes("coiffure") || s.includes("salon de") || s.includes("banque")) return "Commerces";
  if (s.includes("bureau") || s.includes("administration") || s.includes("commissariat") || s.includes("ambassade") || s.includes("musee") || s.includes("musée") || s.includes("théâtre") || s.includes("theatre")) return "Bureaux";
  return "Autres secteurs";
}

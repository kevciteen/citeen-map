/**
 * BDNB (Base de Données Nationale des Bâtiments — CSTB).
 *
 * API publique : https://api-portail.bdnb.io/ (offre Open, sans clé, 10 000 req/mois).
 * Couvre 32M+ bâtiments France entière, croise RNB, DPE résidentiel & tertiaire,
 * SIRENE à l'adresse, réseaux de chaleur, cadastre.
 *
 * Documentation : https://bdnb.io/documentation/
 *
 * Stratégie cache : 7 jours (les données ne bougent qu'au millésime ~semestriel),
 * pour économiser le quota mensuel.
 */
import { cacheGet, cacheSet } from "./cache";

const BDNB_BASE = "https://api-portail.bdnb.io/v1/donnees";
const CACHE_TTL_MS = 7 * 24 * 3600 * 1000;
const REQ_TIMEOUT_MS = 15000;

export type BdnbBuilding = {
  batimentGroupeId: string;
  rnbId?: string | null;
  adresse?: string | null;
  codePostal?: string | null;
  commune?: string | null;
  codeInseeCommune?: string | null;
  // Champs tertiaires (peuvent être null si bâtiment résidentiel pur)
  surfaceUtileTertiaire?: number | null;
  typeUsage?: string | null;
  // DPE tertiaire (issu de batiment_groupe_dpe_tertiaire)
  dpeTertiaire?: {
    etiquetteDpe?: string | null;
    etiquetteGes?: string | null;
    numeroDpe?: string | null;
    surface?: number | null;
    dateEtablissement?: string | null;
  } | null;
  // Cadastre
  section?: string | null;
  numeroParcelle?: string | null;
  // Géométrie
  lat?: number | null;
  lon?: number | null;
  // SIRENE — sociétés liées à l'adresse (SIRET référencés par la BDNB)
  siretsLies?: string[];
};

type BdnbApiResponse = {
  data?: Array<Record<string, unknown>>;
  meta?: { total?: number };
};

function num(v: unknown): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function str(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function mapBuilding(raw: Record<string, unknown>): BdnbBuilding | null {
  const id = str(raw.batiment_groupe_id);
  if (!id) return null;
  const dpeT = raw.batiment_groupe_dpe_tertiaire as Record<string, unknown> | undefined;
  return {
    batimentGroupeId: id,
    rnbId: str(raw.rnb_id),
    adresse: str(raw.libelle_adr_principale),
    codePostal: str(raw.code_postal),
    commune: str(raw.libelle_commune),
    codeInseeCommune: str(raw.code_commune_insee),
    surfaceUtileTertiaire: num(raw.surface_utile_tertiaire) ?? num(raw.s_utile),
    typeUsage: str(raw.usage_principal) ?? str(raw.type_usage),
    section: str(raw.section_cadastrale),
    numeroParcelle: str(raw.numero_parcelle),
    lat: num(raw.latitude) ?? num(raw.lat),
    lon: num(raw.longitude) ?? num(raw.lon),
    dpeTertiaire: dpeT
      ? {
          etiquetteDpe: str(dpeT.classe_bilan_dpe) ?? str(dpeT.etiquette_dpe),
          etiquetteGes: str(dpeT.classe_emission_ges) ?? str(dpeT.etiquette_ges),
          numeroDpe: str(dpeT.numero_dpe),
          surface: num(dpeT.surface_utile) ?? num(dpeT.surface),
          dateEtablissement: str(dpeT.date_etablissement_dpe),
        }
      : null,
    siretsLies: Array.isArray(raw.sirets_lies)
      ? (raw.sirets_lies as unknown[]).map((s) => String(s))
      : [],
  };
}

async function bdnbFetch(path: string, params: Record<string, string>): Promise<BdnbApiResponse | null> {
  const url = new URL(`${BDNB_BASE}${path}`);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);

  const cacheKey = `bdnb:${path}:${url.searchParams.toString()}`;
  const hit = cacheGet<BdnbApiResponse | null>(cacheKey);
  if (hit !== null) return hit;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQ_TIMEOUT_MS);
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 7 * 24 * 3600 },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) {
      cacheSet(cacheKey, null, 60 * 60 * 1000);
      return null;
    }
    const json = (await res.json()) as BdnbApiResponse;
    cacheSet(cacheKey, json, CACHE_TTL_MS);
    return json;
  } catch {
    return null;
  }
}

/**
 * Recherche les bâtiments BDNB autour d'un point. Utilisé après géocodage BAN
 * pour récupérer la fiche du bâtiment à l'adresse saisie.
 *
 * @param lat latitude WGS84
 * @param lon longitude WGS84
 * @param radiusM rayon de recherche en mètres (défaut 30m — emprise bâtiment)
 */
export async function fetchBdnbAround(opts: {
  lat: number;
  lon: number;
  radiusM?: number;
  limit?: number;
}): Promise<BdnbBuilding[]> {
  const { lat, lon, radiusM = 30, limit = 5 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return [];

  // L'API BDNB accepte un filtre géo via PostGIS-style ; on tente
  // une recherche par bbox simple (lat±delta, lon±delta) car le
  // paramètre exact varie selon les endpoints publics ouverts.
  const deltaLat = radiusM / 111_320;
  const deltaLon = radiusM / (111_320 * Math.cos((lat * Math.PI) / 180));
  const bbox = `${(lon - deltaLon).toFixed(6)},${(lat - deltaLat).toFixed(6)},${(lon + deltaLon).toFixed(6)},${(lat + deltaLat).toFixed(6)}`;

  const json = await bdnbFetch("/batiment_groupe", {
    bbox,
    limit: String(limit),
  });
  if (!json?.data) return [];
  return json.data.map(mapBuilding).filter((b): b is BdnbBuilding => b !== null);
}

/**
 * Récupère la fiche détaillée d'un bâtiment BDNB par son identifiant.
 */
export async function fetchBdnbById(batimentGroupeId: string): Promise<BdnbBuilding | null> {
  if (!batimentGroupeId) return null;
  const json = await bdnbFetch(`/batiment_groupe/${encodeURIComponent(batimentGroupeId)}`, {});
  if (!json?.data || !json.data[0]) return null;
  return mapBuilding(json.data[0]);
}

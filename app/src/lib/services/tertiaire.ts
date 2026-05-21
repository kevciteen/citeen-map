/**
 * Service Tertiaire — orchestre l'enrichissement d'une adresse tertiaire.
 *
 * Chaîne :
 *   1. BAN → géocode l'adresse
 *   2. Cadastre IGN → parcelle (parallèle)
 *   3. DB locale (80k bâtiments IDF importés) → DPE tertiaire le plus proche
 *      du point géocodé. C'est la source PRINCIPALE (rapide, fiable).
 *   4. Si rien en DB → ADEME live (fallback, plus lent, 80m radius)
 *   5. BDNB → best-effort (peut être vide, ne bloque pas)
 *   6. Recherche d'entreprises → occupants à l'adresse
 *
 * Le **propriétaire foncier** n'est pas accessible sans convention DV3F/Cerema —
 * on retourne donc les **occupants** (sociétés à l'adresse), qui sont la cible
 * commerciale réaliste pour la rénovation énergétique tertiaire.
 */
import { db } from "../db/client";
import { geocodeAddress, type GeocodeResult } from "./ban";
import { getParcelByPoint, type Parcelle } from "./cadastre";
import { fetchBdnbAround, type BdnbBuilding } from "./bdnb";
import {
  fetchDpeTertiaireAround,
  extractLatLon,
  type DpeTertiaireRecord,
} from "./dpe-tertiaire";
import {
  searchEntreprisesAtAddress,
  type EntrepriseAtAddress,
} from "./entreprises";

export type TertiaireLookupResult = {
  query: string;
  geocode: GeocodeResult | null;
  parcelle: Parcelle | null;
  bdnb: BdnbBuilding | null;
  dpeTertiaire: DpeTertiaireRecord | null;
  occupants: EntrepriseAtAddress[];
  /** Bâtiment trouvé en DB locale (id à utiliser pour /api/tertiaire/[id]) */
  buildingId: number | null;
  /** Liste des bâtiments DB proches du point (utile pour proposer un choix) */
  nearbyBuildings: Array<{
    id: number;
    label: string | null;
    adresse: string | null;
    secteur: string | null;
    lat: number;
    lon: number;
    distanceM: number;
    etiquette_dpe: string | null;
  }>;
  diagnostics: {
    sourceDpe: "db" | "bdnb" | "ademe" | "none";
    bdnbCandidates: number;
    dpeCandidates: number;
    occupantsCount: number;
    dbBuildings: number;
  };
};

function pickBestDpe(records: DpeTertiaireRecord[]): DpeTertiaireRecord | null {
  if (!records.length) return null;
  const sorted = [...records].sort((a, b) => {
    const da = Date.parse(String(a.date_etablissement_dpe ?? ""));
    const db = Date.parse(String(b.date_etablissement_dpe ?? ""));
    return (Number.isFinite(db) ? db : 0) - (Number.isFinite(da) ? da : 0);
  });
  return sorted[0];
}

function haversine(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const R = 6371000;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const lat1 = (a.lat * Math.PI) / 180;
  const lat2 = (b.lat * Math.PI) / 180;
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

/**
 * Cherche les bâtiments tertiaires en DB autour d'un point.
 * Utilise un bbox approximatif puis filtre par distance Haversine réelle.
 */
async function findNearbyInDb(lat: number, lon: number, radiusM = 150): Promise<TertiaireLookupResult["nearbyBuildings"]> {
  const dLat = radiusM / 111_320;
  const dLon = radiusM / (111_320 * Math.cos((lat * Math.PI) / 180));
  type Row = {
    id: number;
    label: string | null;
    adresse: string | null;
    secteur: string | null;
    lat: number;
    lon: number;
    etiquette_dpe: string | null;
  };
  const rows = await db.all<Row>(
    `SELECT b.id, b.label, b.adresse, b.secteur, b.lat, b.lon, d.etiquette_dpe
     FROM tertiary_buildings b LEFT JOIN tertiary_dpe d ON d.building_id = b.id
     WHERE b.lat BETWEEN ? AND ? AND b.lon BETWEEN ? AND ?
     LIMIT 100`,
    [lat - dLat, lat + dLat, lon - dLon, lon + dLon],
  );
  return rows
    .map((r) => ({
      ...r,
      distanceM: haversine({ lat, lon }, { lat: r.lat, lon: r.lon }),
    }))
    .filter((r) => r.distanceM <= radiusM)
    .sort((a, b) => a.distanceM - b.distanceM);
}

/**
 * Convertit un bâtiment DB + son DPE en `DpeTertiaireRecord` pour rester
 * compatible avec le contrat existant côté UI.
 */
async function dbBuildingToDpeRecord(buildingId: number): Promise<DpeTertiaireRecord | null> {
  type Row = {
    numero_dpe: string | null;
    etiquette_dpe: string | null;
    etiquette_ges: string | null;
    conso_energie_primaire: number | null;
    emissions_ges: number | null;
    surface_utile: number | null;
    type_usage_dpe: string | null;
    date_etablissement: number | null;
    adresse: string | null;
    code_postal: string | null;
    commune: string | null;
    annee_construction: number | null;
  };
  const r = await db.get<Row>(
    `SELECT d.numero_dpe, d.etiquette_dpe, d.etiquette_ges, d.conso_energie_primaire,
            d.emissions_ges, d.surface_utile, d.type_usage_dpe, d.date_etablissement,
            b.adresse, b.code_postal, b.commune, b.annee_construction
     FROM tertiary_dpe d JOIN tertiary_buildings b ON b.id = d.building_id
     WHERE d.building_id = ?`,
    [buildingId],
  );
  if (!r) return null;
  return {
    numero_dpe: r.numero_dpe ?? undefined,
    classe_consommation_energie: r.etiquette_dpe ?? undefined,
    classe_estimation_ges: r.etiquette_ges ?? undefined,
    consommation_energie: r.conso_energie_primaire ?? undefined,
    estimation_ges: r.emissions_ges ?? undefined,
    surface_utile: r.surface_utile ?? undefined,
    secteur_activite: r.type_usage_dpe ?? undefined,
    geo_adresse: r.adresse ?? undefined,
    code_postal: r.code_postal ?? undefined,
    commune: r.commune ?? undefined,
    annee_construction: r.annee_construction ?? undefined,
    date_etablissement_dpe: r.date_etablissement
      ? new Date(r.date_etablissement * 1000).toISOString()
      : undefined,
  };
}

/**
 * Enrichit une adresse tertiaire avec toutes les sources disponibles.
 */
export async function lookupTertiaireByAddress(query: string): Promise<TertiaireLookupResult> {
  const q = query.trim();
  const empty: TertiaireLookupResult = {
    query: q,
    geocode: null,
    parcelle: null,
    bdnb: null,
    dpeTertiaire: null,
    occupants: [],
    buildingId: null,
    nearbyBuildings: [],
    diagnostics: { sourceDpe: "none", bdnbCandidates: 0, dpeCandidates: 0, occupantsCount: 0, dbBuildings: 0 },
  };
  if (!q) return empty;

  // 1. Géocodage
  const geos = await geocodeAddress(q, { limit: 1 });
  const geo = geos[0] ?? null;
  if (!geo) return empty;

  // 2-3. Parcelle + DB (nearby) en parallèle. BDNB / ADEME / SIRENE en parallèle
  //      mais isolés des erreurs pour ne pas bloquer le résultat.
  const safe = <T,>(p: Promise<T>, fallback: T): Promise<T> =>
    p.catch(() => fallback);

  const [parcelle, nearbyDb, bdnbList, dpeListLive, occupants] = await Promise.all([
    safe(getParcelByPoint(geo.lat, geo.lon), null),
    findNearbyInDb(geo.lat, geo.lon, 150),
    safe(fetchBdnbAround({ lat: geo.lat, lon: geo.lon, radiusM: 30, limit: 5 }), []),
    safe(fetchDpeTertiaireAround({ lat: geo.lat, lon: geo.lon, radiusM: 80, size: 50 }), [] as DpeTertiaireRecord[]),
    safe(searchEntreprisesAtAddress({
      q,
      codeInsee: geo.citycode,
      codePostal: geo.postcode,
      limit: 50,
    }), [] as EntrepriseAtAddress[]),
  ]);

  const bdnb = bdnbList[0] ?? null;

  // Stratégie DPE (priorité décroissante) :
  //   1. DB locale (le plus proche < 60m)
  //   2. BDNB (si on a un numero_dpe + match avec ADEME live)
  //   3. ADEME live (fallback < 50m)
  let dpeTertiaire: DpeTertiaireRecord | null = null;
  let sourceDpe: "db" | "bdnb" | "ademe" | "none" = "none";
  let buildingId: number | null = null;

  const closestDb = nearbyDb.find((b) => b.distanceM <= 60) ?? nearbyDb[0];
  if (closestDb && closestDb.distanceM <= 60) {
    buildingId = closestDb.id;
    const fromDb = await dbBuildingToDpeRecord(closestDb.id);
    if (fromDb) {
      dpeTertiaire = fromDb;
      sourceDpe = "db";
    }
  }

  if (!dpeTertiaire && bdnb?.dpeTertiaire?.numeroDpe) {
    const match = dpeListLive.find((d) => d.numero_dpe === bdnb.dpeTertiaire?.numeroDpe);
    if (match) {
      dpeTertiaire = match;
      sourceDpe = "bdnb";
    }
  }
  if (!dpeTertiaire) {
    const close = dpeListLive.filter((d) => {
      const ll = extractLatLon(d);
      if (!ll) return true;
      return haversine({ lat: geo.lat, lon: geo.lon }, ll) <= 50;
    });
    const best = pickBestDpe(close.length ? close : dpeListLive);
    if (best) {
      dpeTertiaire = best;
      sourceDpe = "ademe";
    }
  }

  return {
    query: q,
    geocode: geo,
    parcelle,
    bdnb,
    dpeTertiaire,
    occupants,
    buildingId,
    nearbyBuildings: nearbyDb.slice(0, 20),
    diagnostics: {
      sourceDpe,
      bdnbCandidates: bdnbList.length,
      dpeCandidates: dpeListLive.length,
      occupantsCount: occupants.length,
      dbBuildings: nearbyDb.length,
    },
  };
}

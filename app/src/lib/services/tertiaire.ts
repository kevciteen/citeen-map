/**
 * Service Tertiaire — orchestre l'enrichissement d'une adresse tertiaire.
 *
 * Chaîne : BAN (géocode) → Cadastre IGN (parcelle) → BDNB (fiche bâtiment +
 * DPE tertiaire embarqué) → DPE tertiaire ADEME (fallback / complément) →
 * Recherche d'entreprises (occupants à l'adresse).
 *
 * Le **propriétaire foncier** n'est pas accessible sans convention DV3F/Cerema —
 * on retourne donc les **occupants** (sociétés à l'adresse), qui sont la cible
 * commerciale réaliste pour la rénovation énergétique tertiaire.
 */
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
  diagnostics: {
    sourceDpe: "bdnb" | "ademe" | "none";
    bdnbCandidates: number;
    dpeCandidates: number;
    occupantsCount: number;
  };
};

/**
 * Choisit le meilleur DPE tertiaire ADEME : le plus récent dans un rayon donné.
 */
function pickBestDpe(records: DpeTertiaireRecord[]): DpeTertiaireRecord | null {
  if (!records.length) return null;
  const sorted = [...records].sort((a, b) => {
    const da = Date.parse(String(a.date_derniere_modification_dpe ?? a.date_etablissement_dpe ?? ""));
    const db = Date.parse(String(b.date_derniere_modification_dpe ?? b.date_etablissement_dpe ?? ""));
    return (Number.isFinite(db) ? db : 0) - (Number.isFinite(da) ? da : 0);
  });
  return sorted[0];
}

/**
 * Distance Haversine en mètres entre deux points WGS84.
 */
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
 * Enrichit une adresse tertiaire avec toutes les sources disponibles.
 */
export async function lookupTertiaireByAddress(query: string): Promise<TertiaireLookupResult> {
  const q = query.trim();
  if (!q) {
    return {
      query: q,
      geocode: null,
      parcelle: null,
      bdnb: null,
      dpeTertiaire: null,
      occupants: [],
      diagnostics: { sourceDpe: "none", bdnbCandidates: 0, dpeCandidates: 0, occupantsCount: 0 },
    };
  }

  // 1. Géocodage
  const geos = await geocodeAddress(q, { limit: 1 });
  const geo = geos[0] ?? null;
  if (!geo) {
    return {
      query: q,
      geocode: null,
      parcelle: null,
      bdnb: null,
      dpeTertiaire: null,
      occupants: [],
      diagnostics: { sourceDpe: "none", bdnbCandidates: 0, dpeCandidates: 0, occupantsCount: 0 },
    };
  }

  // 2-3-4. Cadastre + BDNB + DPE tertiaire + occupants en parallèle
  const [parcelle, bdnbList, dpeList, occupants] = await Promise.all([
    getParcelByPoint(geo.lat, geo.lon),
    fetchBdnbAround({ lat: geo.lat, lon: geo.lon, radiusM: 30, limit: 5 }),
    fetchDpeTertiaireAround({ lat: geo.lat, lon: geo.lon, radiusM: 80, size: 50 }),
    searchEntreprisesAtAddress({
      q,
      codeInsee: geo.citycode,
      codePostal: geo.postcode,
      limit: 50,
    }),
  ]);

  // Garder la fiche BDNB la plus proche du point (premier de la liste)
  const bdnb = bdnbList[0] ?? null;

  // Stratégie DPE : préférer celui de la BDNB (déjà consolidé), sinon
  // tomber sur ADEME pour le plus récent.
  let dpeTertiaire: DpeTertiaireRecord | null = null;
  let sourceDpe: "bdnb" | "ademe" | "none" = "none";

  if (bdnb?.dpeTertiaire?.numeroDpe) {
    // On enrichit avec le DPE ADEME complet si on a le numéro (pour avoir les conso)
    const match = dpeList.find((d) => d.numero_dpe === bdnb.dpeTertiaire?.numeroDpe);
    if (match) {
      dpeTertiaire = match;
      sourceDpe = "bdnb";
    }
  }
  if (!dpeTertiaire) {
    // Garde uniquement les DPE à moins de 50m du point géocodé pour limiter le bruit
    const close = dpeList.filter((d) => {
      const ll = extractLatLon(d);
      if (!ll) return true;
      return haversine({ lat: geo.lat, lon: geo.lon }, ll) <= 50;
    });
    dpeTertiaire = pickBestDpe(close.length ? close : dpeList);
    if (dpeTertiaire) sourceDpe = "ademe";
  }

  return {
    query: q,
    geocode: geo,
    parcelle,
    bdnb,
    dpeTertiaire,
    occupants,
    diagnostics: {
      sourceDpe,
      bdnbCandidates: bdnbList.length,
      dpeCandidates: dpeList.length,
      occupantsCount: occupants.length,
    },
  };
}

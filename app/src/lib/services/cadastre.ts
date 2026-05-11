/**
 * Cadastre IGN — récupère les parcelles cadastrales (vérité juridique
 * de l'emplacement) pour valider les correspondances DPE/BAN.
 *
 * Doc : https://apicarto.ign.fr/api/doc/cadastre
 */
import { cacheGet, cacheSet } from "./cache";

const APICARTO_BASE = "https://apicarto.ign.fr/api/cadastre/parcelle";

export type Parcelle = {
  idu: string;             // identifiant unique (ex: "930630000T0308")
  code_insee: string;      // code INSEE commune
  nom_commune: string | null;
  section: string;         // ex: "0T", "AB", "BA"
  numero: string;          // numéro parcelle (ex: "0308")
  contenance_m2: number;   // surface en m²
  centroidLat: number;     // centroïde polygone (= centre approx du bâtiment)
  centroidLon: number;
};

function polygonCentroid(ring: number[][]): { lat: number; lon: number } {
  let cx = 0,
    cy = 0,
    area = 0;
  const n = ring.length;
  for (let i = 0; i < n - 1; i++) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[i + 1];
    const c = x1 * y2 - x2 * y1;
    area += c;
    cx += (x1 + x2) * c;
    cy += (y1 + y2) * c;
  }
  area /= 2;
  if (Math.abs(area) < 1e-12) {
    // Polygon dégénéré — moyenne arithmétique
    let sumX = 0,
      sumY = 0,
      k = 0;
    for (const [x, y] of ring) {
      sumX += x;
      sumY += y;
      k++;
    }
    return { lat: sumY / k, lon: sumX / k };
  }
  cx /= 6 * area;
  cy /= 6 * area;
  return { lat: cy, lon: cx };
}

function extractCentroid(geom: GeoJSON.Geometry): { lat: number; lon: number } | null {
  if (!geom) return null;
  if (geom.type === "Polygon") {
    return polygonCentroid(geom.coordinates[0]);
  }
  if (geom.type === "MultiPolygon") {
    // prend le plus grand polygone (en nombre de points)
    const polys = geom.coordinates;
    const biggest = polys.reduce(
      (acc, p) => (p[0].length > acc[0].length ? p : acc),
      polys[0],
    );
    return polygonCentroid(biggest[0]);
  }
  return null;
}

function featureToParcelle(f: GeoJSON.Feature): Parcelle | null {
  const p = (f.properties ?? {}) as Record<string, unknown>;
  const idu = String(p.idu ?? "");
  if (!idu) return null;
  const centroid = extractCentroid(f.geometry as GeoJSON.Geometry);
  if (!centroid) return null;
  return {
    idu,
    code_insee: String(p.code_insee ?? ""),
    nom_commune: (p.nom_com as string) ?? null,
    section: String(p.section ?? ""),
    numero: String(p.numero ?? ""),
    contenance_m2: Number(p.contenance ?? 0),
    centroidLat: centroid.lat,
    centroidLon: centroid.lon,
  };
}

export async function getParcelByPoint(
  lat: number,
  lon: number,
): Promise<Parcelle | null> {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const key = `cadastre:point:${lat.toFixed(6)}|${lon.toFixed(6)}`;
  const hit = cacheGet<Parcelle | null>(key);
  if (hit !== null) return hit;

  const geom = JSON.stringify({ type: "Point", coordinates: [lon, lat] });
  const url = `${APICARTO_BASE}?geom=${encodeURIComponent(geom)}`;
  try {
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 7 * 24 * 3600 }, // cadastre = stable, 7 jours OK
    });
    if (!res.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const j = (await res.json()) as GeoJSON.FeatureCollection;
    const f = (j.features ?? [])[0];
    const parcelle = f ? featureToParcelle(f) : null;
    cacheSet(key, parcelle, 24 * 3600 * 1000);
    return parcelle;
  } catch {
    return null;
  }
}

export async function getParcelByRef(
  codeInsee: string,
  section: string,
  numero: string,
): Promise<Parcelle | null> {
  if (!codeInsee || !section || !numero) return null;
  const key = `cadastre:ref:${codeInsee}|${section}|${numero}`;
  const hit = cacheGet<Parcelle | null>(key);
  if (hit !== null) return hit;

  const url = `${APICARTO_BASE}?code_insee=${codeInsee}&section=${section}&numero=${numero}`;
  try {
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 7 * 24 * 3600 },
    });
    if (!res.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const j = (await res.json()) as GeoJSON.FeatureCollection;
    const f = (j.features ?? [])[0];
    const parcelle = f ? featureToParcelle(f) : null;
    cacheSet(key, parcelle, 24 * 3600 * 1000);
    return parcelle;
  } catch {
    return null;
  }
}

/**
 * Pour un copro donné, détermine la parcelle de référence avec
 * cette priorité :
 *   1. Référence cadastrale stockée (registre) — si elle existe encore
 *   2. Point-in-polygon avec lat/lon stockée — fallback fiable
 */
export async function resolveCoproParcel(opts: {
  lat: number | null;
  lon: number | null;
  codeInseeCommune: string | null;
  section: string | null;
  numeroParcelle: string | null;
}): Promise<{ parcelle: Parcelle | null; method: "ref" | "point" | "none" }> {
  if (opts.codeInseeCommune && opts.section && opts.numeroParcelle) {
    const p = await getParcelByRef(
      opts.codeInseeCommune,
      opts.section,
      opts.numeroParcelle,
    );
    if (p) return { parcelle: p, method: "ref" };
  }
  if (opts.lat != null && opts.lon != null) {
    const p = await getParcelByPoint(opts.lat, opts.lon);
    if (p) return { parcelle: p, method: "point" };
  }
  return { parcelle: null, method: "none" };
}

/**
 * Géorisques — API publique gouvernementale qui liste les risques naturels
 * et technologiques par commune.
 *
 * Endpoint : https://www.georisques.gouv.fr/api/v1/gaspar/risques
 *
 * Critère qualifiant pour la prospection rénovation énergétique :
 *  - Risque argile = enveloppe potentiellement fragile (gros œuvre + isolation)
 *  - Radon = ventilation à revoir
 *  - Inondation = exclusion de certaines aides ou primes
 *  - Sismique = renforcement structurel
 */
import { cacheGet, cacheSet } from "./cache";

const GEORISQUES_BASE = "https://www.georisques.gouv.fr/api/v1/gaspar/risques";
const TIMEOUT_MS = 12000;

export type GeorisqueDetail = {
  num_risque: string;
  libelle_risque_long: string;
  zone_sismicite: string | null;
};

export type GeorisquesResponse = {
  code_insee: string;
  libelle_commune: string;
  risques: GeorisqueDetail[];
  total: number;
};

/**
 * Récupère la liste des risques pour une commune (par code INSEE).
 * Cache 7j (les risques évoluent rarement).
 */
export async function fetchGeorisquesByInsee(
  codeInsee: string,
): Promise<GeorisquesResponse | null> {
  if (!codeInsee || codeInsee.length !== 5) return null;
  const key = `georisques:insee:${codeInsee}`;
  const hit = cacheGet<GeorisquesResponse | null>(key);
  if (hit !== null) return hit;

  const url = new URL(GEORISQUES_BASE);
  url.searchParams.set("code_insee", codeInsee);

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 7 * 24 * 3600 },
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (!res.ok) {
      cacheSet(key, null, 60 * 60 * 1000);
      return null;
    }
    const json = (await res.json()) as {
      data?: Array<{
        risques_detail?: GeorisqueDetail[];
        code_insee?: string;
        libelle_commune?: string;
      }>;
    };
    const d = json.data?.[0];
    if (!d) {
      cacheSet(key, null, 24 * 3600 * 1000);
      return null;
    }
    const result: GeorisquesResponse = {
      code_insee: d.code_insee ?? codeInsee,
      libelle_commune: d.libelle_commune ?? "",
      risques: d.risques_detail ?? [],
      total: (d.risques_detail ?? []).length,
    };
    cacheSet(key, result, 7 * 24 * 3600 * 1000);
    return result;
  } catch {
    return null;
  }
}

/** Catégorise les risques pour un affichage UI plus parlant. */
export type GeorisqueCategory =
  | "inondation"
  | "mouvement_terrain"
  | "sismique"
  | "argile"
  | "radon"
  | "industriel"
  | "transport_matieres"
  | "rupture_barrage"
  | "feux_foret"
  | "nucleaire"
  | "autre";

export function categorize(libelle: string): GeorisqueCategory {
  const s = libelle.toLowerCase();
  if (s.includes("inondation") || s.includes("crue") || s.includes("submersion")) return "inondation";
  if (s.includes("mouvement") || s.includes("affaissement") || s.includes("éboulement") || s.includes("eboulement") || s.includes("chute de bloc")) return "mouvement_terrain";
  if (s.includes("sismi") || s.includes("séisme") || s.includes("seisme")) return "sismique";
  if (s.includes("argile") || s.includes("retrait")) return "argile";
  if (s.includes("radon")) return "radon";
  if (s.includes("industriel") || s.includes("seveso") || s.includes("icpe")) return "industriel";
  if (s.includes("matières dangereuses") || s.includes("transport") || s.includes("pipeline")) return "transport_matieres";
  if (s.includes("barrage") || s.includes("rupture")) return "rupture_barrage";
  if (s.includes("foret") || s.includes("forêt") || s.includes("feux")) return "feux_foret";
  if (s.includes("nucléaire") || s.includes("nucleaire") || s.includes("radiologique")) return "nucleaire";
  return "autre";
}

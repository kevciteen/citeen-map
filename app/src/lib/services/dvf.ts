/**
 * DVF — Demandes de Valeurs Foncières (Etalab, data.gouv.fr)
 *
 * Historique des transactions immobilières publiées par la DGFiP, anonymisé
 * côté acheteur/vendeur. On l'utilise pour :
 *   1. Afficher l'historique des ventes d'une maison (date + prix + surface)
 *   2. Repérer les maisons fraîchement vendues (= cible chaude pour rénovation)
 *
 * Source API : https://api.cquest.org/dvf  (proxy Etalab maintenu par
 * Christian Quest, sans clé, cache CDN). Doc : https://github.com/cquest/dvf
 *
 * Note : pas de nom d'acheteur ni de vendeur — la CNIL impose l'anonymisation.
 */
import { cacheGet, cacheSet } from "./cache";

const API_BASE = "https://api.cquest.org/dvf";

export type DvfTransaction = {
  id_mutation: string;
  date_mutation: string;          // ISO date "YYYY-MM-DD"
  nature_mutation: string;        // "Vente", "Vente en l'état futur d'achèvement", etc.
  valeur_fonciere: number | null; // €
  type_local: string;             // "Maison", "Appartement", "Dépendance", "Local industriel..."
  surface_reelle_bati: number | null; // m²
  nombre_pieces_principales: number | null;
  surface_terrain: number | null; // m²
  // Adresse
  adresse_numero: string | null;
  adresse_suffixe: string | null;
  adresse_nom_voie: string | null;
  code_postal: string | null;
  nom_commune: string | null;
  // Cadastre
  code_commune: string | null;
  section_prefixe: string | null;
  section: string | null;
  numero_plan: string | null;
  // Géo
  lat: number | null;
  lon: number | null;
  // Dérivés
  prix_m2: number | null;
};

type ApiResult = {
  id_mutation?: string;
  date_mutation?: string;
  nature_mutation?: string;
  valeur_fonciere?: string | number;
  type_local?: string;
  surface_reelle_bati?: string | number;
  nombre_pieces_principales?: string | number;
  surface_terrain?: string | number;
  adresse_numero?: string;
  adresse_suffixe?: string;
  adresse_nom_voie?: string;
  code_postal?: string;
  nom_commune?: string;
  code_commune?: string;
  section_prefixe?: string;
  section?: string;
  numero_plan?: string;
  lat?: string | number;
  lon?: string | number;
};

type ApiResponse = {
  nb_resultats?: number;
  resultats?: ApiResult[];
};

function toNumber(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
  return Number.isFinite(n) ? n : null;
}

function toString(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v).trim() || null;
}

function normalize(v: unknown): string {
  return String(v ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function mapResult(r: ApiResult): DvfTransaction {
  const valeur = toNumber(r.valeur_fonciere);
  const surface = toNumber(r.surface_reelle_bati);
  return {
    id_mutation: String(r.id_mutation ?? ""),
    date_mutation: String(r.date_mutation ?? ""),
    nature_mutation: String(r.nature_mutation ?? ""),
    valeur_fonciere: valeur,
    type_local: String(r.type_local ?? ""),
    surface_reelle_bati: surface,
    nombre_pieces_principales: toNumber(r.nombre_pieces_principales),
    surface_terrain: toNumber(r.surface_terrain),
    adresse_numero: toString(r.adresse_numero),
    adresse_suffixe: toString(r.adresse_suffixe),
    adresse_nom_voie: toString(r.adresse_nom_voie),
    code_postal: toString(r.code_postal),
    nom_commune: toString(r.nom_commune),
    code_commune: toString(r.code_commune),
    section_prefixe: toString(r.section_prefixe),
    section: toString(r.section),
    numero_plan: toString(r.numero_plan),
    lat: toNumber(r.lat),
    lon: toNumber(r.lon),
    prix_m2: valeur && surface && surface > 0 ? Math.round(valeur / surface) : null,
  };
}

/**
 * Une mutation peut générer plusieurs lignes (un même acte qui vend maison +
 * dépendance + terrain). On regroupe par id_mutation en gardant la ligne avec
 * la plus grande surface bâtie + type_local "Maison".
 */
function dedupeByMutation(rows: DvfTransaction[]): DvfTransaction[] {
  const best = new Map<string, DvfTransaction>();
  for (const r of rows) {
    if (!r.id_mutation) continue;
    const prev = best.get(r.id_mutation);
    if (!prev) {
      best.set(r.id_mutation, r);
      continue;
    }
    const prevIsMaison = /maison/i.test(prev.type_local);
    const curIsMaison = /maison/i.test(r.type_local);
    if (curIsMaison && !prevIsMaison) {
      best.set(r.id_mutation, r);
      continue;
    }
    if (curIsMaison === prevIsMaison) {
      const a = r.surface_reelle_bati ?? 0;
      const b = prev.surface_reelle_bati ?? 0;
      if (a > b) best.set(r.id_mutation, r);
    }
  }
  return [...best.values()];
}

/**
 * Récupère les transactions DVF autour d'un point géographique.
 *
 * @param lat latitude
 * @param lon longitude
 * @param distMeters rayon en mètres (cquest accepte ~5..500m)
 */
export async function fetchDvfAround(opts: {
  lat: number;
  lon: number;
  distMeters?: number;
}): Promise<DvfTransaction[]> {
  const { lat, lon, distMeters = 30 } = opts;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return [];

  const key = `dvf:point:${lat.toFixed(6)}|${lon.toFixed(6)}|${distMeters}`;
  const hit = cacheGet<DvfTransaction[]>(key);
  if (hit) return hit;

  const url = new URL(API_BASE);
  url.searchParams.set("lat", String(lat));
  url.searchParams.set("lon", String(lon));
  url.searchParams.set("dist", String(distMeters));

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      signal: controller.signal,
      // DVF est publié tous les 6 mois — cache 24h côté Next/edge
      next: { revalidate: 24 * 3600 },
    });
    clearTimeout(timeout);
    if (!res.ok) {
      cacheSet(key, [], 60 * 60 * 1000);
      return [];
    }
    const json = (await res.json()) as ApiResponse;
    const rows = (json.resultats ?? []).map(mapResult);
    const dedup = dedupeByMutation(rows);
    cacheSet(key, dedup, 24 * 3600 * 1000);
    return dedup;
  } catch {
    return [];
  }
}

/**
 * Filtre les transactions DVF pour ne garder que celles qui correspondent
 * à une adresse précise (housenumber + voie). Utile pour la fiche d'une
 * maison individuelle.
 *
 * Le matching est tolérant : on compare le numéro (strict) et un overlap
 * de tokens sur le nom de voie (≥ 60%).
 */
export function filterDvfByAddress(
  rows: DvfTransaction[],
  opts: {
    housenumber: string | null;
    street: string | null;
    typeLocal?: "maison" | "appartement" | null;
  },
): DvfTransaction[] {
  const wantedNumero = (opts.housenumber ?? "").match(/\d+/)?.[0] ?? "";
  const wantedTokens = new Set(
    normalize(opts.street ?? "")
      .split(" ")
      .filter((t) => t.length >= 3),
  );
  const wantedType = opts.typeLocal ?? null;

  return rows.filter((r) => {
    if (wantedType === "maison" && !/maison/i.test(r.type_local)) return false;
    if (wantedType === "appartement" && !/appartement/i.test(r.type_local))
      return false;

    if (wantedNumero) {
      const recNum = (r.adresse_numero ?? "").match(/\d+/)?.[0] ?? "";
      if (recNum && recNum !== wantedNumero) return false;
      if (!recNum) return false;
    }

    if (wantedTokens.size > 0) {
      const recTokens = new Set(
        normalize(r.adresse_nom_voie ?? "")
          .split(" ")
          .filter((t) => t.length >= 3),
      );
      if (recTokens.size === 0) return false;
      let common = 0;
      for (const t of wantedTokens) if (recTokens.has(t)) common += 1;
      const overlap = common / Math.max(wantedTokens.size, 1);
      if (overlap < 0.6) return false;
    }

    return true;
  });
}

/**
 * Tri par date décroissante (plus récent en premier).
 */
export function sortByDateDesc(rows: DvfTransaction[]): DvfTransaction[] {
  return [...rows].sort(
    (a, b) =>
      Date.parse(b.date_mutation || "0") - Date.parse(a.date_mutation || "0"),
  );
}

/**
 * Helper utilitaire : la transaction est-elle récente (< Nans) ?
 */
export function isRecentSale(tx: DvfTransaction, years = 2): boolean {
  if (!tx.date_mutation) return false;
  const t = Date.parse(tx.date_mutation);
  if (!Number.isFinite(t)) return false;
  return t > Date.now() - years * 365.25 * 24 * 3600 * 1000;
}

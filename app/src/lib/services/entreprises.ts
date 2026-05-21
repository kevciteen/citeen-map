/**
 * API Recherche d'entreprises (annuaire-entreprises.data.gouv.fr).
 *
 * Endpoint : https://recherche-entreprises.api.gouv.fr/search
 * Libre, sans clé, ratelimit raisonnable (≈ 7 req/s par IP en pratique).
 *
 * Permet de lister toutes les sociétés actives à une adresse donnée pour
 * identifier les **occupants** d'un bâtiment tertiaire (= prospect principal).
 * Le propriétaire foncier (SCI, foncière) n'est pas exposé ici — il
 * faudrait DV3F / Fichiers fonciers Cerema, non accessibles sans convention.
 */
import { cacheGet, cacheSet } from "./cache";

const RE_BASE = "https://recherche-entreprises.api.gouv.fr/search";
const CACHE_TTL_MS = 24 * 3600 * 1000;
const REQ_TIMEOUT_MS = 12000;

export type EntrepriseAtAddress = {
  siret: string;
  siren: string;
  denomination: string | null;
  nafCode: string | null;
  nafLabel: string | null;
  trancheEffectif: string | null;
  adresseEnregistree: string | null;
  estSiege: boolean;
  estActif: boolean;
};

type RawResult = {
  siren?: string;
  nom_complete?: string;
  nom_raison_sociale?: string;
  nombre_etablissements_ouverts?: number;
  activite_principale?: string;
  section_activite_principale?: string;
  tranche_effectif_salarie?: string;
  matching_etablissements?: Array<{
    siret?: string;
    activite_principale?: string;
    libelle_activite_principale?: string;
    tranche_effectif_salarie?: string;
    adresse?: string;
    est_siege?: boolean;
    etat_administratif?: string;
  }>;
};

type RawResponse = {
  results?: RawResult[];
  total_results?: number;
};

function mapResultsToOccupants(json: RawResponse): EntrepriseAtAddress[] {
  const out: EntrepriseAtAddress[] = [];
  for (const r of json.results ?? []) {
    const siren = r.siren ?? "";
    const denomination = r.nom_complete ?? r.nom_raison_sociale ?? null;
    const naf = r.activite_principale ?? null;
    const trancheEffectif = r.tranche_effectif_salarie ?? null;

    // Un SIREN peut avoir plusieurs établissements (siret) matchant l'adresse.
    // On émet un occupant par SIRET retourné par `matching_etablissements`.
    const etabs = r.matching_etablissements ?? [];
    if (etabs.length === 0 && siren) {
      out.push({
        siret: siren + "00000",
        siren,
        denomination,
        nafCode: naf,
        nafLabel: null,
        trancheEffectif,
        adresseEnregistree: null,
        estSiege: false,
        estActif: true,
      });
      continue;
    }
    for (const e of etabs) {
      const siret = e.siret ?? "";
      if (!siret) continue;
      out.push({
        siret,
        siren,
        denomination,
        nafCode: e.activite_principale ?? naf,
        nafLabel: e.libelle_activite_principale ?? null,
        trancheEffectif: e.tranche_effectif_salarie ?? trancheEffectif,
        adresseEnregistree: e.adresse ?? null,
        estSiege: Boolean(e.est_siege),
        estActif: (e.etat_administratif ?? "A") === "A",
      });
    }
  }
  return out;
}

/**
 * Cherche les sociétés actives à une adresse donnée.
 *
 * @param opts.q chaîne libre (ex: "1 place de la Défense 92800")
 * @param opts.codeInsee filtre commune si fourni (recommandé pour réduire le bruit)
 * @param opts.limit max résultats (défaut 25, max 100)
 */
export async function searchEntreprisesAtAddress(opts: {
  q: string;
  codeInsee?: string;
  codePostal?: string;
  limit?: number;
}): Promise<EntrepriseAtAddress[]> {
  const q = opts.q?.trim();
  if (!q) return [];
  const limit = Math.min(opts.limit ?? 25, 100);

  const key = `recherche-entreprises:${q}|${opts.codeInsee ?? ""}|${opts.codePostal ?? ""}|${limit}`;
  const hit = cacheGet<EntrepriseAtAddress[]>(key);
  if (hit) return hit;

  const url = new URL(RE_BASE);
  url.searchParams.set("q", q);
  url.searchParams.set("page", "1");
  url.searchParams.set("per_page", String(limit));
  url.searchParams.set("etat_administratif", "A");
  if (opts.codeInsee) url.searchParams.set("code_commune", opts.codeInsee);
  if (opts.codePostal) url.searchParams.set("code_postal", opts.codePostal);

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQ_TIMEOUT_MS);
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 24 * 3600 },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) {
      cacheSet(key, [], 60 * 60 * 1000);
      return [];
    }
    const json = (await res.json()) as RawResponse;
    const occupants = mapResultsToOccupants(json);
    cacheSet(key, occupants, CACHE_TTL_MS);
    return occupants;
  } catch {
    return [];
  }
}

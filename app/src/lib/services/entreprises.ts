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
  dirigeants?: Dirigeant[];
};

function mapDirigeant(d: RawDirigeant): Dirigeant {
  const type = d.type_dirigeant?.toLowerCase().includes("moral")
    ? "morale"
    : d.type_dirigeant?.toLowerCase().includes("physi")
      ? "physique"
      : null;
  return {
    nom: d.nom ?? null,
    prenoms: d.prenoms ?? null,
    qualite: d.qualite ?? null,
    typeDirigeant: type,
    denominationMorale: d.denomination ?? null,
    sirenMorale: d.siren ?? null,
  };
}

type RawDirigeant = {
  nom?: string;
  prenoms?: string;
  qualite?: string;
  date_naissance?: string;
  type_dirigeant?: string; // "personne physique" | "personne morale"
  denomination?: string; // si personne morale
  siren?: string;
};

type RawEtablissement = {
  siret?: string;
  activite_principale?: string;
  libelle_activite_principale?: string;
  tranche_effectif_salarie?: string;
  adresse?: string;
  est_siege?: boolean;
  etat_administratif?: string;
};

type RawResult = {
  siren?: string;
  // L'API renvoie `nom_complet` (sans le e final) — on accepte les deux par sécurité
  nom_complet?: string;
  nom_complete?: string;
  nom_raison_sociale?: string;
  sigle?: string;
  nombre_etablissements_ouverts?: number;
  activite_principale?: string;
  section_activite_principale?: string;
  tranche_effectif_salarie?: string;
  dirigeants?: RawDirigeant[];
  siege?: RawEtablissement;
  matching_etablissements?: RawEtablissement[];
};

export type Dirigeant = {
  nom: string | null;
  prenoms: string | null;
  qualite: string | null;
  typeDirigeant: "physique" | "morale" | null;
  denominationMorale: string | null;
  sirenMorale: string | null;
};

type RawResponse = {
  results?: RawResult[];
  total_results?: number;
};

function mapResultsToOccupants(json: RawResponse): EntrepriseAtAddress[] {
  const out: EntrepriseAtAddress[] = [];
  for (const r of json.results ?? []) {
    const siren = r.siren ?? "";
    // L'API renvoie `nom_complet` (sans e) ; on garde aussi `nom_complete` au cas où
    const denomination =
      r.nom_complet ??
      r.nom_complete ??
      r.nom_raison_sociale ??
      r.sigle ??
      null;
    const naf = r.activite_principale ?? null;
    const trancheEffectif = r.tranche_effectif_salarie ?? null;
    const dirigeants = (r.dirigeants ?? []).map(mapDirigeant);

    // Priorité : matching_etablissements (filtrés par recherche), fallback siege seul
    const etabs = r.matching_etablissements ?? [];
    if (etabs.length === 0) {
      // Pas de matching → /near_point ne renvoie pas matching_etablissements,
      // il faut prendre le siège.
      const sg = r.siege;
      if (sg?.siret) {
        out.push({
          siret: sg.siret,
          siren,
          denomination,
          nafCode: sg.activite_principale ?? naf,
          nafLabel: null,
          trancheEffectif: sg.tranche_effectif_salarie ?? trancheEffectif,
          adresseEnregistree: sg.adresse ?? null,
          estSiege: Boolean(sg.est_siege),
          estActif: (sg.etat_administratif ?? "A") === "A",
          dirigeants,
        });
      } else if (siren) {
        // Vraiment rien → on garde quand même la société (siret synthétique)
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
          dirigeants,
        });
      }
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
        dirigeants,
      });
    }
  }
  return out;
}

/**
 * Normalise une chaîne d'adresse pour comparaison : minuscules, accents
 * supprimés, abbréviations standardisées (R/RUE, AV/AVENUE, etc.).
 */
function normalizeAddr(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extrait le numéro de rue (avec bis/ter) d'une chaîne d'adresse.
 */
function extractStreetNumber(s: string): string | null {
  const m = normalizeAddr(s).match(/^(\d+)\s*(bis|ter|quater)?/);
  return m ? `${m[1]}${m[2] ? " " + m[2] : ""}` : null;
}

const VOIE_REGEX = /\b(rue|avenue|av|boulevard|bd|place|pl|allee|allees|impasse|imp|chemin|ch|route|rte|cours|cour|square|sq|quai|passage|villa|cite|hameau|esplanade|parvis|rond[\s-]?point)\s+/;

/**
 * Extrait le nom de rue (après "rue/av/bd…" et avant le code postal).
 * Ex: "3 rue meyerbeer 75009 paris" → "meyerbeer"
 *     "parking meyerbeer 3 rue de la chaussee d antin 75009" → "de la chaussee d antin"
 */
function extractStreetName(s: string): string | null {
  const norm = normalizeAddr(s);
  // Enlève le code postal + commune en fin
  const sansCp = norm.replace(/\s+\d{5}\s+.+$/, "").trim();
  // Cherche le premier "rue/av/bd ... " et capture le reste de la chaîne (nom de la voie)
  const match = sansCp.match(new RegExp(VOIE_REGEX.source + "(.+)$"));
  if (!match) return null;
  // Le nom de rue = ce qui vient après "rue " (jusqu'à fin), nettoyé
  return match[2].trim();
}

/**
 * Vérifie qu'une adresse SIRENE matche la query strictement.
 * Numéro doit matcher (si présent dans query).
 * Nom de rue (après "rue/av/bd…") doit matcher EXACTEMENT (mêmes tokens
 * significatifs, dans l'ordre).
 */
function matchesAddressStrict(sireneAddr: string | null, queryAddr: string): boolean {
  if (!sireneAddr) return false;
  const queryNum = extractStreetNumber(queryAddr);
  const queryStreet = extractStreetName(queryAddr);
  const sireneNum = extractStreetNumber(sireneAddr);
  const sireneStreet = extractStreetName(sireneAddr);

  if (queryNum && sireneNum && queryNum !== sireneNum) return false;
  if (!queryStreet || !sireneStreet) return false;

  // Tokens significatifs (mots de la rue, hors "de/du/la/le/les/des/d/l")
  const stopWords = new Set(["de", "du", "la", "le", "les", "des", "d", "l", "et", "a", "au", "aux", "en"]);
  const tokens = (s: string) => s.split(" ").filter((t) => t.length >= 2 && !stopWords.has(t));
  const qt = tokens(queryStreet);
  const st = tokens(sireneStreet);
  if (qt.length === 0 || st.length === 0) return false;

  // Pour matcher : TOUS les tokens significatifs de query doivent être dans
  // sirene ET vice-versa (égalité des ensembles de tokens significatifs).
  const qSet = new Set(qt);
  const sSet = new Set(st);
  if (qSet.size !== sSet.size) {
    // Si tailles différentes, l'un est sous-ensemble de l'autre → on accepte si
    // tous les tokens de query sont dans sirene (typo plus large dans SIRENE OK).
    return qt.every((t) => sSet.has(t));
  }
  return qt.every((t) => sSet.has(t));
}

/**
 * Cherche les sociétés actives à une adresse donnée.
 *
 * Stratégie multi-passe pour maximiser le rappel, puis filtre STRICT par
 * numéro + nom de rue pour ne garder que les vraies occupants à l'adresse.
 *
 * @param opts.strict (défaut true) : filtre strict sur l'adresse SIRENE.
 *   À false : retourne tout (utile pour debug, ou voir les voisins).
 */
export async function searchEntreprisesAtAddress(opts: {
  q: string;
  codeInsee?: string;
  codePostal?: string;
  lat?: number;
  lon?: number;
  limit?: number;
  strict?: boolean;
}): Promise<EntrepriseAtAddress[]> {
  const q = opts.q?.trim();
  if (!q && !(opts.lat && opts.lon)) return [];
  // L'API recherche-entreprises plafonne per_page à 25 — au-delà c'est HTTP 400.
  // On limite à 25 par requête et on déduplique entre les passes pour atteindre
  // un volume utile (généralement 25-60 sociétés différentes via les 4 passes).
  const userLimit = Math.min(opts.limit ?? 25, 100);
  const perPage = Math.min(userLimit, 25);

  const key = `recherche-entreprises:${q}|${opts.codeInsee ?? ""}|${opts.codePostal ?? ""}|${opts.lat ?? ""}|${opts.lon ?? ""}|${userLimit}`;
  const hit = cacheGet<EntrepriseAtAddress[]>(key);
  if (hit) return hit;

  const dedup = new Map<string, EntrepriseAtAddress>();

  const merge = (list: EntrepriseAtAddress[]) => {
    for (const e of list) {
      const id = e.siret || e.siren;
      if (id && !dedup.has(id)) dedup.set(id, e);
    }
  };

  const safeFetch = async (url: URL): Promise<RawResponse | null> => {
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
        if (process.env.DEBUG_SIRENE) {
          console.error(`[SIRENE] ${url.pathname} HTTP ${res.status} : ${await res.text().catch(() => "")}`);
        }
        return null;
      }
      return (await res.json()) as RawResponse;
    } catch (err) {
      if (process.env.DEBUG_SIRENE) {
        console.error(`[SIRENE] ${url.pathname} exception : ${(err as Error).message}`);
      }
      return null;
    }
  };

  // Pass 1 : near_point si lat/lon — rayon 100m (la majorité des immeubles tertiaires)
  if (opts.lat && opts.lon && Number.isFinite(opts.lat) && Number.isFinite(opts.lon)) {
    const url = new URL("https://recherche-entreprises.api.gouv.fr/near_point");
    url.searchParams.set("lat", String(opts.lat));
    url.searchParams.set("long", String(opts.lon));
    url.searchParams.set("radius", "0.1"); // 100m
    url.searchParams.set("page", "1");
    url.searchParams.set("per_page", String(perPage));
    url.searchParams.set("etat_administratif", "A");
    const json = await safeFetch(url);
    if (json) merge(mapResultsToOccupants(json));
  }

  // Pass 2 : recherche texte par adresse complète
  if (q) {
    const url = new URL(RE_BASE);
    url.searchParams.set("q", q);
    url.searchParams.set("page", "1");
    url.searchParams.set("per_page", String(perPage));
    url.searchParams.set("etat_administratif", "A");
    if (opts.codeInsee) url.searchParams.set("code_commune", opts.codeInsee);
    if (opts.codePostal) url.searchParams.set("code_postal", opts.codePostal);
    const json = await safeFetch(url);
    if (json) merge(mapResultsToOccupants(json));
  }

  // Pass 3 : variante "rue uniquement" (sans numéro) — capture les sociétés
  // dont l'adresse SIRENE diffère légèrement (variantes typographiques).
  if (q) {
    const sansNumero = q.replace(/^\d+\s*(bis|ter|quater)?\s*,?\s*/i, "").trim();
    if (sansNumero && sansNumero !== q && sansNumero.length >= 4) {
      const url = new URL(RE_BASE);
      url.searchParams.set("q", sansNumero);
      url.searchParams.set("page", "1");
      url.searchParams.set("per_page", String(perPage));
      url.searchParams.set("etat_administratif", "A");
      if (opts.codeInsee) url.searchParams.set("code_commune", opts.codeInsee);
      if (opts.codePostal) url.searchParams.set("code_postal", opts.codePostal);
      const json = await safeFetch(url);
      if (json) merge(mapResultsToOccupants(json));
    }
  }

  // Pass 4 : si vraiment rien trouvé via near_point + texte, élargir à 250m
  // (cas des adresses récentes/mal géocodées dans SIRENE)
  if (dedup.size === 0 && opts.lat && opts.lon) {
    const url = new URL("https://recherche-entreprises.api.gouv.fr/near_point");
    url.searchParams.set("lat", String(opts.lat));
    url.searchParams.set("long", String(opts.lon));
    url.searchParams.set("radius", "0.25"); // 250m
    url.searchParams.set("page", "1");
    url.searchParams.set("per_page", String(perPage));
    url.searchParams.set("etat_administratif", "A");
    const json = await safeFetch(url);
    if (json) merge(mapResultsToOccupants(json));
  }

  let occupants = [...dedup.values()];

  // Filtre strict : ne garde que les sociétés dont l'adresse SIRENE matche
  // précisément le numéro + nom de rue de la query.
  if (opts.strict !== false && q) {
    const filtered = occupants.filter((o) => matchesAddressStrict(o.adresseEnregistree, q));
    // Si le filtre strict élimine TOUT (cas d'adresses récentes ou mal indexées
    // dans SIRENE), on garde le near_point en fallback (≤ 10 résultats les plus
    // proches géographiquement).
    if (filtered.length > 0) {
      occupants = filtered;
    } else {
      occupants = occupants.slice(0, 10);
    }
  }

  cacheSet(key, occupants, CACHE_TTL_MS);
  return occupants;
}

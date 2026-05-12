/**
 * Résolution des coordonnées d'un syndic via l'API "Recherche Entreprises"
 * (data.gouv.fr) — gratuite, sans clé, basée sur Sirene + INPI.
 *
 * Doc : https://recherche-entreprises.api.gouv.fr/
 *
 * Stratégie :
 *   1. On cherche par raison sociale (le nom qu'on a dans copros.syndic)
 *   2. On filtre sur le code NAF 6832A (Administration d'immeubles & autres
 *      biens immobiliers) quand on a plusieurs matches — c'est le NAF des
 *      vrais syndics professionnels.
 *   3. On renvoie le siège social comme adresse principale.
 *
 * Le résultat est cacheable 24h via le revalidate de fetch Next.js.
 */
const API_BASE = "https://recherche-entreprises.api.gouv.fr/search";

// Code NAF historiquement utilisé par les syndics pro
const NAF_SYNDIC = "6832A";

export type SyndicContact = {
  source: "recherche-entreprises";
  nomComplet: string;
  siren: string;
  siretSiege: string | null;
  adresse: string | null;
  codePostal: string | null;
  commune: string | null;
  departement: string | null;
  codeApe: string | null;
  libelleApe: string | null;
  dirigeant: string | null;
  trancheEffectif: string | null;
  // Score de confiance heuristique [0..1]
  matchScore: number;
};

function pickBest(results: ApiResult[], query: string): ApiResult | null {
  if (results.length === 0) return null;

  // Priorité 1 : NAF syndic
  const syndicMatches = results.filter((r) => r.siege?.code_naf === NAF_SYNDIC);
  if (syndicMatches.length > 0) {
    // Match exact (case-insensitive, trimmed) si possible
    const qn = normalize(query);
    const exact = syndicMatches.find((r) => normalize(r.nom_complet) === qn);
    if (exact) return exact;
    // Sinon le premier (déjà trié par pertinence par l'API)
    return syndicMatches[0];
  }

  // Pas de match NAF — on prend le premier résultat
  return results[0];
}

function normalize(s: string | null | undefined): string {
  return (s ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function scoreMatch(api: ApiResult, query: string): number {
  const a = normalize(api.nom_complet);
  const q = normalize(query);
  if (!a || !q) return 0;
  if (a === q) return 1;
  if (a.includes(q) || q.includes(a)) return 0.85;
  // Mots en commun
  const aSet = new Set(a.split(" "));
  const qSet = new Set(q.split(" "));
  let common = 0;
  for (const t of qSet) if (aSet.has(t)) common++;
  return common / Math.max(qSet.size, 1) * 0.7;
}

type ApiSiege = {
  siret?: string;
  adresse?: string;
  code_postal?: string;
  libelle_commune?: string;
  departement?: string;
  code_naf?: string;
  libelle_naf?: string;
  tranche_effectif_salarie?: string;
};

type ApiResult = {
  nom_complet?: string;
  siren?: string;
  siege?: ApiSiege;
  dirigeants?: Array<{ nom?: string | null; prenoms?: string | null }>;
};

/**
 * Résout les coordonnées d'un syndic à partir de son nom.
 * Retourne null si pas de match raisonnable.
 */
export async function resolveSyndicByName(
  rawName: string,
): Promise<SyndicContact | null> {
  const name = rawName.trim();
  if (name.length < 3) return null;

  const url = new URL(API_BASE);
  url.searchParams.set("q", name);
  url.searchParams.set("page", "1");
  url.searchParams.set("per_page", "5");
  // Filtre uniquement les entreprises ACTIVES
  url.searchParams.set("etat_administratif", "A");

  const res = await fetch(url, {
    headers: { accept: "application/json" },
    next: { revalidate: 60 * 60 * 24 }, // 24h cache edge
  });
  if (!res.ok) return null;

  const data = (await res.json()) as { results?: ApiResult[]; total_results?: number };
  const results = data.results ?? [];
  const best = pickBest(results, name);
  if (!best) return null;

  const siege = best.siege ?? {};
  const dirigeant =
    best.dirigeants && best.dirigeants[0]
      ? [best.dirigeants[0].prenoms, best.dirigeants[0].nom]
          .filter(Boolean)
          .join(" ")
          .trim() || null
      : null;

  return {
    source: "recherche-entreprises",
    nomComplet: best.nom_complet ?? name,
    siren: best.siren ?? "",
    siretSiege: siege.siret ?? null,
    adresse: siege.adresse ?? null,
    codePostal: siege.code_postal ?? null,
    commune: siege.libelle_commune ?? null,
    departement: siege.departement ?? null,
    codeApe: siege.code_naf ?? null,
    libelleApe: siege.libelle_naf ?? null,
    dirigeant,
    trancheEffectif: siege.tranche_effectif_salarie ?? null,
    matchScore: scoreMatch(best, name),
  };
}

/**
 * Service maisons individuelles — couple BAN + ADEME + Cadastre IGN.
 *
 * Différence avec le service copro :
 *   - Pas de référentiel pré‑importé (search‑driven, à la demande)
 *   - 1 maison = 1 DPE (pas de moyenne pondérée à faire)
 *   - Filtre strict ADEME sur type_batiment = "maison"
 *   - Cadastre pour confirmer que le DPE est bien sur la même parcelle que l'adresse
 */
import { fetchAdemeDpeAround, type AdemeRecord } from "./ademe";
import { geocodeAddress } from "./ban";
import { getParcelByPoint, type Parcelle } from "./cadastre";

const MAISON_TYPES = new Set([
  "maison",
  "maison_individuelle",
  "maison individuelle",
  "MI",
]);

function normalizeType(t: unknown): string {
  return String(t ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z_ ]/g, "")
    .trim();
}

function isMaison(rec: AdemeRecord): boolean {
  const t = normalizeType(rec.type_batiment);
  if (!t) return false;
  return [...MAISON_TYPES].some((m) => t.includes(m));
}

function normalizeAscii(value: unknown): string {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function normalizeCompact(value: unknown): string {
  return normalizeAscii(value).replace(/\s+/g, "");
}

const VOIE_TYPE_MAP: Record<string, string> = {
  r: "rue", rue: "rue",
  av: "avenue", ave: "avenue", avenue: "avenue",
  bd: "boulevard", blvd: "boulevard", boulevard: "boulevard",
  pl: "place", place: "place",
  rte: "route", route: "route",
  all: "allee", allee: "allee",
  imp: "impasse", impasse: "impasse",
  ch: "chemin", chemin: "chemin",
  cour: "cour", cours: "cours",
  sq: "square", square: "square",
};

function extractVoieType(s: unknown): string | null {
  for (const t of normalizeAscii(s).split(/\s+/)) {
    const m = VOIE_TYPE_MAP[t];
    if (m) return m;
  }
  return null;
}

function extractHouseNumber(value: unknown): string | null {
  const m = normalizeAscii(value).match(/\b(\d{1,4})(?:\s*(bis|ter|quater))?\b/);
  return m ? `${m[1]}${m[2] || ""}` : null;
}

function streetTokens(value: unknown): Set<string> {
  const stop = new Set([
    "de", "du", "des", "d", "la", "le", "les", "l", "au", "aux", "et",
    "rue", "r", "avenue", "av", "ave", "boulevard", "bd", "blvd", "place", "pl",
    "route", "rte", "allee", "all", "impasse", "imp", "chemin", "ch",
    "square", "sq", "passage", "pass", "voie", "cours", "quai",
  ]);
  return new Set(
    normalizeAscii(value)
      .split(/\s+/)
      .filter((t) => t.length >= 2 && !stop.has(t) && !/^\d+$/.test(t)),
  );
}

function tokenOverlap(a: Set<string>, b: Set<string>): number {
  if (!a.size || !b.size) return 0;
  let common = 0;
  for (const t of a) if (b.has(t)) common += 1;
  return common / Math.max(a.size, b.size);
}

function parseGeoPoint(p: unknown): { lat: number; lon: number } | null {
  if (!p) return null;
  if (typeof p === "string") {
    const [a, b] = p.split(",").map(Number);
    return Number.isFinite(a) && Number.isFinite(b) ? { lat: a, lon: b } : null;
  }
  if (Array.isArray(p) && p.length >= 2) {
    return { lat: Number(p[0]), lon: Number(p[1]) };
  }
  return null;
}

function pickNumber(d: AdemeRecord, keys: string[]): number | null {
  for (const k of keys) {
    const v = d[k];
    if (v == null || v === "") continue;
    const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
    if (Number.isFinite(n)) return n as number;
  }
  return null;
}

export type MaisonDpe = {
  numero_dpe: string;
  classe: string;
  ges: string;
  conso: number | null;
  surface: number | null;
  date: string | null;
  annee_construction: number | null;
  energie_principale_chauffage: string | null;
  type_batiment: string | null;
  address: {
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    label: string;
  };
  lat: number | null;
  lon: number | null;
  ademe_url: string;
};

export type MaisonLookupResult = {
  banResolved: {
    label: string;
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    lat: number;
    lon: number;
    score: number;
  } | null;
  parcelle: Parcelle | null;
  totalDpeFound: number;
  matched: MaisonDpe[];
  notes: string[];
};

function toMaisonDpe(rec: AdemeRecord): MaisonDpe {
  const numero = String(rec.numero_dpe ?? "");
  return {
    numero_dpe: numero,
    classe: String(rec.etiquette_dpe ?? "NC").toUpperCase(),
    ges: String(rec.etiquette_ges ?? "NC").toUpperCase(),
    conso: pickNumber(rec, ["conso_5_usages_par_m2_ep"]),
    surface: pickNumber(rec, ["surface_habitable_logement", "surface_habitable_immeuble"]),
    date: (rec.date_etablissement_dpe as string) ?? (rec.date_derniere_modification_dpe as string) ?? null,
    annee_construction: pickNumber(rec, ["annee_construction"]),
    energie_principale_chauffage:
      (rec["type_energie_principale_chauffage"] as string) ??
      (rec["type_energie_n1"] as string) ??
      null,
    type_batiment: (rec.type_batiment as string) ?? null,
    address: {
      housenumber: (rec.numero_voie_ban as string) ?? null,
      street: (rec.nom_rue_ban as string) ?? null,
      postcode: (rec.code_postal_ban as string) ?? (rec.code_postal_brut as string) ?? null,
      city: (rec.nom_commune_ban as string) ?? (rec.nom_commune_brut as string) ?? null,
      label:
        (rec.adresse_ban as string) ??
        (rec.adresse_complete_brut as string) ??
        `${rec.nom_rue_ban ?? ""} ${rec.code_postal_ban ?? ""} ${rec.nom_commune_ban ?? ""}`.trim(),
    },
    lat: parseGeoPoint(rec._geopoint)?.lat ?? null,
    lon: parseGeoPoint(rec._geopoint)?.lon ?? null,
    ademe_url: numero
      ? `https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${numero}`
      : "",
  };
}

/**
 * Recherche d'une maison par adresse précise.
 * Pipeline :
 *  1. Géocode BAN forward de l'adresse → lat/lon canoniques + housenumber
 *  2. Cadastre IGN sur ces coords → parcelle de référence
 *  3. ADEME bbox autour des coords (50m) + filtre :
 *     - type_batiment = "maison"
 *     - même CP + commune + voie type + tokens rue + numéro (strict)
 *  4. Optionnel : vérifie que les DPE matchés sont sur la même parcelle cadastrale
 */
export async function lookupMaisonByAddress(query: string): Promise<MaisonLookupResult> {
  const notes: string[] = [];

  // 1. BAN forward
  const banResults = await geocodeAddress(query, { limit: 1 });
  const ban = banResults[0];
  if (!ban || ban.score < 0.4) {
    return {
      banResolved: null,
      parcelle: null,
      totalDpeFound: 0,
      matched: [],
      notes: ["Adresse non trouvée par la BAN. Précisez la requête."],
    };
  }
  notes.push(
    `BAN : ${ban.label} (score ${Math.round(ban.score * 100)}%, type ${
      ban.housenumber ? "housenumber" : "street"
    })`,
  );

  // 2. Cadastre
  const parcelle = await getParcelByPoint(ban.lat, ban.lon);
  if (parcelle) notes.push(`Parcelle IGN : ${parcelle.idu} (${parcelle.contenance_m2} m²)`);

  // 3. ADEME (rayons progressifs 25 → 80m)
  let raw: AdemeRecord[] = [];
  let usedR = 80;
  for (const r of [25, 40, 80]) {
    raw = await fetchAdemeDpeAround({ lat: ban.lat, lon: ban.lon, r, size: 500 });
    usedR = r;
    if (raw.length >= 5) break;
  }

  // 4. Filtre strict : type maison + adresse stricte
  const targetTokens = streetTokens(ban.street ?? ban.label);
  const targetVoieType = extractVoieType(ban.street ?? ban.label);
  const targetHouseNumber = normalizeCompact(ban.housenumber ?? "");
  const targetPostcode = String(ban.postcode ?? "").trim();
  const targetCity = normalizeAscii(ban.city ?? "");

  const matched = raw
    .filter(isMaison)
    .filter((r) => {
      if (targetPostcode) {
        const recCp = String(r.code_postal_ban ?? r.code_postal_brut ?? "").trim();
        if (recCp !== targetPostcode) return false;
      }
      if (targetCity) {
        const recCity = normalizeAscii(r.nom_commune_ban ?? r.nom_commune_brut);
        if (recCity !== targetCity) return false;
      }
      const recStreet = r.nom_rue_ban || r.adresse_ban || r.adresse_complete_brut || "";
      if (targetVoieType) {
        const recType = extractVoieType(recStreet);
        if (!recType || recType !== targetVoieType) return false;
      }
      if (targetTokens.size > 0) {
        const overlap = tokenOverlap(targetTokens, streetTokens(recStreet));
        if (overlap < 0.7) return false;
      }
      if (targetHouseNumber) {
        const recNo = normalizeCompact(r.numero_voie_ban ?? "");
        if (!recNo) return false;
        const wantN = targetHouseNumber.match(/\d+/)?.[0] ?? "";
        const recN = recNo.match(/\d+/)?.[0] ?? "";
        if (wantN !== recN) return false;
      }
      return true;
    })
    .map(toMaisonDpe);

  // 5. (Optionnel) cadastre check sur chaque DPE matché — non bloquant
  if (parcelle && matched.length > 0) {
    const verified = [];
    for (const m of matched) {
      if (m.lat != null && m.lon != null) {
        const p = await getParcelByPoint(m.lat, m.lon);
        if (p && p.idu === parcelle.idu) verified.push(m);
      } else {
        verified.push(m); // pas de coords → bénéfice du doute
      }
    }
    if (verified.length < matched.length) {
      notes.push(
        `${matched.length - verified.length} DPE écarté(s) par vérification cadastrale (parcelle différente)`,
      );
    }
    return {
      banResolved: {
        label: ban.label,
        housenumber: ban.housenumber ?? null,
        street: ban.street ?? null,
        postcode: ban.postcode ?? null,
        city: ban.city ?? null,
        lat: ban.lat,
        lon: ban.lon,
        score: ban.score,
      },
      parcelle,
      totalDpeFound: raw.length,
      matched: verified,
      notes,
    };
  }

  return {
    banResolved: {
      label: ban.label,
      housenumber: ban.housenumber ?? null,
      street: ban.street ?? null,
      postcode: ban.postcode ?? null,
      city: ban.city ?? null,
      lat: ban.lat,
      lon: ban.lon,
      score: ban.score,
    },
    parcelle,
    totalDpeFound: raw.length,
    matched,
    notes: notes.concat(
      matched.length === 0
        ? [
            `Rayon ${usedR}m exploré. Aucun DPE "maison" à cette adresse exacte. Causes possibles :`,
            "• Maison construite avant 2007 sans vente/location récente (DPE non obligatoire)",
            "• DPE expiré et non renouvelé",
            "• Adresse non normalisée sur l'ADEME",
          ]
        : [],
    ),
  };
}

/**
 * Recherche par zone (commune ou CP) — retourne toutes les maisons avec DPE
 * dans la zone, filtrable par classe DPE.
 *
 * Stratégie : on appelle l'ADEME directement avec un filtre sur le code postal
 * (data‑fair supporte `qs=code_postal_ban:"75011"`) puis on filtre côté serveur
 * sur type_batiment = "maison" et la classe DPE demandée.
 */
const ADEME_BASE = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines";

export type MaisonsZoneFilters = {
  cp?: string;
  commune?: string;
  dpeClasses?: string[];     // ["F", "G"]
  consoMin?: number;
  consoMax?: number;
  yearMin?: number;          // année construction min
  yearMax?: number;
  limit?: number;            // 1‑1000
  size?: number;             // taille de la fenêtre ADEME (max 10000)
};

export async function searchMaisonsByZone(
  f: MaisonsZoneFilters,
): Promise<{ total: number; items: MaisonDpe[] }> {
  const params: string[] = [];
  if (f.cp) params.push(`code_postal_ban:"${f.cp}"`);
  if (f.commune) params.push(`nom_commune_ban:"${f.commune}"`);
  // On limite au type maison côté ADEME (plus rapide que filtrer après)
  params.push(`type_batiment:"maison"`);

  const url = new URL(ADEME_BASE);
  url.searchParams.set("qs", params.join(" AND "));
  url.searchParams.set("size", String(Math.min(f.size ?? 1000, 10000)));

  const res = await fetch(url, {
    headers: { accept: "application/json" },
    next: { revalidate: 300 },
  });
  if (!res.ok) {
    throw new Error(`ADEME HTTP ${res.status}`);
  }
  const json = (await res.json()) as { total?: number; results?: AdemeRecord[] };

  let records = (json.results ?? []).filter(isMaison);

  // Filtres post‑fetch
  if (f.dpeClasses && f.dpeClasses.length > 0) {
    const set = new Set(f.dpeClasses.map((c) => c.toUpperCase()));
    records = records.filter((r) =>
      set.has(String(r.etiquette_dpe ?? "NC").toUpperCase()),
    );
  }
  if (f.consoMin != null) {
    records = records.filter((r) => {
      const c = pickNumber(r, ["conso_5_usages_par_m2_ep"]);
      return c != null && c >= f.consoMin!;
    });
  }
  if (f.consoMax != null) {
    records = records.filter((r) => {
      const c = pickNumber(r, ["conso_5_usages_par_m2_ep"]);
      return c != null && c <= f.consoMax!;
    });
  }
  if (f.yearMin != null) {
    records = records.filter((r) => {
      const y = pickNumber(r, ["annee_construction"]);
      return y != null && y >= f.yearMin!;
    });
  }
  if (f.yearMax != null) {
    records = records.filter((r) => {
      const y = pickNumber(r, ["annee_construction"]);
      return y != null && y <= f.yearMax!;
    });
  }

  // Dédup par numero_dpe en gardant le plus récent
  const dedup = new Map<string, AdemeRecord>();
  for (const r of records) {
    const id = String(r.numero_dpe ?? "");
    if (!id) continue;
    const date =
      Date.parse(String(r.date_derniere_modification_dpe ?? r.date_etablissement_dpe ?? "")) ||
      0;
    const prev = dedup.get(id);
    if (!prev) dedup.set(id, r);
    else {
      const pdate =
        Date.parse(String(prev.date_derniere_modification_dpe ?? prev.date_etablissement_dpe ?? "")) ||
        0;
      if (date >= pdate) dedup.set(id, r);
    }
  }

  const items = [...dedup.values()]
    .sort((a, b) => {
      // Plus énergivores d'abord (F/G prioritaires pour la prospection)
      const ca = pickNumber(a, ["conso_5_usages_par_m2_ep"]) ?? 0;
      const cb = pickNumber(b, ["conso_5_usages_par_m2_ep"]) ?? 0;
      return cb - ca;
    })
    .slice(0, Math.min(f.limit ?? 200, 1000))
    .map(toMaisonDpe);

  return { total: dedup.size, items };
}

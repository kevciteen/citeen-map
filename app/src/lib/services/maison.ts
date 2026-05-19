/**
 * Service logements individuels (maisons + appartements) — couple BAN + ADEME
 * + Cadastre IGN.
 *
 * Différence avec le service copro :
 *   - Pas de référentiel pré‑importé (search‑driven, à la demande)
 *   - 1 logement = 1 DPE (pas de moyenne pondérée à faire)
 *   - Filtre strict ADEME sur type_batiment = "maison" ou "appartement"
 *   - Cadastre pour confirmer que le DPE est bien sur la même parcelle que l'adresse
 *
 * Le même service alimente les modules /maisons et /appartements via le
 * paramètre `typeBatiment` ("maison" par défaut pour préserver la rétro-compat).
 */
import { fetchAdemeDpeAround, type AdemeRecord } from "./ademe";
import { ADEME_DPE_LINES_URL } from "./ademe-constants";
import { geocodeAddress } from "./ban";
import { getParcelByPoint, type Parcelle } from "./cadastre";

export type BatimentType = "maison" | "appartement";

const TYPE_PATTERNS: Record<BatimentType, string[]> = {
  maison: ["maison", "maison_individuelle", "maison individuelle", "MI"],
  appartement: ["appartement", "appart", "logement collectif"],
};

function normalizeType(t: unknown): string {
  return String(t ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z_ ]/g, "")
    .trim();
}

function isBatimentType(rec: AdemeRecord, type: BatimentType): boolean {
  const t = normalizeType(rec.type_batiment);
  if (!t) return false;
  return TYPE_PATTERNS[type].some((m) => t.includes(m.toLowerCase()));
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
  emission_ges: number | null;
  surface: number | null;
  date: string | null;
  date_visite: string | null;
  date_fin_validite: string | null;
  annee_construction: number | null;
  nb_niveaux: number | null;
  nb_pieces: number | null;
  hauteur_sous_plafond: number | null;
  type_batiment: string | null;
  methode_dpe: string | null;
  // Chauffage
  energie_principale_chauffage: string | null;
  energie_n2: string | null;
  energie_n3: string | null;
  installation_chauffage: string | null;
  // Équipements chauffage détaillés (matériels/générateurs)
  generateur_chauffage: string | null;       // ex. "Chaudière fioul classique avant 1970"
  generateur_chauffage_desc: string | null;  // description complète installation
  emetteur_chauffage: string | null;         // ex. "Radiateur bitube..."
  // ECS (eau chaude sanitaire)
  generateur_ecs: string | null;
  description_ecs: string | null;
  volume_ballon_ecs: number | null;
  // Climatisation
  energie_climatisation: string | null;
  surface_climatisee: number | null;
  // Ventilation
  type_ventilation: string | null;
  ventilation_recente: boolean | null;
  // Zone climatique
  zone_climatique: string | null;
  // Déperditions thermiques (W/K) — où passe la chaleur
  deperdition_murs: number | null;
  deperdition_baies: number | null;
  deperdition_plancher_bas: number | null;
  deperdition_plancher_haut: number | null;
  // Coûts annuels (€/an)
  cout_total: number | null;
  cout_chauffage: number | null;
  cout_ecs: number | null;
  cout_eclairage: number | null;
  cout_refroidissement: number | null;
  // Isolation (qualités)
  isolation_enveloppe: string | null;
  isolation_murs: string | null;
  isolation_toiture: string | null;
  isolation_plancher_bas: string | null;
  isolation_menuiseries: string | null;
  // Adresse
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

function pickString(d: AdemeRecord, keys: string[]): string | null {
  for (const k of keys) {
    const v = d[k];
    if (v != null && v !== "") return String(v);
  }
  return null;
}

function toMaisonDpe(rec: AdemeRecord): MaisonDpe {
  const numero = String(rec.numero_dpe ?? "");
  return {
    numero_dpe: numero,
    classe: String(rec.etiquette_dpe ?? "NC").toUpperCase(),
    ges: String(rec.etiquette_ges ?? "NC").toUpperCase(),
    conso: pickNumber(rec, ["conso_5_usages_par_m2_ep"]),
    emission_ges: pickNumber(rec, [
      "emission_ges_5_usages_par_m2",
      "emission_ges_5_usages_m2",
    ]),
    surface: pickNumber(rec, [
      "surface_habitable_logement",
      "surface_habitable_immeuble",
    ]),
    date: pickString(rec, [
      "date_etablissement_dpe",
      "date_derniere_modification_dpe",
    ]),
    date_visite: pickString(rec, ["date_visite_diagnostiqueur"]),
    date_fin_validite: pickString(rec, ["date_fin_validite_dpe"]),
    annee_construction: pickNumber(rec, ["annee_construction"]),
    nb_niveaux: pickNumber(rec, ["nb_niveau", "nombre_niveau_logement"]),
    nb_pieces: pickNumber(rec, ["nombre_pieces_principales"]),
    hauteur_sous_plafond: pickNumber(rec, ["hauteur_sous_plafond"]),
    type_batiment: pickString(rec, ["type_batiment"]),
    methode_dpe: pickString(rec, ["nom_methode_dpe"]),
    // Chauffage
    energie_principale_chauffage: pickString(rec, [
      "type_energie_principale_chauffage",
      "type_energie_n1",
    ]),
    energie_n2: pickString(rec, ["type_energie_n2"]),
    energie_n3: pickString(rec, ["type_energie_n3"]),
    installation_chauffage: pickString(rec, [
      "type_installation_chauffage",
      "type_installation_chauffage_n1",
    ]),
    // Équipements détaillés
    generateur_chauffage: pickString(rec, [
      "type_generateur_chauffage_principal",
      "type_generateur_n1_installation_n1",
    ]),
    generateur_chauffage_desc: pickString(rec, [
      "description_installation_chauffage_n1",
      "description_generateur_chauffage_n1_installation_n1",
    ]),
    emetteur_chauffage: pickString(rec, [
      "type_emetteur_installation_chauffage_n1",
      "type_emetteur_chauffage",
    ]),
    // ECS
    generateur_ecs: pickString(rec, [
      "type_generateur_n1_ecs_n1",
      "type_generateur_chauffage_principal_ecs",
    ]),
    description_ecs: pickString(rec, ["description_installation_ecs_n1"]),
    volume_ballon_ecs: pickNumber(rec, ["volume_stockage_generateur_n1_ecs_n1"]),
    // Climatisation
    energie_climatisation: pickString(rec, ["type_energie_climatisation"]),
    surface_climatisee: pickNumber(rec, ["surface_climatisee"]),
    // Ventilation
    type_ventilation: pickString(rec, ["type_ventilation"]),
    ventilation_recente:
      pickNumber(rec, ["ventilation_posterieure_2012"]) === 1 ? true : null,
    // Zone climatique
    zone_climatique: pickString(rec, ["zone_climatique"]),
    // Déperditions thermiques
    deperdition_murs: pickNumber(rec, ["deperditions_murs"]),
    deperdition_baies: pickNumber(rec, ["deperditions_baies_vitrees"]),
    deperdition_plancher_bas: pickNumber(rec, ["deperditions_planchers_bas"]),
    deperdition_plancher_haut: pickNumber(rec, ["deperditions_planchers_hauts"]),
    // Coûts annuels
    cout_total: pickNumber(rec, ["cout_total_5_usages"]),
    cout_chauffage: pickNumber(rec, ["cout_chauffage"]),
    cout_ecs: pickNumber(rec, ["cout_ecs", "cout_eau_chaude_sanitaire"]),
    cout_eclairage: pickNumber(rec, ["cout_eclairage"]),
    cout_refroidissement: pickNumber(rec, ["cout_refroidissement"]),
    // Isolation
    isolation_enveloppe: pickString(rec, ["qualite_isolation_enveloppe"]),
    isolation_murs: pickString(rec, ["qualite_isolation_murs"]),
    isolation_toiture: pickString(rec, [
      "qualite_isolation_plancher_haut_toit",
      "qualite_isolation_plancher_haut",
    ]),
    isolation_plancher_bas: pickString(rec, ["qualite_isolation_plancher_bas"]),
    isolation_menuiseries: pickString(rec, ["qualite_isolation_menuiseries"]),
    address: {
      housenumber: pickString(rec, ["numero_voie_ban"]),
      street: pickString(rec, ["nom_rue_ban"]),
      postcode: pickString(rec, ["code_postal_ban", "code_postal_brut"]),
      city: pickString(rec, ["nom_commune_ban", "nom_commune_brut"]),
      label:
        pickString(rec, ["adresse_ban", "adresse_complete_brut"]) ??
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
export async function lookupMaisonByAddress(
  query: string,
  opts: { typeBatiment?: BatimentType } = {},
): Promise<MaisonLookupResult> {
  const typeBatiment: BatimentType = opts.typeBatiment ?? "maison";
  const typeLabel = typeBatiment === "maison" ? "maison" : "appartement";
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

  // 4. Filtre strict : type {maison|appartement} + adresse stricte
  const targetTokens = streetTokens(ban.street ?? ban.label);
  const targetVoieType = extractVoieType(ban.street ?? ban.label);
  const targetHouseNumber = normalizeCompact(ban.housenumber ?? "");
  const targetPostcode = String(ban.postcode ?? "").trim();
  const targetCity = normalizeAscii(ban.city ?? "");

  const matched = raw
    .filter((r) => isBatimentType(r, typeBatiment))
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
            `Rayon ${usedR}m exploré. Aucun DPE "${typeLabel}" à cette adresse exacte. Causes possibles :`,
            `• ${typeLabel === "maison" ? "Maison" : "Appartement"} construit avant 2007 sans vente/location récente (DPE non obligatoire)`,
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
const ADEME_BASE = ADEME_DPE_LINES_URL;

export type MaisonsZoneFilters = {
  cp?: string;
  commune?: string;
  typeBatiment?: BatimentType; // "maison" (défaut) | "appartement"
  dpeClasses?: string[];     // ["F", "G"]
  gesClasses?: string[];     // ["F", "G"] sur l'étiquette GES
  consoMin?: number;
  consoMax?: number;
  surfaceMin?: number;
  surfaceMax?: number;
  yearMin?: number;          // année construction min
  yearMax?: number;
  energie?: string[];        // ["fioul", "gaz", ...] match substring sur energie_n1
  isolationMursMauvaise?: boolean; // filtre passoires murs
  dpeAncienAnnees?: number;  // DPE de plus de N ans (utile pour relances)
  limit?: number;            // 1‑1000
  size?: number;             // taille de la fenêtre ADEME (max 10000)
};

async function resolveCommuneInsee(commune: string): Promise<{
  inseeCode: string | null;
  canonicalName: string | null;
}> {
  try {
    const url = new URL("https://api-adresse.data.gouv.fr/search/");
    url.searchParams.set("q", commune);
    url.searchParams.set("type", "municipality");
    url.searchParams.set("limit", "1");
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 86400 },
    });
    if (!res.ok) return { inseeCode: null, canonicalName: null };
    const json = (await res.json()) as {
      features?: Array<{ properties?: { citycode?: string; city?: string; label?: string } }>;
    };
    const f = json.features?.[0]?.properties;
    return {
      inseeCode: f?.citycode ?? null,
      canonicalName: f?.city ?? f?.label ?? null,
    };
  } catch {
    return { inseeCode: null, canonicalName: null };
  }
}

export async function searchMaisonsByZone(
  f: MaisonsZoneFilters,
): Promise<{ total: number; items: MaisonDpe[] }> {
  const typeBatiment: BatimentType = f.typeBatiment ?? "maison";
  const params: string[] = [];
  if (f.cp) params.push(`code_postal_ban:"${f.cp}"`);
  if (f.commune) {
    // ADEME exige une correspondance exacte (Elasticsearch case-sensitive).
    // On résout d'abord la commune via BAN pour récupérer le code INSEE,
    // qui est plus fiable que le nom (gère casse, accents, tirets).
    const resolved = await resolveCommuneInsee(f.commune);
    if (resolved.inseeCode) {
      params.push(`code_insee_ban:"${resolved.inseeCode}"`);
    } else if (resolved.canonicalName) {
      params.push(`nom_commune_ban:"${resolved.canonicalName}"`);
    } else {
      params.push(`nom_commune_ban:"${f.commune}"`);
    }
  }
  // On limite au type cible côté ADEME (plus rapide que filtrer après)
  params.push(`type_batiment:"${typeBatiment}"`);

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

  let records = (json.results ?? []).filter((r) => isBatimentType(r, typeBatiment));

  // Filtres post‑fetch
  if (f.dpeClasses && f.dpeClasses.length > 0) {
    const set = new Set(f.dpeClasses.map((c) => c.toUpperCase()));
    records = records.filter((r) =>
      set.has(String(r.etiquette_dpe ?? "NC").toUpperCase()),
    );
  }
  if (f.gesClasses && f.gesClasses.length > 0) {
    const set = new Set(f.gesClasses.map((c) => c.toUpperCase()));
    records = records.filter((r) =>
      set.has(String(r.etiquette_ges ?? "NC").toUpperCase()),
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
  if (f.surfaceMin != null) {
    records = records.filter((r) => {
      const s = pickNumber(r, ["surface_habitable_logement"]);
      return s != null && s >= f.surfaceMin!;
    });
  }
  if (f.surfaceMax != null) {
    records = records.filter((r) => {
      const s = pickNumber(r, ["surface_habitable_logement"]);
      return s != null && s <= f.surfaceMax!;
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
  if (f.energie && f.energie.length > 0) {
    const wanted = f.energie.map((e) => normalizeAscii(e));
    records = records.filter((r) => {
      const energie = normalizeAscii(
        (r["type_energie_principale_chauffage"] as string) ??
          (r["type_energie_n1"] as string) ??
          "",
      );
      return wanted.some((w) => energie.includes(w));
    });
  }
  if (f.isolationMursMauvaise) {
    records = records.filter((r) => {
      const q = String(r["qualite_isolation_murs"] ?? "").toLowerCase();
      return q.includes("insuffisante") || q.includes("tres mauvaise") || q.includes("mauvaise") || q.includes("moyenne");
    });
  }
  if (f.dpeAncienAnnees && f.dpeAncienAnnees > 0) {
    const cutoff = Date.now() - f.dpeAncienAnnees * 365 * 24 * 3600 * 1000;
    records = records.filter((r) => {
      const t = Date.parse(String(r.date_etablissement_dpe ?? ""));
      return Number.isFinite(t) && t < cutoff;
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

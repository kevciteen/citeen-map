import { fetchAdemeDpeAround, type AdemeRecord } from "./ademe";
import { geocodeAddress } from "./ban";
import { getParcelByPoint, resolveCoproParcel, type Parcelle } from "./cadastre";

export const DPE_CLASS_COLOR: Record<string, string> = {
  A: "#1f9d55",
  B: "#7cb342",
  C: "#cddc39",
  D: "#ffeb3b",
  E: "#ffb300",
  F: "#fb8c00",
  G: "#e53935",
  NC: "#94a3b8",
  NA: "#94a3b8",
};

export function dpeColor(classe?: string | null): string {
  const c = String(classe ?? "NC").toUpperCase();
  return DPE_CLASS_COLOR[c] ?? DPE_CLASS_COLOR.NC;
}

/* ============================================================
   Address normalization & scoring (porté de backend/services/dpeService.js)
   ============================================================ */

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

function extractHouseNumber(value: unknown): string | null {
  const match = normalizeAscii(value).match(/\b(\d{1,4})(?:\s*(bis|ter|quater))?\b/);
  if (!match) return null;
  return `${match[1]}${match[2] || ""}`;
}

/* Types de voie — TRAITÉS SÉPARÉMENT (pas en stopword) pour différencier
   "rue Lénine" ≠ "Avenue Lénine" ≠ "Boulevard Lénine". */
const VOIE_TYPE_MAP: Record<string, string> = {
  r: "rue",
  rue: "rue",
  av: "avenue",
  ave: "avenue",
  avenue: "avenue",
  bd: "boulevard",
  blvd: "boulevard",
  boulevard: "boulevard",
  pl: "place",
  place: "place",
  rte: "route",
  route: "route",
  qu: "quai",
  quai: "quai",
  cr: "cours",
  cours: "cours",
  all: "allee",
  allee: "allee",
  imp: "impasse",
  impasse: "impasse",
  ch: "chemin",
  chemin: "chemin",
  sq: "square",
  square: "square",
  pass: "passage",
  passage: "passage",
  villa: "villa",
  pont: "pont",
  voie: "voie",
  esp: "esplanade",
  esplanade: "esplanade",
  prom: "promenade",
  promenade: "promenade",
  rd: "rond-point",
  rondpoint: "rond-point",
};

const STOP_WORDS = new Set([
  // Mots descriptifs (PAS les types de voie)
  "residence",
  "res",
  "batiment",
  "bat",
  // Articles et prépositions français
  "de",
  "du",
  "des",
  "d",
  "la",
  "le",
  "les",
  "l",
  "au",
  "aux",
  "et",
  "en",
  "sur",
  "sous",
]);

function extractVoieType(s: string): string | null {
  for (const t of normalizeAscii(s).split(/\s+/)) {
    const mapped = VOIE_TYPE_MAP[t];
    if (mapped) return mapped;
  }
  return null;
}

function streetTokenSet(value: unknown): Set<string> {
  return new Set(
    normalizeAscii(value)
      .split(/\s+/)
      .filter(Boolean)
      .filter(
        (t) =>
          !STOP_WORDS.has(t) &&
          !VOIE_TYPE_MAP[t] &&
          !/^\d+$/.test(t),
      ),
  );
}

function tokenOverlap(left: Set<string>, right: Set<string>): number {
  if (left.size === 0 || right.size === 0) return 0;
  let common = 0;
  for (const t of left) if (right.has(t)) common += 1;
  return common / Math.max(left.size, right.size);
}

function parseGeoPoint(
  point: unknown,
): { lat: number; lon: number } | null {
  if (!point) return null;
  if (Array.isArray(point) && point.length >= 2) {
    const lat = Number((point as unknown[])[0]);
    const lon = Number((point as unknown[])[1]);
    return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
  }
  if (typeof point === "string") {
    const [a, b] = point.split(",").map(Number);
    return Number.isFinite(a) && Number.isFinite(b) ? { lat: a, lon: b } : null;
  }
  if (typeof point === "object") {
    const p = point as Record<string, unknown>;
    const lat = Number(p.lat ?? p.latitude);
    const lon = Number(p.lon ?? p.lng ?? p.longitude);
    return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
  }
  return null;
}

function haversineM(
  a: { lat: number; lon: number },
  b: { lat: number; lon: number },
): number {
  const R = 6371000;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const la1 = (a.lat * Math.PI) / 180;
  const la2 = (b.lat * Math.PI) / 180;
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(la1) * Math.cos(la2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

export type AddressContext = {
  label: string;
  address: string;
  codePostal: string;
  commune: string;
  numeroImmatriculation?: string;
  houseNumber: string | null;
  voieType: string | null;
  streetTokens: Set<string>;
  lat: number | null;
  lon: number | null;
};

function stripCpAndCommune(address: string, cp: string, commune: string): string {
  let s = ` ${address} `;
  if (cp) {
    s = s.replace(new RegExp(`\\b${cp.replace(/\D/g, "")}\\b`, "g"), " ");
  }
  if (commune) {
    // Replace commune name (case-insensitive, accents-insensitive)
    const normCommune = normalizeAscii(commune);
    if (normCommune) {
      // Build a regex on the original casing — accept various accent variants
      const escaped = normCommune.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      // We work on the normalized version of the address for matching
      const normAddress = normalizeAscii(s);
      const re = new RegExp(`\\b${escaped}\\b`, "gi");
      const cleanedNorm = normAddress.replace(re, " ");
      return cleanedNorm.replace(/\s+/g, " ").trim();
    }
  }
  return normalizeAscii(s);
}

export function buildAddressContext(opts: {
  address?: string | null;
  codePostal?: string | null;
  commune?: string | null;
  numeroImmatriculation?: string | null;
  lat?: number | null;
  lon?: number | null;
}): AddressContext {
  const address = String(opts.address ?? "").trim();
  const codePostal = String(opts.codePostal ?? "").trim();
  const commune = String(opts.commune ?? "").trim();
  const label = [address, codePostal, commune].filter(Boolean).join(" ");
  // Extrait la portion "numéro + rue" en retirant CP et commune du texte
  const streetSource = stripCpAndCommune(address || label, codePostal, commune);
  return {
    label,
    address,
    codePostal,
    commune,
    numeroImmatriculation: opts.numeroImmatriculation
      ? String(opts.numeroImmatriculation).trim()
      : undefined,
    houseNumber: extractHouseNumber(streetSource) || extractHouseNumber(address),
    voieType: extractVoieType(streetSource) || extractVoieType(address),
    streetTokens: streetTokenSet(streetSource),
    lat: Number.isFinite(opts.lat) ? (opts.lat as number) : null,
    lon: Number.isFinite(opts.lon) ? (opts.lon as number) : null,
  };
}

/* Exigences DURES : (CP + commune + rue + numéro) doivent matcher.
   Aucune tolérance "rayon seul" : si l'adresse ne correspond pas, on rejette.
   Le numéro d'immatriculation, s'il match exactement, court-circuite tout. */
function passesStrictAddress(
  rec: AdemeRecord,
  ctx: AddressContext,
): { ok: boolean; reason?: string } {
  const recImmat = normalizeCompact(rec.numero_immatriculation_copropriete ?? "");
  if (
    ctx.numeroImmatriculation &&
    recImmat &&
    recImmat === normalizeCompact(ctx.numeroImmatriculation)
  ) {
    // Match par numéro d'immatriculation copro : signal le plus fort
    return { ok: true };
  }

  // 1) Code postal — obligatoire
  if (ctx.codePostal) {
    const recCp = String(rec.code_postal_ban ?? rec.code_postal_brut ?? "").trim();
    if (!recCp) return { ok: false, reason: "no_cp_in_record" };
    if (recCp !== ctx.codePostal) return { ok: false, reason: "cp_mismatch" };
  }

  // 2) Commune — obligatoire
  if (ctx.commune) {
    const recCity = normalizeAscii(rec.nom_commune_ban ?? rec.nom_commune_brut);
    if (!recCity) return { ok: false, reason: "no_commune_in_record" };
    if (recCity !== normalizeAscii(ctx.commune))
      return { ok: false, reason: "commune_mismatch" };
  }

  const recStreet =
    rec.nom_rue_ban || rec.adresse_ban || rec.adresse_complete_brut || "";

  // 3a) Type de voie — obligatoire (rue ≠ avenue ≠ boulevard)
  if (ctx.voieType) {
    const recVoieType = extractVoieType(recStreet);
    if (!recVoieType) return { ok: false, reason: "no_voie_type_in_record" };
    if (recVoieType !== ctx.voieType)
      return { ok: false, reason: "voie_type_mismatch" };
  }

  // 3b) Nom de rue (token overlap) — obligatoire si on connaît la rue
  if (ctx.streetTokens.size > 0) {
    const recTokens = streetTokenSet(recStreet);
    if (recTokens.size === 0) return { ok: false, reason: "no_street_in_record" };
    const overlap = tokenOverlap(ctx.streetTokens, recTokens);
    if (overlap < 0.7) return { ok: false, reason: "street_mismatch" };
  }

  // 4) Numéro de rue — obligatoire si on connaît un numéro pour la copro
  if (ctx.houseNumber) {
    const recNo = normalizeCompact(rec.numero_voie_ban ?? "");
    if (!recNo) return { ok: false, reason: "no_number_in_record" };
    const want = normalizeCompact(ctx.houseNumber);
    const numericPart = (s: string) => (s.match(/\d+/)?.[0] ?? "");
    const wantN = numericPart(want);
    const recN = numericPart(recNo);
    // Le numéro numérique doit être identique. Suffixes (bis/ter) peuvent différer
    // mais on n'accepte pas un numéro voisin (4 != 5).
    if (wantN !== recN) return { ok: false, reason: "number_mismatch" };
  }

  return { ok: true };
}

/* Score utilisé uniquement pour ordonner les records qui passent déjà
   le filtre strict. */
function rankScore(
  rec: AdemeRecord,
  ctx: AddressContext,
  queryPoint: { lat: number; lon: number } | null,
): number {
  let score = 0;
  const recImmat = normalizeCompact(rec.numero_immatriculation_copropriete ?? "");
  if (
    ctx.numeroImmatriculation &&
    recImmat &&
    recImmat === normalizeCompact(ctx.numeroImmatriculation)
  ) {
    score += 100;
  }
  // bonus distance (départage les records très proches)
  const candidate = parseGeoPoint(rec._geopoint);
  if (queryPoint && candidate) {
    const d = haversineM(queryPoint, candidate);
    if (d <= 8) score += 8;
    else if (d <= 15) score += 6;
    else if (d <= 25) score += 4;
    else if (d <= 50) score += 2;
  }
  // bonus date (plus récent = mieux)
  const t =
    parseDate(rec.date_derniere_modification_dpe) ||
    parseDate(rec.date_etablissement_dpe);
  if (t) score += t / 1e13;
  return score;
}

function filterStrict(
  list: AdemeRecord[],
  ctx: AddressContext,
  queryPoint: { lat: number; lon: number } | null,
): {
  filtered: AdemeRecord[];
  mode: "address" | "no_match";
  reasonsCounts: Record<string, number>;
} {
  const reasonsCounts: Record<string, number> = {};
  const passing: AdemeRecord[] = [];
  for (const r of list) {
    const v = passesStrictAddress(r, ctx);
    if (v.ok) passing.push(r);
    else if (v.reason) {
      reasonsCounts[v.reason] = (reasonsCounts[v.reason] ?? 0) + 1;
    }
  }
  passing.sort((a, b) => rankScore(b, ctx, queryPoint) - rankScore(a, ctx, queryPoint));
  return {
    filtered: passing,
    mode: passing.length > 0 ? "address" : "no_match",
    reasonsCounts,
  };
}

/* ============================================================
   Classification, statut, confiance
   ============================================================ */

function parseDate(d: unknown): number {
  if (!d) return 0;
  const t = Date.parse(String(d));
  return Number.isFinite(t) ? t : 0;
}

function pickNumber(d: AdemeRecord, keys: string[]): number | null {
  for (const k of keys) {
    const v = d[k];
    if (v === null || v === undefined || v === "") continue;
    const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
    if (Number.isFinite(n)) return n as number;
  }
  return null;
}

function consoToClasse(conso: number | null): string {
  if (!Number.isFinite(conso) || conso == null) return "NC";
  if (conso < 70) return "A";
  if (conso < 110) return "B";
  if (conso < 180) return "C";
  if (conso < 250) return "D";
  if (conso < 330) return "E";
  if (conso < 420) return "F";
  return "G";
}

function isCollectifRecord(d: AdemeRecord): boolean {
  const t = String(d?.type_dpe || d?.type_dpe_batiment || "").toUpperCase();
  return Boolean(
    d?.numero_dpe_immeuble ||
      d?.numero_dpe_immeuble_associe ||
      t.includes("IMMEUBLE") ||
      t.includes("COLLECTIF"),
  );
}

export type DpeIndividuel = {
  numero_dpe: string | null;
  date: string | null;
  classe: string;
  ges: string;
  conso: number | null;
  surface: number | null;
  adresse: string | null;
  numero_voie: string | null;
  rue: string | null;
  code_postal: string | null;
  commune: string | null;
  type_dpe: string | null;
  annee_construction: number | null;
  type_batiment: string | null;
  isCollectif: boolean;
};

export type MatchMode = "address" | "no_match";

/* Niveau de qualité du matching (pour décision de prospection) :
   - verified   : DPE collectif réel OU plusieurs DPE individuels sur la MÊME parcelle cadastrale → fiable
   - approximate: 1 DPE individuel matché OU adresse BAN à >15m mais même parcelle → moyennement fiable
   - uncertain  : DPE BAN sur parcelle différente / parcelles non identifiées → ne pas utiliser pour prospection
   - no_data    : aucune donnée DPE attribuable */
export type QualityLevel = "verified" | "approximate" | "uncertain" | "no_data";

export type DpeEstimation = {
  rayonM: number;
  totalDpeFound: number;
  totalDpeMatched: number;
  matchMode: MatchMode;
  matchReasons?: Record<string, number>;
  quality: {
    level: QualityLevel;
    label: string;
    reason: string;
  };
  ctxResolved: {
    address: string;
    codePostal: string;
    commune: string;
    houseNumber: string | null;
    voieType: string | null;
    streetTokens: string[];
  };
  banResolved?: {
    label: string;
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    score: number;
    corrected: boolean;
  } | null;
  parcelle?: {
    idu: string;
    section: string;
    numero: string;
    surfaceM2: number;
    centroidLat: number;
    centroidLon: number;
    method: "ref" | "point";
  } | null;
  collectifReel: {
    classe: string;
    conso: number | null;
    ges: string;
    date: string | null;
    numero: string | null;
  } | null;
  immeubleSimule: {
    classe: string;
    consoMoyenne: number;
    nbDpeUtilises: number;
    methode: string;
  } | null;
  immeubleFinal: {
    statut: "reel" | "simule" | "aucun";
    classe: string;
    classeColor: string;
    conso: number | null;
    confiance: { score: number; label: string };
  };
  matchedRecords?: DpeIndividuel[];
};

function toIndividuel(d: AdemeRecord): DpeIndividuel {
  const conso = pickNumber(d, ["conso_5_usages_par_m2_ep"]);
  const surface = pickNumber(d, [
    "surface_habitable_logement",
    "surface_habitable_immeuble",
  ]);
  const annee = pickNumber(d, ["annee_construction"]);
  return {
    numero_dpe:
      (d.numero_dpe as string) ||
      (d.numero_dpe_immeuble as string) ||
      null,
    date:
      (d.date_etablissement_dpe as string) ||
      (d.date_derniere_modification_dpe as string) ||
      null,
    classe: String(d.etiquette_dpe || "NC").toUpperCase(),
    ges: String(d.etiquette_ges || "NC").toUpperCase(),
    conso,
    surface,
    adresse:
      (d.adresse_ban as string) ||
      (d.adresse_complete_brut as string) ||
      null,
    numero_voie: (d.numero_voie_ban as string) || null,
    rue: (d.nom_rue_ban as string) || null,
    code_postal:
      (d.code_postal_ban as string) ||
      (d.code_postal_brut as string) ||
      null,
    commune:
      (d.nom_commune_ban as string) ||
      (d.nom_commune_brut as string) ||
      null,
    type_dpe:
      (d.type_dpe as string) ||
      (d.type_dpe_batiment as string) ||
      null,
    annee_construction: annee,
    type_batiment: (d.type_batiment as string) || null,
    isCollectif: isCollectifRecord(d),
  };
}

function computeConfidence(opts: {
  hasCollectif: boolean;
  indivCountUsed: number;
  maxIndiv: number;
  matchMode: MatchMode;
}): { score: number; label: string } {
  const { hasCollectif, indivCountUsed, maxIndiv, matchMode } = opts;
  if (matchMode === "no_match")
    return {
      score: 0,
      label: "Aucun DPE strictement attribué à cette adresse",
    };
  if (hasCollectif) return { score: 95, label: "Élevée (DPE collectif réel)" };
  if (!indivCountUsed)
    return { score: 5, label: "Très faible (aucun DPE exploitable)" };
  const ratio = Math.min(indivCountUsed / maxIndiv, 1);
  const score = Math.round(30 + ratio * 60);
  const lotsTxt = `${indivCountUsed} DPE individuel${indivCountUsed > 1 ? "s" : ""} matché${indivCountUsed > 1 ? "s" : ""}`;
  const label =
    score >= 75
      ? `Bonne (immeuble identifié, ${lotsTxt})`
      : score >= 55
        ? `Moyenne (immeuble identifié, ${lotsTxt})`
        : `Faible (${lotsTxt} seulement)`;
  return { score, label };
}

function computeReelEtSimule(
  list: AdemeRecord[],
  n: number,
  matchMode: MatchMode,
): Omit<
  DpeEstimation,
  | "rayonM"
  | "totalDpeFound"
  | "totalDpeMatched"
  | "matchMode"
  | "matchReasons"
  | "ctxResolved"
  | "matchedRecords"
> {
  const items = list.map((d) => ({
    raw: d,
    isCollectif: isCollectifRecord(d),
    date:
      parseDate(d?.date_derniere_modification_dpe) ||
      parseDate(d?.date_etablissement_dpe),
    conso: pickNumber(d, ["conso_5_usages_par_m2_ep"]),
    classe: (String(d?.etiquette_dpe || "").toUpperCase() || "NC") as string,
    ges: (String(d?.etiquette_ges || "").toUpperCase() || "NC") as string,
  }));

  const collectifs = items.filter((x) => x.isCollectif);
  const indivs = items.filter((x) => !x.isCollectif);

  const collectifRecent = collectifs
    .filter((x) => x.date)
    .slice()
    .sort((a, b) => b.date - a.date)[0];

  const collectifReel = collectifRecent
    ? {
        classe: collectifRecent.classe,
        conso: collectifRecent.conso,
        ges: collectifRecent.ges,
        date: collectifRecent.raw?.date_etablissement_dpe ?? null,
        numero:
          collectifRecent.raw?.numero_dpe ||
          collectifRecent.raw?.numero_dpe_immeuble ||
          null,
      }
    : null;

  const indivsTries = indivs
    .filter((x): x is typeof x & { conso: number } => Number.isFinite(x.conso))
    .sort((a, b) => b.date - a.date)
    .slice(0, n);

  const simConso =
    indivsTries.length > 0
      ? indivsTries.reduce((s, x) => s + x.conso, 0) / indivsTries.length
      : null;

  const immeubleSimule =
    simConso !== null
      ? {
          classe: consoToClasse(simConso),
          consoMoyenne: Math.round(simConso),
          nbDpeUtilises: indivsTries.length,
          methode: `moyenne_${indivsTries.length}_individuels_recents`,
        }
      : null;

  const confiance = computeConfidence({
    hasCollectif: !!collectifReel,
    indivCountUsed: indivsTries.length,
    maxIndiv: n,
    matchMode,
  });

  const immeubleFinal = collectifReel
    ? {
        statut: "reel" as const,
        classe: collectifReel.classe,
        classeColor: dpeColor(collectifReel.classe),
        conso: collectifReel.conso,
        confiance,
      }
    : immeubleSimule
      ? {
          statut: "simule" as const,
          classe: immeubleSimule.classe,
          classeColor: dpeColor(immeubleSimule.classe),
          conso: immeubleSimule.consoMoyenne,
          confiance,
        }
      : {
          statut: "aucun" as const,
          classe: "NC",
          classeColor: dpeColor("NC"),
          conso: null,
          confiance,
        };

  return { collectifReel, immeubleSimule, immeubleFinal };
}

/* ============================================================
   Résolution BAN — corrige les erreurs du registre des copros
   (ex. "rue Lénine" alors qu'il s'agit en réalité d'"Avenue Lénine").
   ============================================================ */

type BanReverseHit = {
  label: string;
  housenumber: string | null;
  street: string | null;
  postcode: string | null;
  city: string | null;
  score: number;
  distance: number | null;
};

async function reverseGeocodeBan(
  lat: number,
  lon: number,
): Promise<BanReverseHit | null> {
  try {
    const url = `https://api-adresse.data.gouv.fr/reverse/?lat=${lat}&lon=${lon}&limit=1`;
    const res = await fetch(url, {
      headers: { accept: "application/json" },
      next: { revalidate: 3600 },
    });
    if (!res.ok) return null;
    const j = (await res.json()) as {
      features?: Array<{
        properties?: Record<string, unknown>;
      }>;
    };
    const f = j.features?.[0];
    if (!f?.properties) return null;
    const p = f.properties as {
      label?: string;
      housenumber?: string;
      street?: string;
      postcode?: string;
      city?: string;
      score?: number;
      distance?: number;
    };
    if ((p.score ?? 0) < 0.5) return null;
    return {
      label: p.label ?? "",
      housenumber: p.housenumber ?? null,
      street: p.street ?? null,
      postcode: p.postcode ?? null,
      city: p.city ?? null,
      score: Number(p.score ?? 0),
      distance: p.distance ?? null,
    };
  } catch {
    return null;
  }
}

async function refineContextViaBan(
  ctx: AddressContext,
): Promise<{
  ctx: AddressContext;
  banResolved: NonNullable<DpeEstimation["banResolved"]> | null;
}> {
  // 1. Reverse geocode the stored lat/lon — gives the canonical address at the
  //    building's actual coordinates. Most precise when available.
  let reverse: BanReverseHit | null = null;
  if (Number.isFinite(ctx.lat) && Number.isFinite(ctx.lon)) {
    reverse = await reverseGeocodeBan(ctx.lat as number, ctx.lon as number);
  }

  // 2. Forward geocode the text address (registre value). Useful when reverse
  //    fails or returns an unreliable result.
  let forward: typeof reverse | null = null;
  if (ctx.address || ctx.commune) {
    try {
      const query = [ctx.address, ctx.codePostal, ctx.commune]
        .filter(Boolean)
        .join(" ");
      const results = await geocodeAddress(query, { limit: 1 });
      const r = results[0];
      if (r && r.score >= 0.5) {
        forward = {
          label: r.label,
          housenumber: r.housenumber ?? null,
          street: r.street ?? null,
          postcode: r.postcode ?? null,
          city: r.city ?? null,
          score: r.score,
          distance: null,
        };
      }
    } catch {
      /* swallow */
    }
  }

  // Choice: prefer REVERSE when it is high-confidence and physically close
  //         to the copro coords (< 60 m). Otherwise, fall back to FORWARD.
  const picked =
    reverse &&
    reverse.score >= 0.8 &&
    (reverse.distance ?? 999) < 60 &&
    reverse.street
      ? { hit: reverse, source: "reverse" as const }
      : forward
        ? { hit: forward, source: "forward" as const }
        : null;

  if (!picked) return { ctx, banResolved: null };

  const banStreet = picked.hit.street ?? "";
  const banVoieType = banStreet ? extractVoieType(banStreet) : null;
  const banTokens = banStreet ? streetTokenSet(banStreet) : new Set<string>();

  const corrected =
    (banVoieType !== null &&
      ctx.voieType !== null &&
      banVoieType !== ctx.voieType) ||
    (picked.hit.housenumber != null && ctx.houseNumber == null) ||
    (picked.hit.postcode != null && !ctx.codePostal) ||
    (picked.hit.city != null &&
      ctx.commune &&
      normalizeAscii(picked.hit.city) !== normalizeAscii(ctx.commune)) ||
    (banStreet &&
      ctx.streetTokens.size > 0 &&
      tokenOverlap(ctx.streetTokens, banTokens) < 0.5);

  const refined: AddressContext = {
    ...ctx,
    voieType: banVoieType ?? ctx.voieType,
    streetTokens: banTokens.size > 0 ? banTokens : ctx.streetTokens,
    houseNumber: picked.hit.housenumber ?? ctx.houseNumber ?? null,
    codePostal: ctx.codePostal || picked.hit.postcode || "",
    commune: picked.hit.city || ctx.commune,
  };

  return {
    ctx: refined,
    banResolved: {
      label: picked.hit.label,
      housenumber: picked.hit.housenumber,
      street: picked.hit.street,
      postcode: picked.hit.postcode,
      city: picked.hit.city,
      score: picked.hit.score,
      corrected: !!corrected,
    },
  };
}

/* ============================================================
   Estimation principale (avec calibrage adresse)
   ============================================================ */

export async function estimateDpeForCopro(opts: {
  lat: number;
  lon: number;
  address?: string | null;
  codePostal?: string | null;
  commune?: string | null;
  numeroImmatriculation?: string | null;
  codeInseeCommune?: string | null;
  section?: string | null;
  numeroParcelle?: string | null;
  minResults?: number;
  n?: number;
  includeMatched?: boolean;
}): Promise<DpeEstimation> {
  const { lat, lon, minResults = 3, n = 5, includeMatched = false } = opts;
  const rawCtx = buildAddressContext({
    address: opts.address ?? null,
    codePostal: opts.codePostal ?? null,
    commune: opts.commune ?? null,
    numeroImmatriculation: opts.numeroImmatriculation ?? null,
    lat,
    lon,
  });
  // 1. Cadastre : parcelle "vérité juridique" de la copro
  const { parcelle: coproParcel, method: parcelMethod } = await resolveCoproParcel({
    lat,
    lon,
    codeInseeCommune: opts.codeInseeCommune ?? null,
    section: opts.section ?? null,
    numeroParcelle: opts.numeroParcelle ?? null,
  });

  // 2. Refine via BAN — corrects voie type and adds missing fields (CP, house number)
  const { ctx, banResolved } = await refineContextViaBan(rawCtx);

  /* Stratégie : on lance des appels ADEME avec des rayons croissants
     uniquement pour ÉLARGIR LA SOURCE de candidats. Le filtre d'adresse
     reste STRICT — pas de fallback radius. */
  const radii = [50, 100, 200, 400];
  const queryPoint = { lat, lon };

  let bestList: AdemeRecord[] = [];
  let usedR = radii[radii.length - 1];
  let lastRawTotal = 0;
  let matchMode: MatchMode = "no_match";
  let matchReasons: Record<string, number> = {};

  for (const r of radii) {
    const raw = await fetchAdemeDpeAround({ lat, lon, r, size: 2000 });
    lastRawTotal = raw.length;
    const { filtered, mode, reasonsCounts } = filterStrict(raw, ctx, queryPoint);
    matchMode = mode;
    matchReasons = reasonsCounts;
    usedR = r;

    if (filtered.length > bestList.length) bestList = filtered;

    // On s'arrête si on a déjà au moins une match collectif ou minResults individuels
    const hasCollectif = bestList.some((d) => {
      const t = String(d.type_dpe || d.type_dpe_batiment || "").toUpperCase();
      return Boolean(
        d.numero_dpe_immeuble ||
          d.numero_dpe_immeuble_associe ||
          t.includes("IMMEUBLE") ||
          t.includes("COLLECTIF"),
      );
    });
    if (hasCollectif || bestList.length >= minResults) break;
  }

  /* 3. Vérification cadastrale : pour chaque DPE matché par adresse, on vérifie
     que ses coordonnées tombent dans la MÊME parcelle que la copro. Sinon on
     l'écarte (cas typique : adresse partagée entre 2 bâtiments d'une grande parcelle
     OU bâtiments adjacents de parcelles différentes).
     On ne fait cette vérif que si on a la parcelle de la copro et que des records ont matché. */
  let cadastreVerified = false;
  let cadastreExcluded = 0;
  if (coproParcel && bestList.length > 0) {
    const verifiedList: AdemeRecord[] = [];
    for (const rec of bestList) {
      const recPoint = parseGeoPoint(rec._geopoint);
      if (!recPoint) {
        // Sans coords on ne peut pas vérifier — on garde (charge à BAN)
        verifiedList.push(rec);
        continue;
      }
      const recParcel = await getParcelByPoint(recPoint.lat, recPoint.lon);
      if (recParcel && recParcel.idu === coproParcel.idu) {
        verifiedList.push(rec);
      } else {
        cadastreExcluded++;
      }
    }
    if (verifiedList.length !== bestList.length) {
      bestList = verifiedList;
      if (verifiedList.length === 0) matchMode = "no_match";
    }
    cadastreVerified = true;
  }

  const result = computeReelEtSimule(bestList, n, matchMode);

  // 4. Niveau de qualité pour décision de prospection
  const hasCollectif = !!result.collectifReel;
  const indivCount = result.immeubleSimule?.nbDpeUtilises ?? 0;
  const quality = computeQuality({
    matchMode,
    hasCollectif,
    indivCount,
    cadastreVerified,
    cadastreExcluded,
    parcelMethod,
  });

  return {
    rayonM: usedR,
    totalDpeFound: lastRawTotal,
    totalDpeMatched: bestList.length,
    matchMode,
    matchReasons,
    quality,
    ctxResolved: {
      address: ctx.address,
      codePostal: ctx.codePostal,
      commune: ctx.commune,
      houseNumber: ctx.houseNumber,
      voieType: ctx.voieType,
      streetTokens: Array.from(ctx.streetTokens),
    },
    banResolved,
    parcelle: coproParcel
      ? {
          idu: coproParcel.idu,
          section: coproParcel.section,
          numero: coproParcel.numero,
          surfaceM2: coproParcel.contenance_m2,
          centroidLat: coproParcel.centroidLat,
          centroidLon: coproParcel.centroidLon,
          method: parcelMethod as "ref" | "point",
        }
      : null,
    ...result,
    matchedRecords: includeMatched ? bestList.map(toIndividuel) : undefined,
  };
}

function computeQuality(opts: {
  matchMode: MatchMode;
  hasCollectif: boolean;
  indivCount: number;
  cadastreVerified: boolean;
  cadastreExcluded: number;
  parcelMethod: "ref" | "point" | "none";
}): { level: QualityLevel; label: string; reason: string } {
  const { matchMode, hasCollectif, indivCount, cadastreVerified, cadastreExcluded } = opts;

  if (matchMode === "no_match" || (!hasCollectif && indivCount === 0)) {
    return {
      level: "no_data",
      label: "Aucune donnée DPE attribuable",
      reason: cadastreVerified
        ? `Tous les DPE candidats étaient sur des parcelles différentes (${cadastreExcluded} écartés par vérification cadastrale)`
        : "Aucun DPE strictement matché sur l'adresse",
    };
  }

  if (hasCollectif && cadastreVerified) {
    return {
      level: "verified",
      label: "Vérifié — DPE collectif réel + parcelle cadastrale confirmée",
      reason: "Source la plus fiable pour la prospection",
    };
  }

  if (indivCount >= 3 && cadastreVerified) {
    return {
      level: "verified",
      label: `Vérifié — ${indivCount} DPE individuels sur la parcelle`,
      reason: "Estimation fiable basée sur plusieurs lots du même immeuble",
    };
  }

  if (cadastreVerified && indivCount >= 1) {
    return {
      level: "approximate",
      label: `Approximatif — ${indivCount} DPE individuel${indivCount > 1 ? "s" : ""} sur la parcelle`,
      reason:
        "Parcelle vérifiée mais peu de lots — peut être représentatif si l'immeuble est homogène",
    };
  }

  if (!cadastreVerified) {
    return {
      level: "uncertain",
      label: "Incertain — parcelle cadastrale non identifiée",
      reason:
        "Le matching DPE n'a pas pu être validé par le cadastre IGN — à utiliser avec prudence",
    };
  }

  return {
    level: "uncertain",
    label: "Incertain",
    reason: "Données insuffisantes pour qualifier la fiabilité",
  };
}

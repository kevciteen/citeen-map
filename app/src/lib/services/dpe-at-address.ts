/**
 * Lookup unifié des DPE à une adresse exacte (vrai reflet ADEME).
 *
 * Pipeline :
 *   1. BAN forward sur la requête utilisateur → lat/lon + housenumber + street
 *   2. ADEME geo_distance autour des coords (rayons progressifs 25→80m)
 *   3. Filtre strict adresse (CP + commune + voie type + tokens rue + numéro)
 *   4. Segmentation par type ADEME canonique :
 *      - DPE COLLECTIF RÉEL d'immeuble (methode_application_dpe="dpe immeuble collectif")
 *      - DPE individuel d'appartement (réel diagnostic, pas dérivé)
 *      - DPE d'appartement dérivé d'un DPE immeuble
 *      - DPE maison individuelle
 *
 * Ne segmente PAS par module métier (Maisons / Appartements) — l'idée est
 * d'avoir la vue ADEME EXHAUSTIVE pour une adresse, et de laisser l'UI ou
 * le caller choisir quoi afficher.
 */
import { geocodeAddress } from "./ban";
import { getParcelByPoint } from "./cadastre";
import { fetchAdemeDpeAround, type AdemeRecord } from "./ademe";
import {
  fetchDpeTertiaireAround,
  isReallyTertiary,
  extractLatLon as extractTertLatLon,
  type DpeTertiaireRecord,
} from "./dpe-tertiaire";
import {
  fetchDpeLegacyAround,
  extractLegacyLatLon,
  type DpeLegacyRecord,
} from "./dpe-legacy";

export type DpeKind =
  | "collectif_reel"
  | "appartement_individuel"
  | "appartement_derive_immeuble"
  | "maison_individuelle"
  | "tertiaire"
  | "legacy_residentiel"
  | "autre";

export type DpeAtAddressItem = {
  kind: DpeKind;
  numero_dpe: string | null;
  numero_dpe_immeuble: string | null;
  etiquette_dpe: string | null;
  etiquette_ges: string | null;
  date_etablissement: string | null;
  date_modification: string | null;
  type_batiment: string | null;
  methode_application_dpe: string | null;
  numero_voie_ban: string | null;
  nom_rue_ban: string | null;
  code_postal_ban: string | null;
  nom_commune_ban: string | null;
  surface_habitable: number | null;
  conso_5_usages_par_m2_ep: number | null;
  lat: number | null;
  lon: number | null;
};

export type DpeAtAddressResult = {
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
  parcelle: { idu: string; centroidLat: number; centroidLon: number } | null;
  rayonMetres: number;
  totalAdeme: number;
  matchedCount: number;
  collectifsReels: DpeAtAddressItem[];
  appartementsIndividuels: DpeAtAddressItem[];
  appartementsDerivesImmeuble: DpeAtAddressItem[];
  maisonsIndividuelles: DpeAtAddressItem[];
  tertiaires: DpeAtAddressItem[];
  legacyResidentiels: DpeAtAddressItem[];
  autres: DpeAtAddressItem[];
  notes: string[];
};

const VOIE_TYPE_MAP: Record<string, string> = {
  r: "rue", rue: "rue",
  av: "avenue", ave: "avenue", avenue: "avenue",
  bd: "boulevard", blvd: "boulevard", boulevard: "boulevard",
  pl: "place", place: "place",
  rte: "route", route: "route",
  all: "allee", allee: "allee", "allée": "allee",
  imp: "impasse", impasse: "impasse",
  ch: "chemin", chemin: "chemin",
  sq: "square", square: "square",
  qu: "quai", quai: "quai",
  sente: "sente", se: "sente",
};

function normalizeAscii(value: unknown): string {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function extractVoieType(s: string): string | null {
  const ascii = normalizeAscii(s);
  const first = ascii.split(/\s+/)[0];
  return first ? (VOIE_TYPE_MAP[first] ?? null) : null;
}

const STREET_STOPWORDS = new Set([
  "de", "du", "des", "le", "la", "les", "et", "en", "au", "aux", "d", "l",
  ...Object.keys(VOIE_TYPE_MAP),
  ...Object.values(VOIE_TYPE_MAP),
]);

function streetTokens(s: string): Set<string> {
  const ascii = normalizeAscii(s);
  return new Set(
    ascii
      .split(/\s+/)
      .filter((t) => t.length >= 2)
      .filter((t) => !STREET_STOPWORDS.has(t))
      .filter((t) => !/^\d+$/.test(t)),
  );
}

function tokenOverlap(a: Set<string>, b: Set<string>): number {
  if (a.size === 0 || b.size === 0) return 0;
  let common = 0;
  for (const t of a) if (b.has(t)) common++;
  return common / Math.max(a.size, b.size);
}

function numericPart(s: string | null | undefined): string {
  return String(s ?? "").match(/\d+/)?.[0] ?? "";
}

/**
 * Distance haversine en mètres entre 2 coords WGS84.
 * Utilisée pour rejeter les DPE dont les coords ADEME tombent trop loin
 * du point BAN (cas Lénine : ~38m d'écart = parcelle voisine).
 */
function distanceMeters(
  lat1: number, lon1: number, lat2: number, lon2: number,
): number {
  const R = 6371e3;
  const phi1 = (lat1 * Math.PI) / 180;
  const phi2 = (lat2 * Math.PI) / 180;
  const dPhi = ((lat2 - lat1) * Math.PI) / 180;
  const dLambda = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

const TERTIARY_MAX_DISTANCE_M = 20;

function classify(r: AdemeRecord): DpeKind {
  const meth = String(r?.methode_application_dpe ?? "").toLowerCase();
  if (meth.includes("immeuble collectif")) return "collectif_reel";
  if (meth.includes("appartement généré") || meth.includes("appartement genere"))
    return "appartement_derive_immeuble";
  if (meth.includes("appartement individuel") || meth.includes("appartement"))
    return "appartement_individuel";
  if (meth.includes("maison individuelle") || meth.includes("maison"))
    return "maison_individuelle";
  // Fallback : si type_batiment dit immeuble, on prend collectif
  const tb = String(r?.type_batiment ?? "").toLowerCase();
  if (tb === "immeuble") return "collectif_reel";
  if (tb === "appartement") return "appartement_individuel";
  if (tb === "maison") return "maison_individuelle";
  return "autre";
}

function getLatLon(r: AdemeRecord): { lat: number | null; lon: number | null } {
  const g = r?._geopoint;
  if (typeof g === "string") {
    const [a, b] = g.split(",").map((v) => Number(v.trim()));
    if (Number.isFinite(a) && Number.isFinite(b)) return { lat: a, lon: b };
  } else if (Array.isArray(g) && g.length === 2) {
    return { lat: Number(g[0]), lon: Number(g[1]) };
  } else if (g && typeof g === "object") {
    const o = g as { lat?: number; lon?: number };
    if (Number.isFinite(o.lat) && Number.isFinite(o.lon)) {
      return { lat: o.lat as number, lon: o.lon as number };
    }
  }
  return { lat: null, lon: null };
}

function toItem(r: AdemeRecord): DpeAtAddressItem {
  const { lat, lon } = getLatLon(r);
  return {
    kind: classify(r),
    numero_dpe: r.numero_dpe ?? null,
    numero_dpe_immeuble: r.numero_dpe_immeuble ?? null,
    etiquette_dpe: r.etiquette_dpe ?? null,
    etiquette_ges: r.etiquette_ges ?? null,
    date_etablissement: r.date_etablissement_dpe ?? null,
    date_modification: r.date_derniere_modification_dpe ?? null,
    type_batiment: r.type_batiment ?? null,
    methode_application_dpe: r.methode_application_dpe ?? null,
    numero_voie_ban: r.numero_voie_ban ?? null,
    nom_rue_ban: r.nom_rue_ban ?? null,
    code_postal_ban: r.code_postal_ban ?? null,
    nom_commune_ban: r.nom_commune_ban ?? null,
    surface_habitable:
      Number(r.surface_habitable_logement ?? r.surface_habitable_immeuble ?? NaN) || null,
    conso_5_usages_par_m2_ep: Number(r.conso_5_usages_par_m2_ep ?? NaN) || null,
    lat,
    lon,
  };
}

export async function lookupDpeAtAddress(
  query: string,
): Promise<DpeAtAddressResult> {
  const notes: string[] = [];

  // 1. BAN
  const banResults = await geocodeAddress(query, { limit: 1 });
  const ban = banResults[0];
  if (!ban || ban.score < 0.4) {
    return {
      banResolved: null,
      parcelle: null,
      rayonMetres: 0,
      totalAdeme: 0,
      matchedCount: 0,
      collectifsReels: [],
      appartementsIndividuels: [],
      appartementsDerivesImmeuble: [],
      maisonsIndividuelles: [],
      tertiaires: [],
      legacyResidentiels: [],
      autres: [],
      notes: [
        ban
          ? `BAN renvoie ${ban.label} mais score trop faible (${ban.score.toFixed(2)} < 0.4)`
          : "Adresse non trouvée par la BAN",
      ],
    };
  }
  notes.push(
    `BAN : ${ban.label} (score ${Math.round(ban.score * 100)}%)`,
  );

  // 2. Cadastre (best-effort)
  const parcelle = await getParcelByPoint(ban.lat, ban.lon).catch(() => null);
  if (parcelle) {
    notes.push(`Parcelle IGN : ${parcelle.idu}`);
  }

  // ★ Préparation des targets pour le filtre adresse (sans le filtre
  //   type_batiment qui faisait perdre des résultats).
  const targetStreet = ban.street ?? ban.label;
  const targetTokens = streetTokens(targetStreet);
  const targetVoieType = extractVoieType(targetStreet);
  const targetHouseN = numericPart(ban.housenumber);
  const targetPostcode = String(ban.postcode ?? "").trim();
  const targetCity = normalizeAscii(ban.city ?? "");

  const applyStrictFilter = (rs: AdemeRecord[]): AdemeRecord[] => rs.filter((r) => {
    if (targetPostcode) {
      const recCp = String(r.code_postal_ban ?? r.code_postal_brut ?? "").trim();
      if (recCp && recCp !== targetPostcode) return false;
    }
    if (targetCity) {
      const recCity = normalizeAscii(r.nom_commune_ban ?? r.nom_commune_brut);
      if (recCity && recCity !== targetCity) return false;
    }
    const recStreet = String(r.nom_rue_ban ?? r.adresse_ban ?? r.adresse_complete_brut ?? "");
    if (targetVoieType) {
      const recType = extractVoieType(recStreet);
      if (recType && recType !== targetVoieType) return false;
    }
    if (targetTokens.size > 0) {
      const overlap = tokenOverlap(targetTokens, streetTokens(recStreet));
      if (overlap < 0.7) return false;
    }
    if (targetHouseN) {
      const recN = numericPart(String(r.numero_voie_ban ?? ""));
      if (recN && recN !== targetHouseN) return false;
    }
    return true;
  });

  // 3. ADEME résidentiel : rayons progressifs (80→150→300m).
  //    On ré-essaie tant qu'on n'a pas de match strict (au cas où le
  //    _geopoint ADEME est éloigné du point BAN — cas du "2 Av Lénine"
  //    où ADEME a le DPE mais à >25m du géocodage BAN).
  let raw: AdemeRecord[] = [];
  let matched: AdemeRecord[] = [];
  let usedR = 80;
  for (const r of [80, 150, 300]) {
    raw = await fetchAdemeDpeAround({ lat: ban.lat, lon: ban.lon, r, size: 500 });
    usedR = r;
    matched = applyStrictFilter(raw);
    if (matched.length > 0) break;
  }
  notes.push(
    `ADEME résidentiel : ${raw.length} DPE bruts à ${usedR}m, ${matched.length} matchent strict`,
  );

  // Tertiaire (dataset différent)
  let rawTert: DpeTertiaireRecord[] = [];
  try {
    rawTert = await fetchDpeTertiaireAround({
      lat: ban.lat,
      lon: ban.lon,
      radiusM: usedR,
      size: 200,
    });
    notes.push(`ADEME tertiaire : ${rawTert.length} DPE bruts dans un rayon de ${usedR}m`);
  } catch (e) {
    notes.push(`ADEME tertiaire : erreur (${(e as Error).message})`);
  }

  // DPE Legacy résidentiel (méthode pré-2021, dataset dpe-france)
  let rawLegacy: DpeLegacyRecord[] = [];
  try {
    rawLegacy = await fetchDpeLegacyAround({
      lat: ban.lat,
      lon: ban.lon,
      radiusM: usedR,
      size: 200,
    });
    notes.push(`ADEME DPE legacy résidentiel : ${rawLegacy.length} DPE bruts à ${usedR}m`);
  } catch (e) {
    notes.push(`ADEME DPE legacy : erreur (${(e as Error).message})`);
  }

  // 4. Le filtre adresse strict a déjà été appliqué dans la boucle
  //    progressive ci-dessus (matched contient les DPE conservés).

  // 5. Segmentation par type ADEME canonique
  const items = matched.map(toItem);

  // Tertiaire : filtre STRICT (post-Lénine fix) :
  //   - isReallyTertiary (exige mot-clé tertiaire positif, exclut habitation)
  //   - CP + commune exacts
  //   - Numéro de voie exact (extrait du début de nom_rue/geo_adresse)
  //   - Token overlap >= 0.7 (mêmes seuils que résidentiel)
  //   - Cadastre IDU match avec la parcelle BAN (rejette les DPE qui ont
  //     bien la bonne adresse texte mais coords sur une parcelle voisine)
  const tertiaireCandidates = rawTert.filter((r) => {
    if (!isReallyTertiary(r)) return false;
    const recCp = String(r.code_postal ?? "").trim();
    if (targetPostcode && recCp && recCp !== targetPostcode) return false;
    const recCity = normalizeAscii(r.commune ?? "");
    if (targetCity && recCity && recCity !== targetCity) return false;
    const recStreetField = String(r.nom_rue ?? r.geo_adresse ?? "");
    // Numéro de voie : "2 AVENUE LENINE" → "2"
    if (targetHouseN) {
      const recN = recStreetField.trim().match(/^(\d+)/)?.[1] ?? "";
      if (!recN || recN !== targetHouseN) return false;
    }
    if (targetTokens.size > 0) {
      const overlap = tokenOverlap(targetTokens, streetTokens(recStreetField));
      if (overlap < 0.7) return false;
    }
    return true;
  });

  // Filtre coords STRICT pour les candidats restants :
  //   1. Distance BAN ↔ DPE doit être ≤ 20m (cas Lénine = 38m → rejeté)
  //   2. Si BAN a une parcelle cadastrale, le DPE doit être sur la
  //      même parcelle (vérification IGN best-effort, kept si IGN timeout)
  const tertiaireItems: DpeAtAddressItem[] = [];
  for (const r of tertiaireCandidates.slice(0, 10)) {
    const dpeCoords = extractTertLatLon(r);

    // ★ Check distance brut — déterministe et rapide
    if (dpeCoords) {
      const dist = distanceMeters(ban.lat, ban.lon, dpeCoords.lat, dpeCoords.lon);
      if (dist > TERTIARY_MAX_DISTANCE_M) {
        notes.push(
          `DPE tertiaire ${r.numero_dpe ?? "?"} rejeté : ${dist.toFixed(0)}m du point BAN (> ${TERTIARY_MAX_DISTANCE_M}m)`,
        );
        continue;
      }
    } else {
      // Sans coords ADEME, on ne peut pas vérifier — rejette par défaut
      notes.push(
        `DPE tertiaire ${r.numero_dpe ?? "?"} rejeté : pas de coordonnées GPS ADEME`,
      );
      continue;
    }

    // Check cadastre IDU (si parcelle BAN dispo) — couche supplémentaire
    if (parcelle && dpeCoords) {
      const dpePar = await getParcelByPoint(dpeCoords.lat, dpeCoords.lon).catch(() => null);
      if (dpePar && dpePar.idu !== parcelle.idu) {
        notes.push(
          `DPE tertiaire ${r.numero_dpe ?? "?"} rejeté : parcelle ${dpePar.idu} ≠ ${parcelle.idu}`,
        );
        continue;
      }
    }

    tertiaireItems.push(tertiaryToItem(r));
  }

  // Legacy résidentiel : filtre strict (CP + commune + voie + numéro + distance)
  const legacyItems: DpeAtAddressItem[] = [];
  for (const r of rawLegacy.slice(0, 50)) {
    const recCp = String(r.code_postal ?? "").trim();
    if (targetPostcode && recCp && recCp !== targetPostcode) continue;
    const recCity = normalizeAscii(r.commune ?? "");
    if (targetCity && recCity && recCity !== targetCity) continue;
    const recStreetField = String(r.nom_rue ?? r.geo_adresse ?? "");
    if (targetHouseN) {
      const recN = recStreetField.trim().match(/^(\d+)/)?.[1] ?? "";
      if (!recN || recN !== targetHouseN) continue;
    }
    if (targetTokens.size > 0) {
      const overlap = tokenOverlap(targetTokens, streetTokens(recStreetField));
      if (overlap < 0.7) continue;
    }
    // Distance check (anti-faux-positifs cross-parcel)
    const coords = extractLegacyLatLon(r);
    if (coords) {
      const dist = distanceMeters(ban.lat, ban.lon, coords.lat, coords.lon);
      if (dist > 30) continue; // legacy plus tolérant (30m vs 20m tertiaire)
    }
    legacyItems.push(legacyToItem(r));
  }

  const collectifsReels = items.filter((i) => i.kind === "collectif_reel");
  const appartementsIndividuels = items.filter((i) => i.kind === "appartement_individuel");
  const appartementsDerivesImmeuble = items.filter((i) => i.kind === "appartement_derive_immeuble");
  const maisonsIndividuelles = items.filter((i) => i.kind === "maison_individuelle");
  const autres = items.filter((i) => i.kind === "autre");

  // Tri par date desc (le plus récent en premier)
  const byDateDesc = (a: DpeAtAddressItem, b: DpeAtAddressItem) => {
    const ta = Date.parse(a.date_etablissement ?? "") || 0;
    const tb = Date.parse(b.date_etablissement ?? "") || 0;
    return tb - ta;
  };
  collectifsReels.sort(byDateDesc);
  appartementsIndividuels.sort(byDateDesc);
  appartementsDerivesImmeuble.sort(byDateDesc);
  maisonsIndividuelles.sort(byDateDesc);
  tertiaireItems.sort(byDateDesc);
  legacyItems.sort(byDateDesc);
  autres.sort(byDateDesc);

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
    parcelle: parcelle
      ? { idu: parcelle.idu, centroidLat: parcelle.centroidLat, centroidLon: parcelle.centroidLon }
      : null,
    rayonMetres: usedR,
    totalAdeme: raw.length,
    matchedCount: matched.length,
    collectifsReels,
    appartementsIndividuels,
    appartementsDerivesImmeuble,
    maisonsIndividuelles,
    tertiaires: tertiaireItems,
    legacyResidentiels: legacyItems,
    autres,
    notes,
  };
}

/** Convertit un DPE legacy résidentiel en DpeAtAddressItem */
function legacyToItem(r: DpeLegacyRecord): DpeAtAddressItem {
  const coords = extractLegacyLatLon(r);
  return {
    kind: "legacy_residentiel",
    numero_dpe: r.numero_dpe ?? null,
    numero_dpe_immeuble: null,
    etiquette_dpe: r.classe_consommation_energie ?? null,
    etiquette_ges: r.classe_estimation_ges ?? null,
    date_etablissement: r.date_etablissement_dpe ?? null,
    date_modification: null,
    type_batiment: r.tr002_type_batiment_libelle ?? "Logement",
    methode_application_dpe: "DPE pré-2021 (méthode 3CL ou autre)",
    numero_voie_ban: null,
    nom_rue_ban: r.nom_rue ?? r.geo_adresse ?? null,
    code_postal_ban: r.code_postal ?? null,
    nom_commune_ban: r.commune ?? null,
    surface_habitable:
      Number(r.surface_thermique_lot ?? r.surface_habitable ?? r.shon ?? NaN) || null,
    conso_5_usages_par_m2_ep: Number(r.consommation_energie ?? NaN) || null,
    lat: coords?.lat ?? null,
    lon: coords?.lon ?? null,
  };
}

/** Convertit un DPE tertiaire en DpeAtAddressItem (champs différents). */
function tertiaryToItem(r: DpeTertiaireRecord): DpeAtAddressItem {
  const coords = extractTertLatLon(r);
  return {
    kind: "tertiaire",
    numero_dpe: r.numero_dpe ?? null,
    numero_dpe_immeuble: null,
    etiquette_dpe: r.classe_consommation_energie ?? null,
    etiquette_ges: r.classe_estimation_ges ?? null,
    date_etablissement:
      (r as Record<string, unknown>).date_etablissement_dpe as string | undefined ?? null,
    date_modification:
      (r as Record<string, unknown>).date_arrete_legifrance as string | undefined ?? null,
    type_batiment: (r as Record<string, unknown>).tr002_type_batiment_libelle as string | undefined ?? "Tertiaire",
    methode_application_dpe: r.secteur_activite ?? null,
    numero_voie_ban: null,
    nom_rue_ban: (r.nom_rue ?? r.geo_adresse) ?? null,
    code_postal_ban: r.code_postal ?? null,
    nom_commune_ban: r.commune ?? null,
    surface_habitable:
      Number(r.surface_utile ?? r.surface_habitable ?? r.shon ?? NaN) || null,
    conso_5_usages_par_m2_ep: Number(r.consommation_energie ?? NaN) || null,
    lat: coords?.lat ?? null,
    lon: coords?.lon ?? null,
  };
}

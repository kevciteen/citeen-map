// backend/services/dpeService.js
const DPE_API_BASE =
  "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines";
const BAN_API_BASE = "https://api-adresse.data.gouv.fr/search/";

/* ------------------------------ cache simple ------------------------------ */
const cache = new Map();
const CACHE_TTL_MS = 10 * 60 * 1000;

function cacheGet(key) {
  const v = cache.get(key);
  if (!v) return null;
  if (Date.now() > v.exp) {
    cache.delete(key);
    return null;
  }
  return v.data;
}
function cacheSet(key, data, ttlMs = CACHE_TTL_MS) {
  cache.set(key, { exp: Date.now() + ttlMs, data });
}

/* ------------------------------- helpers --------------------------------- */
function parseDate(d) {
  const t = Date.parse(d);
  return Number.isFinite(t) ? t : 0;
}

function normalizeAscii(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeCompact(value) {
  return normalizeAscii(value).replace(/\s+/g, "");
}

function extractHouseNumber(value) {
  const match = normalizeAscii(value).match(/\b(\d{1,4})(?:\s*(bis|ter|quater))?\b/);
  if (!match) return null;
  return `${match[1]}${match[2] || ""}`;
}

function streetTokenSet(value) {
  const stopWords = new Set([
    "rue",
    "avenue",
    "av",
    "boulevard",
    "bd",
    "place",
    "route",
    "quai",
    "cours",
    "allee",
    "impasse",
    "chemin",
    "residence",
    "batiment",
  ]);

  return new Set(
    normalizeAscii(value)
      .split(/\s+/)
      .filter(Boolean)
      .filter((token) => !stopWords.has(token))
  );
}

function scoreTokenOverlap(left, right) {
  if (!(left instanceof Set) || !(right instanceof Set) || left.size === 0 || right.size === 0) {
    return 0;
  }

  let common = 0;
  for (const token of left) {
    if (right.has(token)) common += 1;
  }

  return common / Math.max(left.size, right.size);
}

function parseGeoPoint(point) {
  if (!point) return null;

  if (Array.isArray(point) && point.length >= 2) {
    const lat = Number(point[0]);
    const lon = Number(point[1]);
    return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
  }

  if (typeof point === "string") {
    const [latRaw, lonRaw] = point.split(",").map(Number);
    return Number.isFinite(latRaw) && Number.isFinite(lonRaw)
      ? { lat: latRaw, lon: lonRaw }
      : null;
  }

  if (typeof point === "object") {
    const lat = Number(point.lat ?? point.latitude);
    const lon = Number(point.lon ?? point.lng ?? point.longitude);
    return Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
  }

  return null;
}

function haversineDistanceMeters(from, to) {
  if (!from || !to) return Number.POSITIVE_INFINITY;

  const earthRadius = 6371000;
  const dLat = ((to.lat - from.lat) * Math.PI) / 180;
  const dLon = ((to.lon - from.lon) * Math.PI) / 180;
  const lat1 = (from.lat * Math.PI) / 180;
  const lat2 = (to.lat * Math.PI) / 180;

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;

  return 2 * earthRadius * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function buildAddressContext(rawContext = null) {
  if (!rawContext) return null;

  const label =
    rawContext.label ||
    [
      rawContext.address,
      rawContext.code_postal || rawContext.codePostal,
      rawContext.commune,
    ]
      .filter(Boolean)
      .join(" ");
  const inferredPostal = label.match(/\b(\d{5})\b/)?.[1] || "";
  const inferredCommune =
    label.match(/\b\d{5}\s+(.+)$/)?.[1]?.trim() ||
    "";

  const codePostal = String(rawContext.code_postal || rawContext.codePostal || inferredPostal).trim();
  const commune = String(rawContext.commune || inferredCommune).trim();
  const houseNumber =
    extractHouseNumber(rawContext.address) ||
    extractHouseNumber(label) ||
    null;
  const lat = Number(rawContext.lat);
  const lon = Number(rawContext.lon);

  return {
    label,
    address: String(rawContext.address || "").trim(),
    codePostal,
    commune,
    numeroImmatriculation: String(rawContext.numero_immatriculation || "").trim(),
    houseNumber,
    streetTokens: streetTokenSet(rawContext.address || label),
    lat: Number.isFinite(lat) ? lat : null,
    lon: Number.isFinite(lon) ? lon : null,
  };
}

function isPreciseAddressContext(context) {
  if (!context) return false;
  return Boolean(context.houseNumber && (context.codePostal || context.commune));
}

async function geocodeAddressContext(context) {
  if (!isPreciseAddressContext(context)) return null;

  const cacheKey = `ban:${normalizeCompact(context.label)}`;
  const hit = cacheGet(cacheKey);
  if (hit) return hit;

  const query = [context.address || context.label, context.codePostal, context.commune]
    .filter(Boolean)
    .join(" ");

  const url = new URL(BAN_API_BASE);
  url.searchParams.set("q", query);
  url.searchParams.set("limit", "1");
  url.searchParams.set("autocomplete", "0");

  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) {
    return null;
  }

  const json = await res.json();
  const first = Array.isArray(json?.features) ? json.features[0] : null;
  const coords = first?.geometry?.coordinates;
  if (!Array.isArray(coords) || coords.length !== 2) return null;

  const resolved = {
    lon: Number(coords[0]),
    lat: Number(coords[1]),
    label: first?.properties?.label || query,
  };

  if (!Number.isFinite(resolved.lon) || !Number.isFinite(resolved.lat)) {
    return null;
  }

  cacheSet(cacheKey, resolved);
  return resolved;
}

function pickNumber(d, keys) {
  for (const k of keys) {
    const v = d?.[k];
    if (v === null || v === undefined || v === "") continue;
    const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function consoToClasse(conso) {
  if (!Number.isFinite(conso)) return "NC";
  if (conso < 70) return "A";
  if (conso < 110) return "B";
  if (conso < 180) return "C";
  if (conso < 250) return "D";
  if (conso < 330) return "E";
  if (conso < 420) return "F";
  return "G";
}

/* ----------------------------- couleurs DPE ------------------------------ */
export function dpeClasseToColor(classe) {
  const c = String(classe || "NC").toUpperCase();
  const map = {
    A: "#009A44",
    B: "#52B54A",
    C: "#C8D400",
    D: "#F4E500",
    E: "#F9B233",
    F: "#EA5B0C",
    G: "#C00000",
    NC: "#6B7280",
  };
  return map[c] || map.NC;
}

function isCollectifRecord(d) {
  const typeDpe = String(d?.type_dpe || d?.type_dpe_batiment || "").toUpperCase();
  return Boolean(
    d?.numero_dpe_immeuble ||
      d?.numero_dpe_immeuble_associe ||
      typeDpe.includes("IMMEUBLE") ||
      typeDpe.includes("COLLECTIF")
  );
}

/* --------------------- dédoublonnage ADEME (robuste) ---------------------- */
// On ne garde que le "dernier état" par numero_dpe / numero_dpe_immeuble
function dedupeLatestByNumeroDpe(list) {
  const best = new Map();

  for (const d of Array.isArray(list) ? list : []) {
    const numero = d?.numero_dpe || d?.numero_dpe_immeuble || null;
    if (!numero) continue;

    const t1 =
      parseDate(d?.date_derniere_modification_dpe) ||
      parseDate(d?.date_etablissement_dpe);

    const prev = best.get(numero);
    const t0 = prev
      ? parseDate(prev?.date_derniere_modification_dpe) ||
        parseDate(prev?.date_etablissement_dpe)
      : -1;

    if (!prev || t1 >= t0) best.set(numero, d);
  }

  return [...best.values()];
}

/* --------------------------- fetch ADEME autour -------------------------- */
export async function fetchAdeMeDpeAround({
  lat,
  lon,
  r = 50,
  size = 2000,
}) {
  const latN = Number(lat);
  const lonN = Number(lon);
  const rN = Number(r);

  if (!Number.isFinite(latN) || !Number.isFinite(lonN)) {
    throw new Error("lat/lon invalides");
  }
  if (!Number.isFinite(rN) || rN <= 0) {
    throw new Error("r invalide");
  }

  const key = `${latN.toFixed(6)}|${lonN.toFixed(6)}|${rN}|${size}`;
  const hit = cacheGet(key);
  if (hit) return hit;

  const url = new URL(DPE_API_BASE);
  url.searchParams.set("size", String(size));
  // DataFair geo_distance attend "lon,lat,r" (r en mètres)
  url.searchParams.set("geo_distance", `${lonN},${latN},${rN}`);

  const res = await fetch(url, { headers: { accept: "application/json" } });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`ADEME HTTP ${res.status} ${txt}`);
  }

  const json = await res.json();
  const raw = Array.isArray(json?.results) ? json.results : [];

  const list = dedupeLatestByNumeroDpe(raw);
  cacheSet(key, list);
  return list;
}

/* --------------------- split collectif vs individuel ---------------------- */
function splitCollectifIndividuel(dpeList) {
  const items = (Array.isArray(dpeList) ? dpeList : []).map((d) => {
    const isCollectif = isCollectifRecord(d);
    return {
      raw: d,
      isCollectif,
      date:
        parseDate(d?.date_derniere_modification_dpe) ||
        parseDate(d?.date_etablissement_dpe),
      conso: pickNumber(d, ["conso_5_usages_par_m2_ep"]),
      classe: (d?.etiquette_dpe || "").toUpperCase() || "NC",
      ges: (d?.etiquette_ges || "").toUpperCase() || "NC",
    };
  });

  const collectifs = items.filter((x) => x.isCollectif);
  const indivs = items.filter((x) => !x.isCollectif);

  return { collectifs, indivs };
}

function scoreDpeRecord(record, addressContext, queryPoint = null) {
  if (!addressContext) return 0;

  let score = 0;
  const recordCity = normalizeAscii(record?.nom_commune_ban || record?.nom_commune_brut);
  const recordPostal = String(record?.code_postal_ban || record?.code_postal_brut || "").trim();
  const recordStreet = record?.nom_rue_ban || record?.adresse_ban || record?.adresse_complete_brut || "";
  const recordHouseNumber = normalizeCompact(record?.numero_voie_ban || "");
  const recordImmat = normalizeCompact(record?.numero_immatriculation_copropriete || "");

  if (addressContext.codePostal && recordPostal === addressContext.codePostal) score += 12;
  if (addressContext.commune && recordCity === normalizeAscii(addressContext.commune)) score += 12;

  const overlap = scoreTokenOverlap(addressContext.streetTokens, streetTokenSet(recordStreet));
  score += Math.round(overlap * 25);

  if (addressContext.houseNumber && recordHouseNumber) {
    if (recordHouseNumber.startsWith(normalizeCompact(addressContext.houseNumber))) {
      score += 18;
    } else {
      score -= 8;
    }
  }

  if (
    addressContext.numeroImmatriculation &&
    recordImmat &&
    recordImmat === normalizeCompact(addressContext.numeroImmatriculation)
  ) {
    score += 60;
  }

  const candidatePoint = parseGeoPoint(record?._geopoint);
  const originPoint =
    queryPoint ||
    (Number.isFinite(addressContext?.lat) && Number.isFinite(addressContext?.lon)
      ? { lat: addressContext.lat, lon: addressContext.lon }
      : null);

  if (originPoint && candidatePoint) {
    const distance = haversineDistanceMeters(originPoint, candidatePoint);
    if (distance <= 8) score += 40;
    else if (distance <= 15) score += 32;
    else if (distance <= 25) score += 24;
    else if (distance <= 50) score += 12;
    else if (distance <= 90) score += 4;
  }

  return score;
}

function filterDpeListByAddress(list, addressContext, queryPoint = null) {
  if (!addressContext) {
    return { filtered: Array.isArray(list) ? list : [], meta: {} };
  }

  const scored = (Array.isArray(list) ? list : [])
    .map((record) => ({
      record,
      score: scoreDpeRecord(record, addressContext, queryPoint),
      date:
        parseDate(record?.date_derniere_modification_dpe) ||
        parseDate(record?.date_etablissement_dpe),
    }))
    .sort((left, right) => right.score - left.score || right.date - left.date);

  const bestScore = scored[0]?.score ?? 0;
  if (bestScore < 20) {
    return {
      filtered: Array.isArray(list) ? list : [],
      meta: { address_match_mode: "fallback_radius", address_match_top_score: bestScore },
    };
  }

  const minAcceptedScore = Math.max(20, bestScore - 18);
  return {
    filtered: scored
      .filter((item) => item.score >= minAcceptedScore)
      .map((item) => item.record),
    meta: {
      address_match_mode: "scored",
      address_match_top_score: bestScore,
      address_match_min_score: minAcceptedScore,
    },
  };
}

function computeConfidence({ hasCollectif, indivCountUsed, maxIndiv = 5 }) {
  if (hasCollectif) return { score: 95, label: "Élevée (DPE collectif réel)" };
  if (!indivCountUsed)
    return { score: 5, label: "Très faible (aucun DPE exploitable)" };

  const ratio = Math.min(indivCountUsed / maxIndiv, 1);
  const score = Math.round(30 + ratio * 50); // 30..80

  const label =
    score >= 70
      ? "Bonne (simulation basée sur plusieurs lots)"
      : score >= 50
      ? "Moyenne (simulation partielle)"
      : "Faible (peu de DPE individuels)";

  return { score, label };
}

/* ----------------------- calcul reel / simule / final --------------------- */
export function computeReelEtSimule(dpeList, n = 5) {
  const { collectifs, indivs } = splitCollectifIndividuel(dpeList);

  const collectifRecent = collectifs
    .filter((x) => x.date)
    .slice()
    .sort((a, b) => b.date - a.date)[0];

  const dpeCollectifReel = collectifRecent
    ? {
        statut: "reel",
        type: "collectif",
        date: collectifRecent.raw?.date_etablissement_dpe || null,
        classe: collectifRecent.classe,
        classe_color: dpeClasseToColor(collectifRecent.classe),
        ges: collectifRecent.ges,
        conso_kwh_m2_an: collectifRecent.conso,
        numero_dpe:
          collectifRecent.raw?.numero_dpe ||
          collectifRecent.raw?.numero_dpe_immeuble ||
          null,
      }
    : null;

  const indivsTries = indivs
    .filter((x) => Number.isFinite(x.conso))
    .sort((a, b) => b.date - a.date)
    .slice(0, n);

  const simConso =
    indivsTries.length > 0
      ? indivsTries.reduce((s, x) => s + x.conso, 0) / indivsTries.length
      : null;

  const simClasse = Number.isFinite(simConso) ? consoToClasse(simConso) : "NC";

  const simMethod = indivsTries.length
    ? `moyenne_${indivsTries.length}_individuels_recents`
    : null;

  const dpeImmeubleSimule = Number.isFinite(simConso)
    ? {
        statut: "simule",
        type: "immeuble",
        methode: simMethod,
        conso_kwh_m2_an: Math.round(simConso),
        classe: simClasse,
        classe_color: dpeClasseToColor(simClasse),
        ges: "NC",
      }
    : null;

  const confidence = computeConfidence({
    hasCollectif: !!dpeCollectifReel,
    indivCountUsed: indivsTries.length,
    maxIndiv: n,
  });

  const base = dpeCollectifReel || dpeImmeubleSimule;

  const dpeImmeubleFinal = base
    ? { ...base, confiance: confidence }
    : {
        statut: "aucun",
        type: "immeuble",
        classe: "NC",
        classe_color: dpeClasseToColor("NC"),
        confiance: confidence,
      };

  return {
    dpeCollectifReel,
    dpeImmeubleSimule,
    dpeImmeubleFinal,
    meta: {
      dpe_total_raw: Array.isArray(dpeList) ? dpeList.length : 0,
      has_collectif_reel: !!dpeCollectifReel,
      indiv_eligibles: indivs.filter((x) => Number.isFinite(x.conso)).length,
      indiv_used_for_simulation: indivsTries.length,
      simulation_method: simMethod,
      final_source: dpeCollectifReel ? "collectif_reel" : dpeImmeubleSimule ? "immeuble_simule" : "aucun",
    },
  };
}


/* --------------------- fetch adaptatif (rayon croissant) ------------------ */
export async function fetchDpeAdaptive({ lat, lon, minResults = 8, addressContext = null }) {
  const radii = [15, 20, 30, 60, 120, 200];
  let last = [];
  let usedR = radii[radii.length - 1];
  let matchMeta = {};
  const queryPoint =
    Number.isFinite(Number(lat)) && Number.isFinite(Number(lon))
      ? { lat: Number(lat), lon: Number(lon) }
      : null;

  for (const r of radii) {
    const list = await fetchAdeMeDpeAround({ lat, lon, r, size: 2000 });
    const filtered = filterDpeListByAddress(list, addressContext, queryPoint);
    last = filtered.filtered;
    usedR = r;
    matchMeta = filtered.meta;
    if (last.length >= minResults) break;
  }

  return { list: last, usedR, matchMeta };
}

/**
 * API canonique utilisée par /copros/:id/dpe
 */
export async function getDpeForLatLon({
  lat,
  lon,
  minResults = 8,
  n = 5,
  addressContext = null,
  allowGeocode = false,
}) {
  const context = buildAddressContext(addressContext);
  let queryLat = Number(lat);
  let queryLon = Number(lon);
  let resolvedTarget = null;

  if (context && allowGeocode) {
    const geocoded = await geocodeAddressContext(context);
    if (geocoded) {
      queryLat = geocoded.lat;
      queryLon = geocoded.lon;
      context.lat = geocoded.lat;
      context.lon = geocoded.lon;
      context.label = geocoded.label || context.label;
      resolvedTarget = geocoded;
    }
  }

  if (context && Number.isFinite(queryLat) && Number.isFinite(queryLon)) {
    context.lat = queryLat;
    context.lon = queryLon;
  }

  const { list, usedR, matchMeta } = await fetchDpeAdaptive({
    lat: queryLat,
    lon: queryLon,
    minResults,
    addressContext: context,
  });
  const { dpeCollectifReel, dpeImmeubleSimule, dpeImmeubleFinal, meta } =
  computeReelEtSimule(list, n);

return {
  usedR,
  list,
  stats: { dpe_total: list.length, rayon_m: usedR },
  dpeCollectifReel,
  dpeImmeubleSimule,
  dpeImmeubleFinal,
  meta: {
    ...meta,
    ...matchMeta,
    resolved_target_label: resolvedTarget?.label || null,
    resolved_target_lat: resolvedTarget?.lat ?? null,
    resolved_target_lon: resolvedTarget?.lon ?? null,
  },
};
}

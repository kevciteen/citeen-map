// backend/services/dpeService.js
const DPE_API_BASE =
  "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines";

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
    const isCollectif = !!d?.numero_dpe_immeuble_associe;
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
        numero_dpe: collectifRecent.raw?.numero_dpe || null,
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
export async function fetchDpeAdaptive({ lat, lon, minResults = 8 }) {
  const radii = [15, 20, 30, 60, 120, 200];
  let last = [];
  let usedR = radii[radii.length - 1];

  for (const r of radii) {
    const list = await fetchAdeMeDpeAround({ lat, lon, r, size: 2000 });
    last = list;
    usedR = r;
    if (list.length >= minResults) break;
  }

  return { list: last, usedR };
}

/**
 * API canonique utilisée par /copros/:id/dpe
 */
export async function getDpeForLatLon({ lat, lon, minResults = 8, n = 5 }) {
  const { list, usedR } = await fetchDpeAdaptive({ lat, lon, minResults });
  const { dpeCollectifReel, dpeImmeubleSimule, dpeImmeubleFinal, meta } =
  computeReelEtSimule(list, n);

return {
  usedR,
  list,
  stats: { dpe_total: list.length, rayon_m: usedR },
  dpeCollectifReel,
  dpeImmeubleSimule,
  dpeImmeubleFinal,
  meta,
};
}

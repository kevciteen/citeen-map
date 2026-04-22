// backend/routes/export.js
import fp from "fastify-plugin";
import { getDpeForLatLon } from "../services/dpeService.js";

// ----------------------
// CSV helpers
// ----------------------
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[;"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsvLine(values) {
  return values.map(csvEscape).join(";") + "\n";
}

function buildLonLatBbox(minLon, minLat, maxLon, maxLat, startIndex = 1) {
  return {
    clause: `lon BETWEEN $${startIndex} AND $${startIndex + 1} AND lat BETWEEN $${startIndex + 2} AND $${startIndex + 3}`,
    params: [minLon, maxLon, minLat, maxLat],
  };
}

// ----------------------
// Concurrency helper (no lib)
// ----------------------
async function mapLimit(items, limit, fn) {
  const ret = [];
  const executing = [];
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item));
    ret.push(p);
    executing.push(p);

    const clean = () => {
      const idx = executing.indexOf(p);
      if (idx >= 0) executing.splice(idx, 1);
    };
    p.then(clean).catch(clean);

    if (executing.length >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(ret);
}

// ----------------------
// Date helpers for ADEME lines
// ----------------------
function parseDate(d) {
  const t = Date.parse(d);
  return Number.isFinite(t) ? t : 0;
}
function pickDateStr(d) {
  // Dans la dataset ADEME, on a souvent ces champs (selon les lignes)
  return (
    d?.date_derniere_modification_dpe ||
    d?.date_etablissement_dpe ||
    d?.date_reception_dpe ||
    ""
  );
}
function guessType(d) {
  // Si le DPE référence un DPE immeuble associé => c'est un "collectif"
  const isCollectif = !!d?.numero_dpe_immeuble_associe;
  return isCollectif ? "collectif" : "individuel";
}

// ----------------------
// FULL export helpers (union des clés)
// ----------------------
function collectAllKeys(list) {
  const set = new Set();
  for (const d of Array.isArray(list) ? list : []) {
    if (!d || typeof d !== "object") continue;
    for (const k of Object.keys(d)) set.add(k);
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b));
}

function normalizeRow(d) {
  const date =
    d?.date_derniere_modification_dpe ||
    d?.date_etablissement_dpe ||
    d?.date_reception_dpe ||
    "";

  const numero =
    d?.numero_dpe ||
    d?.numero_dpe_immeuble ||
    d?.numero_dpe_logement ||
    d?.numero_dpe_immeuble_associe ||
    "";

  return {
    _numero_dpe: numero,
    _date: date,
    _classe_dpe: (d?.etiquette_dpe || "").toUpperCase(),
    _classe_ges: (d?.etiquette_ges || "").toUpperCase(),
    _type_guess: guessType(d), // individuel/collectif (ton helper)
    _type_dpe_raw: d?.type_dpe || d?.type_dpe_batiment || "",
    _methode: d?.methode_application_dpe || d?.methodologie || d?.methodologie_calcul || "",
  };
}

function buildFullCsv(list) {
  const rawKeys = collectAllKeys(list);

  // colonnes normalisées en tête + toutes les clés ADEME ensuite
  const header = [
    "_numero_dpe",
    "_date",
    "_classe_dpe",
    "_classe_ges",
    "_type_guess",
    "_type_dpe_raw",
    "_methode",
    ...rawKeys,
  ];

  let out = "\uFEFF"; // BOM Excel
  out += toCsvLine(header);

  for (const d of Array.isArray(list) ? list : []) {
    const norm = normalizeRow(d);
    const row = header.map((k) => {
      if (k in norm) return norm[k];

      const v = d?.[k];
      if (v && typeof v === "object") return JSON.stringify(v);
      return v ?? "";
    });

    out += toCsvLine(row);
  }

  return out;
}


export default fp(async function exportRoutes(fastify) {
  // =========================================================================
  // ROUTE 1 : Export COPROS + DPE (réel/simulé/final)
  // =========================================================================
  fastify.get("/export/copros_dpe.csv", async (req, reply) => {
    const {
      bbox,
      limit = 2000,
      syndic,
      q,
      copro,
      commune,
      code_postal,
      numero_immatriculation,
      departement,
    } = req.query;
    if (!bbox) return reply.code(400).send({ error: "bbox requis" });

    const [minLon, minLat, maxLon, maxLat] = String(bbox).split(",").map(Number);
    if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) {
      return reply.code(400).send({ error: "bbox invalide" });
    }

    const lim = Math.min(Number(limit) || 2000, 5000);

    const where = [];
    const params = [];
    let i = 1;

    const bboxSql = buildLonLatBbox(minLon, minLat, maxLon, maxLat, i);
    where.push(bboxSql.clause);
    params.push(...bboxSql.params);
    i += 4;

    if (departement) {
      where.push(`departement = $${i++}`);
      params.push(String(departement));
    }
    if (code_postal) {
      where.push(`code_postal ILIKE $${i++}`);
      params.push(`${String(code_postal).trim()}%`);
    }
    if (commune) {
      where.push(`commune ILIKE $${i++}`);
      params.push(`%${commune}%`);
    }
    if (syndic) {
      where.push(`syndic ILIKE $${i++}`);
      params.push(`%${syndic}%`);
    }
    if (copro) {
      where.push(`nom_copro ILIKE $${i++}`);
      params.push(`%${copro}%`);
    }
    if (numero_immatriculation) {
      where.push(`numero_immatriculation ILIKE $${i++}`);
      params.push(`%${numero_immatriculation}%`);
    }
    if (q) {
      where.push(`(
        COALESCE(nom_copro,'') ILIKE $${i}
        OR COALESCE(adresse,'') ILIKE $${i}
        OR COALESCE(commune,'') ILIKE $${i}
        OR COALESCE(code_postal,'') ILIKE $${i}
        OR COALESCE(numero_immatriculation,'') ILIKE $${i}
      )`);
      params.push(`%${q}%`);
      i++;
    }

    const sql = `
      SELECT id, numero_immatriculation, nom_copro, adresse, code_postal, commune, syndic, departement, lat, lon
      FROM copros
      WHERE ${where.join(" AND ")}
      LIMIT ${lim};
    `;

    const { rows } = await fastify.db.query(sql, params);

    reply
      .header("Content-Type", "text/csv; charset=utf-8")
      .header("Content-Disposition", `attachment; filename="copros_dpe_export.csv"`);

    // BOM UTF-8 pour Excel
    let out = "\uFEFF";

    out += toCsvLine([
      "id",
      "numero_immatriculation",
      "nom_copro",
      "adresse",
      "code_postal",
      "commune",
      "syndic",
      "departement",
      "lat",
      "lon",

      // DPE COLLECTIF REEL (si dispo)
      "dpe_collectif_statut", // reel / vide
      "dpe_collectif_classe",
      "dpe_collectif_ges",
      "dpe_collectif_conso_kwh_m2_an",
      "dpe_collectif_date_realisation",
      "dpe_collectif_numero_dpe",

      // DPE IMMEUBLE SIMULE (si dispo)
      "dpe_simule_statut", // simule / vide
      "dpe_simule_classe",
      "dpe_simule_conso_kwh_m2_an",
      "dpe_simule_methode",

      // DPE FINAL (celui affiché)
      "dpe_final_statut", // reel / simule / aucun
      "dpe_final_classe",
      "dpe_final_ges",
      "dpe_final_conso_kwh_m2_an",
      "dpe_final_date", // si reel
      "confiance_score",
      "confiance_label",
      "rayon_m",
      "dpe_total",
    ]);

    // Concurrence raisonnable (ADEME)
    const enriched = await mapLimit(rows, 3, async (c) => {
      try {
        const dpe = await getDpeForLatLon({
          lat: Number(c.lat),
          lon: Number(c.lon),
          minResults: 8,
          n: 5,
        });
        return { c, dpe };
      } catch {
        return { c, dpe: null };
      }
    });

    for (const { c, dpe } of enriched) {
      const reel = dpe?.dpeCollectifReel || null;
      const sim = dpe?.dpeImmeubleSimule || null;
      const fin = dpe?.dpeImmeubleFinal || null;

      out += toCsvLine([
        c.id,
        c.numero_immatriculation,
        c.nom_copro,
        c.adresse,
        c.code_postal,
        c.commune,
        c.syndic,
        c.departement,
        c.lat,
        c.lon,

        // Collectif réel
        reel ? "reel" : "",
        reel?.classe || "",
        reel?.ges || "",
        reel?.conso_kwh_m2_an ?? "",
        reel?.date || "",
        reel?.numero_dpe || "",

        // Simulé
        sim ? "simule" : "",
        sim?.classe || "",
        sim?.conso_kwh_m2_an ?? "",
        sim?.methode || "",

        // Final
        fin?.statut || "aucun",
        fin?.classe || "NC",
        fin?.ges || "",
        fin?.conso_kwh_m2_an ?? "",
        fin?.date || "",
        fin?.confiance?.score ?? "",
        fin?.confiance?.label ?? "",
        dpe?.usedR ?? "",
        dpe?.list?.length ?? 0,
      ]);
    }

    return out;
  });

  // =========================================================================
  // ROUTE 2 : Export DPE “bruts” ADEME (tous ceux trouvés), dédoublonnés
  // =========================================================================
  // GET /export/dpes.csv?bbox=...&limit=200&maxDpePerCopro=200
  fastify.get("/export/dpes.csv", async (req, reply) => {
    const {
      bbox,
      limit = 500,
      syndic,
      q,
      copro,
      commune,
      code_postal,
      numero_immatriculation,
      departement,
      maxDpePerCopro = 200,
    } = req.query;
    if (!bbox) return reply.code(400).send({ error: "bbox requis" });

    const [minLon, minLat, maxLon, maxLat] = String(bbox).split(",").map(Number);
    if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) {
      return reply.code(400).send({ error: "bbox invalide" });
    }

    const lim = Math.min(Number(limit) || 500, 1500);
    const perCopro = Math.min(Number(maxDpePerCopro) || 200, 2000);

    const where = [];
    const params = [];
    let i = 1;

    const bboxSql = buildLonLatBbox(minLon, minLat, maxLon, maxLat, i);
    where.push(bboxSql.clause);
    params.push(...bboxSql.params);
    i += 4;

    if (departement) {
      where.push(`departement = $${i++}`);
      params.push(String(departement));
    }
    if (code_postal) {
      where.push(`code_postal ILIKE $${i++}`);
      params.push(`${String(code_postal).trim()}%`);
    }
    if (commune) {
      where.push(`commune ILIKE $${i++}`);
      params.push(`%${commune}%`);
    }
    if (syndic) {
      where.push(`syndic ILIKE $${i++}`);
      params.push(`%${syndic}%`);
    }
    if (copro) {
      where.push(`nom_copro ILIKE $${i++}`);
      params.push(`%${copro}%`);
    }
    if (numero_immatriculation) {
      where.push(`numero_immatriculation ILIKE $${i++}`);
      params.push(`%${numero_immatriculation}%`);
    }
    if (q) {
      where.push(`(
        COALESCE(nom_copro,'') ILIKE $${i}
        OR COALESCE(adresse,'') ILIKE $${i}
        OR COALESCE(commune,'') ILIKE $${i}
        OR COALESCE(code_postal,'') ILIKE $${i}
        OR COALESCE(numero_immatriculation,'') ILIKE $${i}
      )`);
      params.push(`%${q}%`);
      i++;
    }

    const sql = `
      SELECT id, numero_immatriculation, nom_copro, adresse, code_postal, commune, departement, lat, lon
      FROM copros
      WHERE ${where.join(" AND ")}
      LIMIT ${lim};
    `;
    const { rows } = await fastify.db.query(sql, params);

    reply
      .header("Content-Type", "text/csv; charset=utf-8")
      .header("Content-Disposition", `attachment; filename="dpes_export.csv"`);

    let out = "\uFEFF";
    out += toCsvLine([
      "copro_id",
      "copro_numero_immatriculation",
      "copro_nom",
      "copro_adresse",
      "copro_code_postal",
      "copro_commune",
      "copro_departement",
      "copro_lat",
      "copro_lon",
      "rayon_m",

      // DPE brut
      "numero_dpe",
      "type_dpe", // collectif/individuel
      "classe_dpe",
      "ges",
      "conso_5_usages_par_m2_ep",
      "date_principale", // dernière modif sinon établissement
    ]);

    // Concurrence raisonnable
    const packs = await mapLimit(rows, 3, async (c) => {
      try {
        const dpe = await getDpeForLatLon({
          lat: Number(c.lat),
          lon: Number(c.lon),
          minResults: 8,
          n: 5,
        });
        // dpe.list est déjà dédoublonnée côté service (dernier état)
        return { c, usedR: dpe.usedR, list: dpe.list || [] };
      } catch {
        return { c, usedR: "", list: [] };
      }
    });

    for (const pack of packs) {
      const { c, usedR, list } = pack;

      // On prend les “perCopro” plus récents
      const sorted = list
        .slice()
        .sort((a, b) => parseDate(pickDateStr(b)) - parseDate(pickDateStr(a)))
        .slice(0, perCopro);

      for (const d of sorted) {
        const numero =
          d?.numero_dpe || d?.numero_dpe_immeuble || d?.numero_dpe_logement || "";
        if (!numero) continue;

        out += toCsvLine([
          c.id,
          c.numero_immatriculation,
          c.nom_copro,
          c.adresse,
          c.code_postal,
          c.commune,
          c.departement,
          c.lat,
          c.lon,
          usedR,

          numero,
          guessType(d),
          (d?.etiquette_dpe || "").toUpperCase() || "",
          (d?.etiquette_ges || "").toUpperCase() || "",
          d?.conso_5_usages_par_m2_ep ?? "",
          pickDateStr(d),
        ]);
      }
    }
      // =========================================================================
  // ROUTE 3 : Export FULL (tous champs ADEME) pour UNE COPRO (par fiche)
  // =========================================================================
  fastify.get("/copros/:id/dpe/export_full.csv", async (req, reply) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: "id invalide" });

    const { rows } = await fastify.db.query(
      `SELECT id, nom_copro, adresse, code_postal, commune, lat, lon
       FROM copros WHERE id=$1`,
      [id]
    );
    const copro = rows[0];
    if (!copro) return reply.code(404).send({ error: "copro introuvable" });

    const dpe = await getDpeForLatLon({
      lat: Number(copro.lat),
      lon: Number(copro.lon),
      minResults: 8,
      n: 5,
    });

    const list = dpe?.list || [];
    const csv = buildFullCsv(list);

    const safeName = (copro.nom_copro || copro.adresse || `copro_${id}`)
      .replace(/[^\w\d-_]+/g, "_")
      .slice(0, 80);

    reply
      .header("Content-Type", "text/csv; charset=utf-8")
      .header("Content-Disposition", `attachment; filename="fiche_${safeName}_${id}_dpes_full.csv"`);

    return csv;
  });

    // =========================================================================
  // ROUTE 4 : Export FULL (tous champs ADEME) pour UNE ADRESSE (lat/lon)
  // =========================================================================
  fastify.get("/export/dpe_around_full.csv", async (req, reply) => {
    const lat = Number(req.query.lat);
    const lon = Number(req.query.lon);
    const r = req.query.r ? Number(req.query.r) : 50;

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return reply.code(400).send({ error: "lat/lon invalides" });
    }

    const dpe = await getDpeForLatLon({
      lat,
      lon,
      minResults: 8,
      n: 5,
      // si ton service utilise un rayon progressif, r est indicatif
    });

    const list = dpe?.list || [];
    const csv = buildFullCsv(list);

    reply
      .header("Content-Type", "text/csv; charset=utf-8")
      .header("Content-Disposition", `attachment; filename="adresse_${lat}_${lon}_dpes_full.csv"`);

    return csv;
  });


    return out;
  });
});

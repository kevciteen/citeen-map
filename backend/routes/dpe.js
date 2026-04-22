// backend/routes/dpe.js
import fp from "fastify-plugin";
import {
  fetchAdeMeDpeAround,
  computeReelEtSimule,
  getDpeForLatLon,
  dpeClasseToColor,
} from "../services/dpeService.js";

/* ------------------------- helpers robustes ADEME ------------------------- */
function parseDateMs(d) {
  const t = Date.parse(d);
  return Number.isFinite(t) ? t : 0;
}
function pick(obj, keys) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== null && v !== undefined && v !== "") return v;
  }
  return null;
}
function pickByRegex(obj, regexes) {
  if (!obj) return null;
  const entries = Object.entries(obj);
  for (const re of regexes) {
    const hit = entries.find(([k, v]) => re.test(k) && v !== null && v !== undefined && v !== "");
    if (hit) return hit[1];
  }
  return null;
}
function pickNumByRegex(obj, regexes) {
  const v = pickByRegex(obj, regexes);
  if (v === null || v === undefined) return null;
  const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
  return Number.isFinite(n) ? n : null;
}
function listNonEmptyKeys(obj) {
  if (!obj) return [];
  return Object.keys(obj).filter((k) => {
    const v = obj[k];
    return v !== null && v !== undefined && v !== "" && !(typeof v === "number" && !Number.isFinite(v));
  });
}

function pickNum(obj, keys) {
  const v = pick(obj, keys);
  if (v === null) return null;
  const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
  return Number.isFinite(n) ? n : null;
}
function upper(v, fallback = "NC") {
  const s = String(v ?? "").trim().toUpperCase();
  return s || fallback;
}
function formatMaybe(v) {
  if (v === null || v === undefined || v === "") return null;
  return String(v);
}
function pickLatestRecord(list) {
  if (!Array.isArray(list) || list.length === 0) return null;
  return list
    .slice()
    .sort((a, b) => {
      const ta =
        parseDateMs(a?.date_derniere_modification_dpe) ||
        parseDateMs(a?.date_etablissement_dpe);
      const tb =
        parseDateMs(b?.date_derniere_modification_dpe) ||
        parseDateMs(b?.date_etablissement_dpe);
      return tb - ta;
    })[0];
}
function computeClasseCounts(list) {
  const counts = { A: 0, B: 0, C: 0, D: 0, E: 0, F: 0, G: 0, NC: 0 };
  for (const d of Array.isArray(list) ? list : []) {
    const c = upper(d?.etiquette_dpe, "NC");
    if (!counts[c]) counts.NC++;
    else counts[c]++;
  }
  return counts;
}

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

/* --------------------------- GoRenov-like builder -------------------------- */
function buildGoRenov({
  copro,
  stats,
  list,
  dpeCollectifReel,
  dpeImmeubleSimule,
  dpeImmeubleFinal,
}) {
  const safeList = Array.isArray(list) ? list : [];
  const latest = pickLatestRecord(safeList);
  const classeCounts = computeClasseCounts(safeList);

  const rep = latest
    ? {
        date_realisation: formatMaybe(
          pick(latest, ["date_etablissement_dpe", "date_derniere_modification_dpe"])
        ),
        identifiant: formatMaybe(pick(latest, ["numero_dpe", "numero_dpe_immeuble"])),
        type_dpe: formatMaybe(pick(latest, ["type_dpe", "type_dpe_batiment"])),
        methodology: formatMaybe(
          pick(latest, ["methode_application_dpe", "methodologie", "methodologie_calcul"])
        ),
        etiquette_dpe_officielle: upper(pick(latest, ["etiquette_dpe"]), "NC"),
        etiquette_ges_officielle: upper(pick(latest, ["etiquette_ges"]), "NC"),
        conso_5_usages_kwh_m2_an: pickNum(latest, [
          "conso_5_usages_par_m2_ep",
          "conso_5_usages_par_m2",
          "conso_5_usages_par_m2_finale",
        ]),
        emissions_ges_kgco2_m2_an: pickNum(latest, [
          "emission_ges_5_usages_par_m2",
          "emission_ges_par_m2",
          "emission_ges",
        ]),
        adresse_postale_principale: formatMaybe(
          pick(latest, ["adresse_bien", "adresse", "adresse_complete"])
        ),
        code_insee: formatMaybe(pick(latest, ["code_insee_commune", "code_insee"])),
        code_iris: formatMaybe(pick(latest, ["code_iris"])),
        altitude_m: pickNum(latest, ["altitude"]),
        perimetre_bat_hist_m: pickNum(latest, [
          "perimetre_batiment_historique",
          "perimetre_bat_hist",
        ]),
        nom_bat_hist: formatMaybe(
          pick(latest, ["nom_batiment_historique", "nom_bat_hist"])
        ),
      }
    : null;

  const caracteristiques = latest
    ? {
        categorie_usage: formatMaybe(
          pick(latest, ["categorie_usage_batiment", "usage_principal", "type_batiment"])
        ),
        annee_construction: pickNum(latest, [
          "annee_construction",
          "annee_construction_batiment",
        ]),
        nb_logements_total: pickNum(latest, [
          "nombre_logements",
          "nb_logements_total",
          "nombre_logements_total",
        ]),
        etages: pickNum(latest, ["etages", "nb_etages"]),
        hauteur_m: pickNum(latest, ["hauteur", "hauteur_m"]),
        emprise_sol_m2: pickNum(latest, ["emprise_au_sol", "emprise_sol"]),
        surface_habitable_m2: pickNum(latest, [
          "surface_habitable_logement",
          "surface_habitable",
          "shab",
        ]),
        surface_toiture_m2: pickNum(latest, ["surface_toiture", "surface_de_toiture"]),
        surface_facade_vitree_m2: pickNum(latest, [
          "surface_facade_vitree",
          "surface_facade_vitree_m2",
        ]),
        pourcentage_vitrage_ext: pickNum(latest, [
          "pourcentage_vitrage_exterieur",
          "pct_vitrage_exterieur",
        ]),
      }
    : null;

  const identification = latest
    ? {
        numero_immatriculation: formatMaybe(
          pick(latest, ["numero_immatriculation", "numero_immatriculation_copro"])
        ),
        identifiant_parcelle: formatMaybe(
          pick(latest, ["identifiant_parcelle", "id_parcelle", "parcelle"])
        ),
        identifiant_batiment_bdnb: formatMaybe(
          pick(latest, ["identifiant_batiment_bdnb", "bdnb_id_batiment", "id_bdnb"])
        ),
        cle_interop_adresse: formatMaybe(
          pick(latest, ["cle_interoperabilite_adresse", "cle_interop_adresse"])
        ),
      }
    : null;

    const systemes = latest
  ? {
      // Chauffage
      type_installation_chauffage:
        formatMaybe(pick(latest, ["type_installation_chauffage", "installation_chauffage"])) ??
        formatMaybe(pickByRegex(latest, [/type.*install.*chauffage/i, /install.*chauffage/i, /chauffage.*install/i])),

      energie_chauffage:
        formatMaybe(pick(latest, ["energie_chauffage", "energie_chauffage_principale"])) ??
        formatMaybe(pickByRegex(latest, [/energie.*chauffage/i, /chauffage.*energie/i])),

      type_chauffage:
        formatMaybe(pick(latest, ["type_chauffage"])) ??
        formatMaybe(pickByRegex(latest, [/type.*chauffage/i, /chauffage.*type/i])),

      generateur_chauffage:
        formatMaybe(pick(latest, ["generateur_chauffage", "generateur_chauffage_principal"])) ??
        formatMaybe(pickByRegex(latest, [/generateur.*chauffage/i, /chauffage.*generateur/i, /equipement.*chauffage/i])),

      // ECS (eau chaude sanitaire)
      ecs_type_installation:
        formatMaybe(pick(latest, ["type_installation_ecs", "installation_ecs"])) ??
        formatMaybe(pickByRegex(latest, [/type.*install.*ecs/i, /install.*ecs/i, /chauffe.?eau.*install/i])),

      ecs_energie:
        formatMaybe(pick(latest, ["energie_ecs", "energie_ef_chauffe_eau"])) ??
        formatMaybe(pickByRegex(latest, [/energie.*ecs/i, /energie.*chauffe.?eau/i, /ecs.*energie/i])),

      ecs_generateur:
        formatMaybe(pick(latest, ["generateur_ecs", "generateur_chauffe_eau"])) ??
        formatMaybe(pickByRegex(latest, [/generateur.*ecs/i, /chauffe.?eau.*generateur/i, /type.*chauffe.?eau/i])),

      ecs_mode_production:
        formatMaybe(pick(latest, ["mode_production_ecs"])) ??
        formatMaybe(pickByRegex(latest, [/ballon/i, /stockage/i, /instantan/i, /accumulation/i, /thermodynam/i])),

      // Ventilation
      ventilation_type:
        formatMaybe(pick(latest, ["type_ventilation", "ventilation"])) ??
        formatMaybe(pickByRegex(latest, [/vmc/i, /ventilation/i, /hygro/i, /naturelle/i])),

      ventilation_presence:
        formatMaybe(pick(latest, ["presence_ventilation"])) ??
        formatMaybe(pickByRegex(latest, [/presence.*ventil/i, /ventil.*presence/i])),

      // Clim / rafraîchissement
      climatisation_type:
        formatMaybe(pick(latest, ["type_climatisation", "climatisation", "generateur_climatisation"])) ??
        formatMaybe(pickByRegex(latest, [/clim/i, /rafraich/i, /refroid/i])),

      anciennete_chauffage:
        formatMaybe(pick(latest, ["anciennete_chauffage_principal", "anciennete_chauffage"])) ??
        formatMaybe(pickByRegex(latest, [/anciennet.*chauffage/i])),

      anciennete_ecs:
        formatMaybe(pick(latest, ["anciennete_ecs", "anciennete_chauffe_eau"])) ??
        formatMaybe(pickByRegex(latest, [/anciennet.*ecs/i, /anciennet.*chauffe.?eau/i])),

      anciennete_ventilation:
        formatMaybe(pick(latest, ["anciennete_ventilation"])) ??
        formatMaybe(pickByRegex(latest, [/anciennet.*ventil/i])),
    }
  : null;


      const enveloppe = latest
  ? {
      murs_materiau:
        formatMaybe(pick(latest, ["materiaux_mur_exterieur", "materiau_mur_exterieur"])) ??
        formatMaybe(pickByRegex(latest, [/mur.*mater/i, /mater.*mur/i, /type.*mur/i])),

      murs_isolation:
        formatMaybe(pick(latest, ["type_isolation_mur_exterieur", "isolation_mur_exterieur"])) ??
        formatMaybe(pickByRegex(latest, [/isol.*mur/i, /mur.*isol/i])),

      toiture_materiau:
        formatMaybe(pick(latest, ["materiaux_toiture", "materiau_toiture"])) ??
        formatMaybe(pickByRegex(latest, [/toit/i, /toiture/i, /couverture/i])),

      toiture_isolation:
        formatMaybe(pick(latest, ["type_isolation_plancher_haut", "isolation_plancher_haut"])) ??
        formatMaybe(pickByRegex(latest, [/isol.*plancher.*haut/i, /isol.*toit/i, /toit.*isol/i])),

      plancher_bas_isolation:
        formatMaybe(pick(latest, ["type_isolation_plancher_bas", "isolation_plancher_bas"])) ??
        formatMaybe(pickByRegex(latest, [/isol.*plancher.*bas/i, /plancher.*bas.*isol/i])),

      inertie:
        formatMaybe(pick(latest, ["classe_inertie"])) ??
        formatMaybe(pickByRegex(latest, [/inertie/i])),

      ponts_thermiques:
        formatMaybe(pick(latest, ["ponts_thermiques"])) ??
        formatMaybe(pickByRegex(latest, [/pont.*therm/i])),

      etancheite_air:
        formatMaybe(pick(latest, ["etancheite_air"])) ??
        formatMaybe(pickByRegex(latest, [/etanche/i, /perm.*air/i])),
    }
  : null;


     const vitrage = latest
  ? {
      type_vitrage:
        formatMaybe(pick(latest, ["type_vitrage"])) ??
        formatMaybe(pickByRegex(latest, [/vitrage/i, /double/i, /triple/i, /simple/i])),

      menuiseries_materiau:
        formatMaybe(pick(latest, ["menuiserie_vitrage", "materiau_menuiserie"])) ??
        formatMaybe(pickByRegex(latest, [/menuiser/i, /mater.*menuiser/i, /cadre/i, /alu/i, /pvc/i, /bois/i])),

      protections_solaires:
        formatMaybe(pick(latest, ["type_fermeture_vitrage", "fermeture_vitrage"])) ??
        formatMaybe(pickByRegex(latest, [/volet/i, /store/i, /fermeture/i, /brise.?soleil/i])),

      vitrage_renforce:
        formatMaybe(pick(latest, ["vitrage_renforce"])) ??
        formatMaybe(pickByRegex(latest, [/renforce/i])),

      coeff_u_vitrage:
        pickNum(latest, ["coefficient_transmission_vitrage", "u_vitrage"]) ??
        pickNumByRegex(latest, [/u.*vitrage/i, /transmission.*vitrage/i]),

      orientation:
        formatMaybe(pick(latest, ["orientation_vitrage"])) ??
        formatMaybe(pickByRegex(latest, [/orientation/i, /expo/i])),
    }
  : null;



  const consoReelle = latest
    ? {
        nb_pdl_electricite: pickNum(latest, [
          "nombre_pdl_electricite_residentiel",
          "nb_pdl_elec",
        ]),
        conso_elec_kwh_an: pickNum(latest, [
          "consommation_electricite_residentiel",
          "conso_elec_kwh_an",
        ]),
        nb_pdl_gaz: pickNum(latest, ["nombre_pdl_gaz_residentiel", "nb_pdl_gaz"]),
        conso_gaz_kwh_an: pickNum(latest, [
          "consommation_gaz_residentiel",
          "conso_gaz_kwh_an",
        ]),
      }
    : null;

  const simulations = latest
    ? {
        etiquette_dpe_batiment_simule: upper(
          pick(latest, ["etiquette_dpe_batiment_simule", "etiquette_simulation_dpe"]),
          "NC"
        ),
        indicateur_surchauffe_estivale: formatMaybe(
          pick(latest, ["indicateur_surchauffe_estivale", "risque_surchauffe"])
        ),
        estimation_etiquette_apres_travaux: upper(
          pick(latest, ["estimation_etiquette_dpe_apres_travaux", "etiquette_apres_travaux"]),
          "NC"
        ),
        plus_value_verte: formatMaybe(pick(latest, ["plus_value_verte"])),
      }
    : null;

  const final = dpeImmeubleFinal
    ? {
        ...dpeImmeubleFinal,
        classe_color: dpeImmeubleFinal.classe_color || dpeClasseToColor(dpeImmeubleFinal.classe),
      }
    : null;

  const collectif = dpeCollectifReel
    ? {
        ...dpeCollectifReel,
        statut: "reel",
        classe_color: dpeCollectifReel.classe_color || dpeClasseToColor(dpeCollectifReel.classe),
      }
    : null;

  const immeubleSimule = dpeImmeubleSimule
    ? {
        ...dpeImmeubleSimule,
        statut: "simule",
        classe_color: dpeImmeubleSimule.classe_color || dpeClasseToColor(dpeImmeubleSimule.classe),
      }
    : null;

   return {
    header: {
      titre: copro?.nom_copro || copro?.adresse || "Immeuble",
      adresse: [copro?.adresse, copro?.code_postal, copro?.commune]
        .filter(Boolean)
        .join(" "),
    },

    dpe: {
      final,
      collectif_reel: collectif,
      immeuble_simule: immeubleSimule,
      stats: {
        dpe_total: stats?.dpe_total ?? 0,
        rayon_m: stats?.rayon_m ?? null,
        counts_by_classe: classeCounts,
      },
      meta: {
        final_source: stats?.final_source ?? null,
        indiv_used_for_simulation: stats?.indiv_used_for_simulation ?? null,
        indiv_eligibles: stats?.indiv_eligibles ?? null,
        simulation_method: stats?.simulation_method ?? null,
      },
    },

    sections: {
      caracteristiques,
      systemes,
      enveloppe,
      vitrage,
      simulations,
      conso_reelle: consoReelle,
      identification,
      dpe_representatif: rep,

      debug: latest
        ? { latest_non_empty_keys: listNonEmptyKeys(latest) }
        : null,
    },
  };
}


function makeFallbackDpe() {
  return {
    usedR: null,
    list: [],
    stats: { dpe_total: 0, rayon_m: null },
    dpeCollectifReel: null,
    dpeImmeubleSimule: null,
    dpeImmeubleFinal: {
      statut: "aucun",
      type: "immeuble",
      classe: "NC",
      classe_color: dpeClasseToColor("NC"),
      confiance: { score: 5, label: "Très faible (erreur API)" },
    },
  };
}

/* ---------------------------------- routes --------------------------------- */
export default fp(async function dpeRoutes(fastify) {
  // DPE autour d’un point (adresse)
  fastify.get("/dpe/around", async (req) => {
    const lat = Number(req.query.lat);
    const lon = Number(req.query.lon);
    const r = req.query.r ? Number(req.query.r) : 50;

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return fastify.httpErrors.badRequest("lat/lon invalides");
    }

    const list = await fetchAdeMeDpeAround({ lat, lon, r });
    const { dpeCollectifReel, dpeImmeubleSimule, dpeImmeubleFinal, meta } =
  computeReelEtSimule(list, 5);

const stats = { dpe_total: list.length, rayon_m: r, ...(meta || {}) };


    return {
      stats,
      usedR: r,
      list,
      dpeCollectifReel,
      dpeImmeubleSimule,
      dpeImmeubleFinal,
      goRenov: buildGoRenov({
        copro: null,
        stats,
        list,
        dpeCollectifReel,
        dpeImmeubleSimule,
        dpeImmeubleFinal,
      }),
    };
  });

  // Export tous les DPE (bruts ADEME) trouvés autour de la copro (collectif + individuel)
  fastify.get("/copros/dpe-summaries", async (req, reply) => {
    const ids = [
      ...new Set(
        String(req.query.ids || "")
          .split(",")
          .map((v) => Number(v.trim()))
          .filter(Number.isFinite)
      ),
    ].slice(0, 120);

    if (ids.length === 0) {
      return reply.code(400).send({ error: "ids requis" });
    }

    const { rows } = await fastify.db.query(
      `
        SELECT id, lat, lon, nom_copro, adresse, code_postal, commune
        FROM copros
        WHERE id = ANY($1::int[])
      `,
      [ids]
    );

    const byId = new Map(rows.map((row) => [row.id, row]));
    const items = await mapLimit(ids, 4, async (id) => {
      const copro = byId.get(id);
      if (!copro) return null;

      let dpe = makeFallbackDpe();
      try {
        dpe = await getDpeForLatLon({
          lat: Number(copro.lat),
          lon: Number(copro.lon),
          minResults: 8,
          n: 5,
        });
      } catch {
        dpe = makeFallbackDpe();
      }

      const final = dpe.dpeImmeubleFinal || makeFallbackDpe().dpeImmeubleFinal;
      return {
        id: copro.id,
        nom_copro: copro.nom_copro,
        adresse: [copro.adresse, copro.code_postal, copro.commune].filter(Boolean).join(" "),
        statut: final?.statut || "aucun",
        classe: final?.classe || "NC",
        classe_color: final?.classe_color || dpeClasseToColor("NC"),
        conso_kwh_m2_an: final?.conso_kwh_m2_an ?? null,
        ges: final?.ges || "NC",
        confiance_score: final?.confiance?.score ?? null,
        confiance_label: final?.confiance?.label ?? null,
        has_collectif_reel: Boolean(dpe.dpeCollectifReel),
        has_simulation: Boolean(dpe.dpeImmeubleSimule),
        collectif_date: dpe.dpeCollectifReel?.date || null,
        numero_dpe:
          final?.numero_dpe ||
          dpe.dpeCollectifReel?.numero_dpe ||
          null,
        rayon_m: dpe.usedR ?? null,
        dpe_total: dpe.list?.length ?? 0,
      };
    });

    return { items: items.filter(Boolean) };
  });

fastify.get("/copros/:id/dpe/export_all.csv", async (req, reply) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return reply.code(400).send("id invalide");

  const { rows } = await fastify.db.query(
    `SELECT id, lat, lon, nom_copro, adresse, code_postal, commune
     FROM copros WHERE id=$1`,
    [id]
  );
  const copro = rows[0];
  if (!copro) return reply.code(404).send("copro introuvable");

  // récupère la liste ADEME dédoublonnée côté service
  let dpe;
  try {
    dpe = await getDpeForLatLon({
      lat: Number(copro.lat),
      lon: Number(copro.lon),
      minResults: 8,
      n: 5,
    });
  } catch {
    dpe = { usedR: null, list: [] };
  }

  const list = Array.isArray(dpe.list) ? dpe.list : [];

  // CSV helpers
  const esc = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[;"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const line = (arr) => arr.map(esc).join(";") + "\n";
  const upper = (v) => String(v ?? "").trim().toUpperCase();

  const guessType = (d) => {
    // heuristique robuste
    if (d?.numero_dpe_immeuble || d?.numero_dpe_immeuble_associe) return "collectif";
    if (upper(d?.type_dpe).includes("IMMEUBLE")) return "collectif";
    return "individuel";
  };

  const pickNumero = (d) =>
    d?.numero_dpe ||
    d?.numero_dpe_immeuble ||
    d?.numero_dpe_logement ||
    d?.numero_dpe_immeuble_associe ||
    "";

  const pickDate = (d) =>
    d?.date_derniere_modification_dpe ||
    d?.date_etablissement_dpe ||
    d?.date_reception_dpe ||
    "";

  reply
    .header("Content-Type", "text/csv; charset=utf-8")
    .header(
      "Content-Disposition",
      `attachment; filename="copro_${id}_dpes_all.csv"`
    );

  let out = "\uFEFF"; // BOM excel
  out += line([
    "copro_id",
    "copro_nom",
    "copro_adresse",
    "lat",
    "lon",
    "rayon_utilise_m",

    "numero_dpe",
    "type_dpe_guess",
    "type_dpe_raw",
    "classe_dpe",
    "classe_ges",
    "conso_5_usages_par_m2_ep",
    "date_principale",

    // quelques champs utiles (si présents)
    "annee_construction",
    "surface_habitable",
    "energie_chauffage",
    "generateur_chauffage",
    "energie_ecs",
    "generateur_ecs",
    "type_ventilation",

    // debug (optionnel) : clés disponibles sur la ligne ADEME
    "raw_keys",
  ]);

  for (const d of list) {
    const numero = pickNumero(d);
    if (!numero) continue;

    out += line([
      copro.id,
      copro.nom_copro || "",
      [copro.adresse, copro.code_postal, copro.commune].filter(Boolean).join(" "),
      copro.lat,
      copro.lon,
      dpe.usedR ?? "",

      numero,
      guessType(d),
      d?.type_dpe || d?.type_dpe_batiment || "",
      upper(d?.etiquette_dpe),
      upper(d?.etiquette_ges),
      d?.conso_5_usages_par_m2_ep ?? d?.conso_5_usages_par_m2 ?? "",
      pickDate(d),

      d?.annee_construction ?? d?.annee_construction_batiment ?? "",
      d?.surface_habitable ?? d?.surface_habitable_logement ?? "",
      d?.energie_chauffage ?? d?.energie_chauffage_principale ?? "",
      d?.generateur_chauffage ?? d?.generateur_chauffage_principal ?? "",
      d?.energie_ecs ?? d?.energie_ef_chauffe_eau ?? "",
      d?.generateur_ecs ?? d?.generateur_chauffe_eau ?? "",
      d?.type_ventilation ?? d?.ventilation ?? "",

      Object.keys(d || {}).join("|"),
    ]);
  }

  return out;
});

  // DPE pour une copro (id) : version ultra robuste (zéro 500)
  fastify.get("/copros/:id/dpe", async (req, reply) => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id)) return fastify.httpErrors.badRequest("id invalide");

      const { rows } = await fastify.db.query(
        `SELECT id, lat, lon, nom_copro, adresse, code_postal, commune, syndic, departement
         FROM copros WHERE id=$1`,
        [id]
      );

      const copro = rows[0];
      if (!copro) return fastify.httpErrors.notFound("copro introuvable");

      let dpe = makeFallbackDpe();
      try {
        dpe = await getDpeForLatLon({
          lat: copro.lat,
          lon: copro.lon,
          minResults: 8,
          n: 5,
        });
      } catch (err) {
        req.log.error({ err, coproId: id }, "getDpeForLatLon failed");
        dpe = makeFallbackDpe();
      }

      return {
        copro,
        stats: dpe.stats,
        usedR: dpe.usedR,
        list: dpe.list,
        dpeCollectifReel: dpe.dpeCollectifReel,
        dpeImmeubleSimule: dpe.dpeImmeubleSimule,
        dpeImmeubleFinal: dpe.dpeImmeubleFinal,
        goRenov: buildGoRenov({
  copro,
  stats: { ...dpe.stats, ...(dpe.meta || {}) },
  list: dpe.list,
  dpeCollectifReel: dpe.dpeCollectifReel,
  dpeImmeubleSimule: dpe.dpeImmeubleSimule,
  dpeImmeubleFinal: dpe.dpeImmeubleFinal,
}),

      };
    } catch (err) {
      // Ici, on garantit qu'on ne renvoie JAMAIS 500
      req.log.error({ err }, "/copros/:id/dpe fatal");
      reply.code(200);
      const dpe = makeFallbackDpe();
      return {
        copro: null,
        stats: dpe.stats,
        usedR: dpe.usedR,
        list: dpe.list,
        dpeCollectifReel: dpe.dpeCollectifReel,
        dpeImmeubleSimule: dpe.dpeImmeubleSimule,
        dpeImmeubleFinal: dpe.dpeImmeubleFinal,
        goRenov: buildGoRenov({
          copro: null,
          stats: dpe.stats,
          list: dpe.list,
          dpeCollectifReel: dpe.dpeCollectifReel,
          dpeImmeubleSimule: dpe.dpeImmeubleSimule,
          dpeImmeubleFinal: dpe.dpeImmeubleFinal,
        }),
        error: "internal_error",
      };
    }
  });
});

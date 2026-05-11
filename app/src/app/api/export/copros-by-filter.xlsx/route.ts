import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import ExcelJS from "exceljs";
import {
  applyHeaderStyle,
  styleDpeCell,
  styleQualityCell,
  freezeAndZebra,
} from "@/lib/services/excel";

export const runtime = "nodejs";
export const maxDuration = 60;

const MAX_ROWS = 20_000;

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

const PERIOD_LABELS: Record<string, string> = {
  AVANT_1949: "Avant 1949",
  DE_1949_A_1974: "1949 - 1974",
  DE_1975_A_1993: "1975 - 1993",
  DE_1994_A_2000: "1994 - 2000",
  DE_2001_A_2010: "2001 - 2010",
  APRES_2011: "Après 2011",
  NON_CONNUE: "Non connue",
};

const STAGE_LABELS: Record<string, string> = {
  lead: "Lead",
  to_contact: "À contacter",
  contacted: "Contacté",
  meeting: "RDV",
  proposal: "Proposition",
  won: "Signé",
  lost: "Perdu",
};

/**
 * Exporte directement le résultat d'un filtre (mêmes params que /api/copros/list)
 * en XLSX premium. Permet d'exporter "tout ce qui matche" sans devoir cocher.
 */
export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;

  // Re-build the same WHERE clause that /api/copros/list uses
  const q = sp.get("q")?.trim();
  const cp = sp.get("cp")?.trim();
  const commune = sp.get("commune")?.trim();
  const syndic = sp.get("syndic")?.trim();
  const dept = sp.get("dept")?.trim();
  const dpeClass = sp.get("dpe")?.trim().toUpperCase();
  const minLots = num(sp.get("minLots"));
  const maxLots = num(sp.get("maxLots"));
  const periode = sp.get("periode")?.trim();
  const consoMin = num(sp.get("consoMin"));
  const consoMax = num(sp.get("consoMax"));
  const hasCollectif = sp.get("hasCollectif");
  const dpeComputed = sp.get("dpeComputed");
  const quality = sp.get("quality");
  const inPipeline = sp.get("inPipeline");

  const where: string[] = [];
  const params: InValue[] = [];

  if (cp) {
    where.push("c.code_postal LIKE ?");
    params.push(`${cp}%`);
  }
  if (commune) {
    where.push("LOWER(c.commune) LIKE ?");
    params.push(`%${commune.toLowerCase()}%`);
  }
  if (syndic) {
    where.push("LOWER(c.syndic) LIKE ?");
    params.push(`%${syndic.toLowerCase()}%`);
  }
  if (dept) {
    where.push("c.departement = ?");
    params.push(dept);
  }
  if (minLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) >= ?");
    params.push(minLots);
  }
  if (maxLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) <= ?");
    params.push(maxLots);
  }
  if (periode) {
    where.push("c.periode_construction = ?");
    params.push(periode);
  }
  if (consoMin != null) {
    where.push("e.conso_moyenne >= ?");
    params.push(consoMin);
  }
  if (consoMax != null) {
    where.push("e.conso_moyenne <= ?");
    params.push(consoMax);
  }
  if (hasCollectif === "1") where.push("e.classe_reelle IS NOT NULL");
  else if (hasCollectif === "0") where.push("e.classe_reelle IS NULL");
  if (dpeComputed === "1") where.push("e.classe_finale IS NOT NULL");
  else if (dpeComputed === "0") where.push("e.classe_finale IS NULL");
  if (inPipeline === "1") {
    where.push("EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)");
  } else if (inPipeline === "0") {
    where.push("NOT EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)");
  }
  if (dpeClass) {
    const classes = dpeClass
      .split(",")
      .map((c) => c.trim().toUpperCase())
      .filter((c) => /^[A-G]$/.test(c) || c === "NC");
    if (classes.length) {
      const placeholders = classes.map(() => "?").join(",");
      if (classes.includes("NC")) {
        where.push(
          `(COALESCE(e.classe_finale, 'NC') IN (${placeholders}) OR e.classe_finale IS NULL)`,
        );
      } else {
        where.push(`e.classe_finale IN (${placeholders})`);
      }
      params.push(...classes);
    }
  }
  if (quality) {
    const allowed = ["verified", "approximate", "uncertain", "no_data"];
    const wanted = quality.split(",").filter((q) => allowed.includes(q));
    if (wanted.length) {
      const placeholders = wanted.map(() => "?").join(",");
      where.push(`e.quality_level IN (${placeholders})`);
      params.push(...wanted);
    }
  }

  // FTS5
  let useFts = false;
  let ftsQuery = "";
  if (q) {
    const tokens = q
      .replace(/["()*]/g, " ")
      .trim()
      .split(/\s+/)
      .filter((t) => t.length >= 2);
    if (tokens.length > 0) {
      useFts = true;
      ftsQuery = tokens.map((t) => `"${t}"*`).join(" AND ");
    }
  }

  const fromSql = useFts
    ? `FROM copros_fts JOIN copros c ON c.id = copros_fts.rowid LEFT JOIN dpe_estimates e ON e.copro_id = c.id`
    : `FROM copros c LEFT JOIN dpe_estimates e ON e.copro_id = c.id`;
  const ftsWhere = useFts ? `copros_fts MATCH ?` : "";
  const fullWhere = [ftsWhere, ...where].filter(Boolean).join(" AND ");
  const fullWhereSql = fullWhere ? `WHERE ${fullWhere}` : "";
  const ftsParams: InValue[] = useFts ? [ftsQuery] : [];

  // Cap to MAX_ROWS
  const sql = `
    SELECT c.*, e.classe_finale, e.classe_reelle, e.classe_simulee,
           e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche, e.quality_level,
           (SELECT p.id FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_id,
           (SELECT p.stage FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_stage,
           (SELECT p.estimated_value FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_value
    ${fromSql}
    ${fullWhereSql}
    ORDER BY c.commune, c.code_postal, c.adresse
    LIMIT ?
  `;
  const rows = await db.all<Record<string, unknown>>(sql, [...ftsParams, ...params, MAX_ROWS]);

  const wb = new ExcelJS.Workbook();
  wb.creator = "Citeen CRM";
  wb.created = new Date();
  wb.title = `Export filtre — ${rows.length} copros`;

  // Sheet 1: synthèse filtre
  const wsInfo = wb.addWorksheet("Filtre appliqué");
  wsInfo.columns = [
    { header: "Paramètre", key: "k", width: 30 },
    { header: "Valeur", key: "v", width: 40 },
  ];
  applyHeaderStyle(wsInfo.getRow(1));
  const addInfo = (k: string, v: unknown) => {
    if (v == null || v === "") return;
    wsInfo.addRow({ k, v });
  };
  addInfo("Date export", new Date().toLocaleString("fr-FR"));
  addInfo("Recherche (q)", q);
  addInfo("Département", dept);
  addInfo("Code postal", cp);
  addInfo("Commune", commune);
  addInfo("Syndic", syndic);
  addInfo("Classe DPE", dpeClass);
  addInfo("Période construction", periode);
  addInfo("Lots min", minLots);
  addInfo("Lots max", maxLots);
  addInfo("Conso min", consoMin);
  addInfo("Conso max", consoMax);
  addInfo(
    "DPE collectif réel",
    hasCollectif === "1" ? "Oui" : hasCollectif === "0" ? "Non" : null,
  );
  addInfo(
    "DPE estimé",
    dpeComputed === "1" ? "Oui" : dpeComputed === "0" ? "Non" : null,
  );
  addInfo("Qualité matching", quality);
  addInfo(
    "Pipeline",
    inPipeline === "1" ? "Déjà dans pipeline" : inPipeline === "0" ? "Pas dans pipeline" : null,
  );
  addInfo("", "");
  addInfo("Nb copros exportées", rows.length);
  addInfo("Limite", `${MAX_ROWS} max`);

  // Sheet 2: copros
  const ws = wb.addWorksheet("Copropriétés");
  ws.columns = [
    { header: "N° immatriculation", key: "immat", width: 18 },
    { header: "Nom copropriété", key: "nom", width: 35 },
    { header: "Adresse", key: "adresse", width: 35 },
    { header: "CP", key: "cp", width: 8 },
    { header: "Commune", key: "commune", width: 22 },
    { header: "Dpt", key: "dept", width: 5 },
    { header: "Lots habitation", key: "lots_hab", width: 9 },
    { header: "Lots total", key: "lots", width: 9 },
    { header: "Période construction", key: "periode", width: 18 },
    { header: "Syndic", key: "syndic", width: 30 },
    { header: "DPE final", key: "dpe", width: 9 },
    { header: "DPE réel ?", key: "dpe_reel", width: 9 },
    { header: "DPE simulé", key: "dpe_sim", width: 9 },
    { header: "Conso (kWhep/m²/an)", key: "conso", width: 14 },
    { header: "Nb DPE matchés", key: "nb_dpe", width: 10 },
    { header: "Qualité matching", key: "quality", width: 16 },
    { header: "Étape pipeline", key: "stage", width: 16 },
    { header: "Valeur estimée €", key: "value", width: 14 },
    { header: "Latitude", key: "lat", width: 11 },
    { header: "Longitude", key: "lon", width: 11 },
    { header: "Cadastre", key: "cadastre", width: 18 },
  ];
  applyHeaderStyle(ws.getRow(1));

  for (const r of rows) {
    const row = ws.addRow({
      immat: r.numero_immatriculation,
      nom: r.nom_copro,
      adresse: r.adresse,
      cp: r.code_postal,
      commune: r.commune,
      dept: r.departement,
      lots_hab: r.nb_lots_habitation,
      lots: r.nb_lots,
      periode:
        PERIOD_LABELS[String(r.periode_construction)] ?? r.periode_construction,
      syndic: r.syndic,
      dpe: null,
      dpe_reel: r.classe_reelle ?? "—",
      dpe_sim: r.classe_simulee ?? "—",
      conso: r.conso_moyenne,
      nb_dpe: r.nb_dpe_individuels,
      quality: null,
      stage: r.prospect_stage ? STAGE_LABELS[String(r.prospect_stage)] : "—",
      value: r.prospect_value,
      lat: r.lat,
      lon: r.lon,
      cadastre: r.reference_cadastrale,
    });
    styleDpeCell(row.getCell("dpe"), r.classe_finale as string);
    styleQualityCell(row.getCell("quality"), r.quality_level as string);
  }
  freezeAndZebra(ws);

  const buf = await wb.xlsx.writeBuffer();
  return new NextResponse(buf as ArrayBuffer, {
    headers: {
      "content-type":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "content-disposition": `attachment; filename="copros-filtre-${rows.length}-${new Date().toISOString().slice(0, 10)}.xlsx"`,
    },
  });
}

import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import ExcelJS from "exceljs";
import {
  applyHeaderStyle,
  styleDpeCell,
  styleQualityCell,
  freezeAndZebra,
  autoSizeColumns,
} from "@/lib/services/excel";

export const runtime = "nodejs";

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

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const ids = sp
    .get("ids")
    ?.split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n));
  if (!ids || ids.length === 0) {
    return NextResponse.json({ error: "ids requis" }, { status: 400 });
  }
  if (ids.length > 10000) {
    return NextResponse.json({ error: "Max 10 000 ids par export" }, { status: 400 });
  }

  const placeholders = ids.map(() => "?").join(",");
  const rows = sqlite
    .prepare(
      `SELECT c.*, e.classe_finale, e.classe_reelle, e.classe_simulee,
              e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche,
              e.quality_level,
              (SELECT p.id FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_id,
              (SELECT p.stage FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_stage,
              (SELECT p.estimated_value FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_value
       FROM copros c
       LEFT JOIN dpe_estimates e ON e.copro_id = c.id
       WHERE c.id IN (${placeholders})
       ORDER BY c.commune, c.code_postal, c.adresse`,
    )
    .all(...ids) as Record<string, unknown>[];

  const wb = new ExcelJS.Workbook();
  wb.creator = "Citeen CRM";
  wb.created = new Date();
  wb.title = `Export copros — ${rows.length} immeubles`;

  const ws = wb.addWorksheet("Copropriétés", {
    properties: { defaultRowHeight: 18 },
  });

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
      periode: PERIOD_LABELS[String(r.periode_construction)] ?? r.periode_construction,
      syndic: r.syndic,
      dpe: null, // styled below
      dpe_reel: r.classe_reelle ?? "—",
      dpe_sim: r.classe_simulee ?? "—",
      conso: r.conso_moyenne,
      nb_dpe: r.nb_dpe_individuels,
      quality: null, // styled below
      stage: r.prospect_stage ? STAGE_LABELS[String(r.prospect_stage)] : "—",
      value: r.prospect_value,
      lat: r.lat,
      lon: r.lon,
      cadastre: r.reference_cadastrale,
    });
    styleDpeCell(row.getCell("dpe"), r.classe_finale as string);
    styleQualityCell(row.getCell("quality"), r.quality_level as string);
    if (r.classe_reelle) {
      const cell = row.getCell("dpe_reel");
      cell.font = { bold: true, color: { argb: "FF14532D" } };
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFDCFCE7" },
      };
    }
  }

  freezeAndZebra(ws);

  const buf = await wb.xlsx.writeBuffer();
  return new NextResponse(buf as ArrayBuffer, {
    headers: {
      "content-type":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "content-disposition": `attachment; filename="copros-${rows.length}-${new Date().toISOString().slice(0, 10)}.xlsx"`,
    },
  });
}

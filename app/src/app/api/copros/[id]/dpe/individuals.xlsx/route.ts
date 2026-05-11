import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { estimateDpeForCopro } from "@/lib/services/dpe";
import ExcelJS from "exceljs";
import {
  applyHeaderStyle,
  styleDpeCell,
  freezeAndZebra,
} from "@/lib/services/excel";

export const runtime = "nodejs";

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId))
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });

  const copro = sqlite
    .prepare(
      `SELECT id, lat, lon, adresse, code_postal, commune,
              numero_immatriculation, nom_copro, syndic, nb_lots_habitation,
              code_insee_commune, section, numero_parcelle
       FROM copros WHERE id = ?`,
    )
    .get(coproId) as Record<string, unknown> | undefined;

  if (!copro) return NextResponse.json({ error: "Not found" }, { status: 404 });

  const est = await estimateDpeForCopro({
    lat: copro.lat as number,
    lon: copro.lon as number,
    address: copro.adresse as string,
    codePostal: copro.code_postal as string,
    commune: copro.commune as string,
    numeroImmatriculation: copro.numero_immatriculation as string,
    codeInseeCommune: copro.code_insee_commune as string,
    section: copro.section as string,
    numeroParcelle: copro.numero_parcelle as string,
    includeMatched: true,
  });

  const records = est.matchedRecords ?? [];

  const wb = new ExcelJS.Workbook();
  wb.creator = "Citeen CRM";
  wb.created = new Date();
  wb.title = `DPE ${copro.nom_copro || copro.numero_immatriculation}`;

  // === FEUILLE 1 : Synthèse immeuble ===
  const wsSummary = wb.addWorksheet("Synthèse immeuble");
  wsSummary.columns = [
    { header: "Champ", key: "k", width: 32 },
    { header: "Valeur", key: "v", width: 50 },
  ];
  applyHeaderStyle(wsSummary.getRow(1));

  const add = (k: string, v: unknown, special?: "dpe") => {
    const r = wsSummary.addRow({ k, v });
    if (special === "dpe") {
      styleDpeCell(r.getCell("v"), String(v ?? ""));
    }
  };

  add("Nom copropriété", copro.nom_copro);
  add("Numéro d'immatriculation", copro.numero_immatriculation);
  add("Syndic", copro.syndic);
  add("Adresse", `${copro.adresse} · ${copro.code_postal} ${copro.commune}`);
  add("Lots habitation", copro.nb_lots_habitation);
  add("", "");
  add("DPE FINAL IMMEUBLE", est.immeubleFinal.classe, "dpe");
  add("Statut", est.immeubleFinal.statut);
  add("Consommation (kWhep/m²/an)", est.immeubleFinal.conso);
  add("Confiance", `${est.immeubleFinal.confiance.score}% — ${est.immeubleFinal.confiance.label}`);
  add("", "");
  add("Qualité prospection", est.quality.label);
  add("Raison qualité", est.quality.reason);
  add("Parcelle cadastrale IGN", est.parcelle?.idu);
  add(
    "Surface parcelle (m²)",
    est.parcelle?.surfaceM2 ? est.parcelle.surfaceM2 : null,
  );
  add("", "");
  add("DPE candidats trouvés (rayon)", est.totalDpeFound);
  add("DPE retenus pour l'immeuble", est.totalDpeMatched);
  add("Rayon de recherche (m)", est.rayonM);
  if (est.banResolved) {
    add("Adresse canonique BAN", est.banResolved.label);
    add(
      "Correction BAN appliquée",
      est.banResolved.corrected ? "Oui" : "Non (déjà canonique)",
    );
  }

  // Bold the "section header" rows (empty key separators)
  wsSummary.eachRow((r, i) => {
    if (i === 1) return;
    const k = r.getCell("k").value as string;
    if (k && k === k.toUpperCase() && k.length > 3) {
      r.getCell("k").font = { bold: true, size: 11 };
      r.getCell("k").fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFEFF6FF" },
      };
    }
  });

  // === FEUILLE 2 : DPE individuels retenus ===
  const ws = wb.addWorksheet("DPE individuels retenus");
  ws.columns = [
    { header: "Classe DPE", key: "classe", width: 11 },
    { header: "GES", key: "ges", width: 8 },
    { header: "Conso (kWhep/m²/an)", key: "conso", width: 14 },
    { header: "Surface (m²)", key: "surface", width: 11 },
    { header: "N° de voie", key: "numero", width: 9 },
    { header: "Rue", key: "rue", width: 32 },
    { header: "CP", key: "cp", width: 8 },
    { header: "Commune", key: "commune", width: 22 },
    { header: "Type DPE", key: "type", width: 14 },
    { header: "Type bâtiment", key: "bat", width: 14 },
    { header: "Année construction", key: "annee", width: 12 },
    { header: "Date DPE", key: "date", width: 13 },
    { header: "Lien ADEME officiel", key: "lien", width: 25 },
    { header: "N° DPE", key: "numero_dpe", width: 22 },
  ];
  applyHeaderStyle(ws.getRow(1));

  for (const r of records) {
    const row = ws.addRow({
      classe: null,
      ges: null,
      conso: r.conso,
      surface: r.surface,
      numero: r.numero_voie,
      rue: r.rue,
      cp: r.code_postal,
      commune: r.commune,
      type: r.isCollectif ? "Collectif" : "Individuel",
      bat: r.type_batiment,
      annee: r.annee_construction,
      date: r.date ? new Date(r.date) : null,
      lien: r.numero_dpe ? "Voir sur ADEME" : null,
      numero_dpe: r.numero_dpe,
    });
    styleDpeCell(row.getCell("classe"), r.classe);
    styleDpeCell(row.getCell("ges"), r.ges);
    if (r.date) {
      row.getCell("date").numFmt = "dd/mm/yyyy";
    }
    if (r.isCollectif) {
      const c = row.getCell("type");
      c.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "FFDCFCE7" },
      };
      c.font = { bold: true, color: { argb: "FF14532D" } };
    }
    // Hyperlink to ADEME official viewer
    if (r.numero_dpe) {
      const lienCell = row.getCell("lien");
      lienCell.value = {
        text: "Voir sur ADEME",
        hyperlink: `https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${r.numero_dpe}`,
      };
      lienCell.font = {
        color: { argb: "FF2563EB" },
        underline: true,
      };
    }
  }

  freezeAndZebra(ws);

  const buf = await wb.xlsx.writeBuffer();
  return new NextResponse(buf as ArrayBuffer, {
    headers: {
      "content-type":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "content-disposition": `attachment; filename="dpe-individuels-${copro.numero_immatriculation ?? coproId}.xlsx"`,
    },
  });
}

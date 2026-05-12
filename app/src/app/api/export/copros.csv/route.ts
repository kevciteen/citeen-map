import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

const HEADER = [
  "id",
  "numero_immatriculation",
  "nom_copro",
  "adresse",
  "code_postal",
  "commune",
  "departement",
  "syndic",
  "nb_lots",
  "nb_lots_habitation",
  "periode_construction",
  "lat",
  "lon",
  "reference_cadastrale",
  "dpe_classe_finale",
  "dpe_classe_reelle",
  "dpe_classe_simulee",
  "dpe_conso_moyenne",
  "dpe_nb_individuels",
  "dpe_rayon_recherche",
  "prospect_id",
  "prospect_stage",
];

function esc(v: unknown): string {
  if (v == null) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes(";")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const sp = req.nextUrl.searchParams;
  const ids = sp.get("ids")?.split(",").map((s) => Number(s.trim())).filter((n) => Number.isFinite(n));
  if (!ids || ids.length === 0) {
    return NextResponse.json({ error: "ids requis" }, { status: 400 });
  }
  if (ids.length > 5000) {
    return NextResponse.json({ error: "Max 5000 ids par export" }, { status: 400 });
  }

  const placeholders = ids.map(() => "?").join(",");
  const rows = await db.all<Record<string, unknown>>(
    `SELECT c.*, e.classe_finale, e.classe_reelle, e.classe_simulee,
            e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche,
            (SELECT p.id FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_id,
            (SELECT p.stage FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_stage
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.id IN (${placeholders})
     ORDER BY c.commune, c.code_postal, c.adresse`,
    ids,
  );

  const lines = [HEADER.join(",")];
  for (const r of rows) {
    lines.push(
      [
        r.id,
        r.numero_immatriculation,
        r.nom_copro,
        r.adresse,
        r.code_postal,
        r.commune,
        r.departement,
        r.syndic,
        r.nb_lots,
        r.nb_lots_habitation,
        r.periode_construction,
        r.lat,
        r.lon,
        r.reference_cadastrale,
        r.classe_finale,
        r.classe_reelle,
        r.classe_simulee,
        r.conso_moyenne,
        r.nb_dpe_individuels,
        r.rayon_recherche,
        r.prospect_id,
        r.prospect_stage,
      ]
        .map(esc)
        .join(","),
    );
  }

  const body = "﻿" + lines.join("\n") + "\n";
  return new NextResponse(body, {
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="copros-${rows.length}-${new Date()
        .toISOString()
        .slice(0, 10)}.csv"`,
    },
  });
}

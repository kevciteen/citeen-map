import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";

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
  "section",
  "numero_parcelle",
  "code_insee_commune",
  "dpe_classe_finale",
  "dpe_classe_reelle",
  "dpe_classe_simulee",
  "dpe_conso_moyenne",
  "dpe_nb_individuels",
  "dpe_rayon_recherche",
  "dpe_computed_at",
];

function esc(v: unknown): string {
  if (v == null) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes(";")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const row = sqlite
    .prepare(
      `SELECT c.*, e.classe_finale, e.classe_reelle, e.classe_simulee,
              e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche, e.computed_at
       FROM copros c
       LEFT JOIN dpe_estimates e ON e.copro_id = c.id
       WHERE c.id = ?`,
    )
    .get(coproId) as Record<string, unknown> | undefined;

  if (!row) return NextResponse.json({ error: "Not found" }, { status: 404 });

  const line = [
    row.id,
    row.numero_immatriculation,
    row.nom_copro,
    row.adresse,
    row.code_postal,
    row.commune,
    row.departement,
    row.syndic,
    row.nb_lots,
    row.nb_lots_habitation,
    row.periode_construction,
    row.lat,
    row.lon,
    row.reference_cadastrale,
    row.section,
    row.numero_parcelle,
    row.code_insee_commune,
    row.classe_finale,
    row.classe_reelle,
    row.classe_simulee,
    row.conso_moyenne,
    row.nb_dpe_individuels,
    row.rayon_recherche,
    row.computed_at,
  ]
    .map(esc)
    .join(",");

  const body = "﻿" + HEADER.join(",") + "\n" + line + "\n";
  return new NextResponse(body, {
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="copro-${row.numero_immatriculation ?? coproId}.csv"`,
    },
  });
}

import { NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";

export const runtime = "nodejs";

const HEADER = [
  "id",
  "stage",
  "priority",
  "estimated_value",
  "next_action_at",
  "next_action_label",
  "created_at",
  "updated_at",
  "copro_numero_immatriculation",
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
  "dpe_classe_finale",
  "dpe_classe_reelle",
  "dpe_classe_simulee",
  "dpe_conso_moyenne",
  "dpe_nb_individuels",
];

function esc(v: unknown): string {
  if (v == null) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes(";")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export async function GET() {
  const rows = sqlite
    .prepare(
      `SELECT p.id, p.stage, p.priority, p.estimated_value,
              p.next_action_at, p.next_action_label, p.created_at, p.updated_at,
              c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal, c.commune,
              c.departement, c.syndic, c.nb_lots, c.nb_lots_habitation, c.periode_construction,
              COALESCE(c.lat, p.custom_lat) as lat,
              COALESCE(c.lon, p.custom_lon) as lon,
              e.classe_finale, e.classe_reelle, e.classe_simulee, e.conso_moyenne, e.nb_dpe_individuels
       FROM prospects p
       LEFT JOIN copros c ON c.id = p.copro_id
       LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
       ORDER BY p.updated_at DESC`,
    )
    .all() as Record<string, unknown>[];

  const lines = [HEADER.join(",")];
  for (const r of rows) {
    lines.push(
      [
        r.id,
        r.stage,
        r.priority,
        r.estimated_value,
        r.next_action_at,
        r.next_action_label,
        r.created_at,
        r.updated_at,
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
        r.classe_finale,
        r.classe_reelle,
        r.classe_simulee,
        r.conso_moyenne,
        r.nb_dpe_individuels,
      ]
        .map(esc)
        .join(","),
    );
  }

  const body = "﻿" + lines.join("\n");
  return new NextResponse(body, {
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="prospects-${new Date().toISOString().slice(0, 10)}.csv"`,
    },
  });
}

import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { estimateDpeForCopro } from "@/lib/services/dpe";

export const runtime = "nodejs";

const HEADER = [
  "numero_dpe",
  "is_collectif",
  "type_dpe",
  "classe",
  "ges",
  "conso_kwhep_m2_an",
  "surface_m2",
  "numero_voie",
  "rue",
  "code_postal",
  "commune",
  "annee_construction",
  "type_batiment",
  "date_dpe",
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

  const copro = sqlite
    .prepare(
      "SELECT id, lat, lon, adresse, code_postal, commune, numero_immatriculation FROM copros WHERE id = ?",
    )
    .get(coproId) as
    | {
        id: number;
        lat: number;
        lon: number;
        adresse: string | null;
        code_postal: string | null;
        commune: string | null;
        numero_immatriculation: string | null;
      }
    | undefined;
  if (!copro) return NextResponse.json({ error: "Not found" }, { status: 404 });

  const est = await estimateDpeForCopro({
    lat: copro.lat,
    lon: copro.lon,
    address: copro.adresse,
    codePostal: copro.code_postal,
    commune: copro.commune,
    numeroImmatriculation: copro.numero_immatriculation,
    includeMatched: true,
  });

  const records = est.matchedRecords ?? [];
  const lines = [HEADER.join(",")];
  for (const r of records) {
    lines.push(
      [
        r.numero_dpe,
        r.isCollectif ? "1" : "0",
        r.type_dpe,
        r.classe,
        r.ges,
        r.conso,
        r.surface,
        r.numero_voie,
        r.rue,
        r.code_postal,
        r.commune,
        r.annee_construction,
        r.type_batiment,
        r.date,
      ]
        .map(esc)
        .join(","),
    );
  }

  const body = "﻿" + lines.join("\n") + "\n";
  return new NextResponse(body, {
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="dpe-individuels-${copro.numero_immatriculation ?? coproId}.csv"`,
    },
  });
}

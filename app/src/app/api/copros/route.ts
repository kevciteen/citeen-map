import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";

export const runtime = "nodejs";

type CoproRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  periode_construction: string | null;
  lat: number | null;
  lon: number | null;
  classe_finale: string | null;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const minLat = num(sp.get("minLat"));
  const maxLat = num(sp.get("maxLat"));
  const minLon = num(sp.get("minLon"));
  const maxLon = num(sp.get("maxLon"));
  const limit = Math.min(Math.max(num(sp.get("limit")) ?? 800, 1), 5000);
  const search = sp.get("q")?.trim();
  const cp = sp.get("cp")?.trim();
  const commune = sp.get("commune")?.trim();
  const syndic = sp.get("syndic")?.trim();
  const dept = sp.get("dept")?.trim();
  const dpeClass = sp.get("dpe")?.trim().toUpperCase(); // comma-separated A,B,C...
  const minLots = num(sp.get("minLots"));
  const periode = sp.get("periode")?.trim();

  const where: string[] = ["c.lat IS NOT NULL", "c.lon IS NOT NULL"];
  const params: InValue[] = [];

  if (minLat != null && maxLat != null && minLon != null && maxLon != null) {
    where.push("c.lat BETWEEN ? AND ?");
    params.push(minLat, maxLat);
    where.push("c.lon BETWEEN ? AND ?");
    params.push(minLon, maxLon);
  }
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
  if (periode) {
    where.push("c.periode_construction = ?");
    params.push(periode);
  }
  if (search) {
    where.push(
      "(LOWER(c.nom_copro) LIKE ? OR LOWER(c.adresse) LIKE ? OR c.numero_immatriculation LIKE ? OR LOWER(c.commune) LIKE ?)",
    );
    const s = `%${search.toLowerCase()}%`;
    params.push(s, s, `%${search.toUpperCase()}%`, s);
  }
  if (dpeClass) {
    const classes = dpeClass
      .split(",")
      .map((c) => c.trim().toUpperCase())
      .filter((c) => /^[A-G]$/.test(c) || c === "NC");
    if (classes.length) {
      const placeholders = classes.map(() => "?").join(",");
      where.push(`COALESCE(e.classe_finale, 'NC') IN (${placeholders})`);
      params.push(...classes);
    }
  }

  const sql = `
    SELECT c.id, c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal,
           c.commune, c.departement, c.syndic,
           c.nb_lots, c.nb_lots_habitation, c.periode_construction,
           c.lat, c.lon,
           e.classe_finale
    FROM copros c
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE ${where.join(" AND ")}
    ORDER BY c.id
    LIMIT ?
  `;
  params.push(limit);

  const rows = await db.all<CoproRow>(sql, params);

  return NextResponse.json({
    count: rows.length,
    limit,
    items: rows,
  });
}

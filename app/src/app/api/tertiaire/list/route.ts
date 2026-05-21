/**
 * GET /api/tertiaire/list — liste des bâtiments tertiaires.
 *
 * Filtres (querystring) :
 *   - bbox : "minLon,minLat,maxLon,maxLat" (carte)
 *   - secteur : Bureaux | Commerces | "Hotellerie / Restauration" | Sante | Enseignement | "Autres secteurs"
 *   - dpe : A..G ou ABC ou DEF ou FG (multi-classes)
 *   - dept : code département (75, 77, 78, 91, 92, 93, 94, 95)
 *   - cp : code postal
 *   - limit : max résultats (défaut 1000, max 5000)
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import type { InValue } from "@libsql/client";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  const sp = req.nextUrl.searchParams;
  const bbox = sp.get("bbox")?.split(",").map(Number);
  const secteur = sp.get("secteur");
  const dpe = sp.get("dpe"); // ex: "FG" ou "A" ou "ABC"
  const dept = sp.get("dept");
  const cp = sp.get("cp");
  const limit = Math.min(Number(sp.get("limit") ?? "1000"), 5000);

  const where: string[] = [];
  const args: InValue[] = [];

  if (bbox && bbox.length === 4 && bbox.every(Number.isFinite)) {
    where.push(`b.lon BETWEEN ? AND ?`);
    args.push(bbox[0], bbox[2]);
    where.push(`b.lat BETWEEN ? AND ?`);
    args.push(bbox[1], bbox[3]);
  }
  if (secteur) {
    where.push(`b.secteur = ?`);
    args.push(secteur);
  }
  if (dept) {
    where.push(`b.departement = ?`);
    args.push(dept);
  }
  if (cp) {
    where.push(`b.code_postal = ?`);
    args.push(cp);
  }
  if (dpe && dpe.length > 0) {
    const classes = dpe.toUpperCase().split("").filter((c) => "ABCDEFG".includes(c));
    if (classes.length > 0) {
      const placeholders = classes.map(() => "?").join(",");
      where.push(`d.etiquette_dpe IN (${placeholders})`);
      args.push(...classes);
    }
  }

  const sql = `
    SELECT
      b.id, b.label, b.adresse, b.code_postal, b.commune, b.departement,
      b.lat, b.lon, b.secteur, b.type_usage, b.surface_m2, b.annee_construction,
      d.etiquette_dpe, d.etiquette_ges, d.conso_energie_primaire
    FROM tertiary_buildings b
    LEFT JOIN tertiary_dpe d ON d.building_id = b.id
    ${where.length ? "WHERE " + where.join(" AND ") : ""}
    ORDER BY b.id
    LIMIT ?
  `;
  args.push(limit);

  const rows = await db.all(sql, args);
  return NextResponse.json({ items: rows, count: rows.length, limit });
}

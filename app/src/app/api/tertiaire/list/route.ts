/**
 * GET /api/tertiaire/list — liste des bâtiments tertiaires.
 *
 * Filtres (querystring) :
 *   - bbox : "minLon,minLat,maxLon,maxLat" (carte)
 *   - secteur : Bureaux | Commerces | "Hotellerie / Restauration" | Sante | Enseignement | "Autres secteurs"
 *   - dpe : "A,B,C" ou "FG" (multi-classes)
 *   - dept : code département (75, 77, 78, 91, 92, 93, 94, 95)
 *   - cp : code postal
 *   - commune : nom commune (LIKE %x%)
 *   - q : recherche texte sur label/adresse (LIKE %x%)
 *   - surfaceMin / surfaceMax : surface en m²
 *   - proprietaire : recherche texte sur dénomination occupant (LIKE %x%)
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
  const dpe = sp.get("dpe");
  const dept = sp.get("dept");
  const cp = sp.get("cp");
  const commune = sp.get("commune");
  const q = sp.get("q");
  const surfaceMin = sp.get("surfaceMin");
  const surfaceMax = sp.get("surfaceMax");
  const proprietaire = sp.get("proprietaire");
  const limit = Math.min(Number(sp.get("limit") ?? "1000"), 5000);

  // Garde-fou : exclut toujours les bâtiments sans coordonnées (sinon MapLibre crash)
  const where: string[] = ["b.lat IS NOT NULL", "b.lon IS NOT NULL"];
  const args: InValue[] = [];

  if (bbox && bbox.length === 4 && bbox.every(Number.isFinite)) {
    where.push(`b.lon BETWEEN ? AND ? AND b.lat BETWEEN ? AND ?`);
    args.push(bbox[0], bbox[2], bbox[1], bbox[3]);
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
  if (commune) {
    where.push(`LOWER(b.commune) LIKE ?`);
    args.push(`%${commune.toLowerCase()}%`);
  }
  if (q) {
    where.push(`(LOWER(b.label) LIKE ? OR LOWER(b.adresse) LIKE ?)`);
    args.push(`%${q.toLowerCase()}%`, `%${q.toLowerCase()}%`);
  }
  if (surfaceMin && Number.isFinite(Number(surfaceMin))) {
    where.push(`b.surface_m2 >= ?`);
    args.push(Number(surfaceMin));
  }
  if (surfaceMax && Number.isFinite(Number(surfaceMax))) {
    where.push(`b.surface_m2 <= ?`);
    args.push(Number(surfaceMax));
  }
  if (dpe && dpe.length > 0) {
    const classes = dpe.toUpperCase().split(/[,\s]+/).filter((c) => "ABCDEFG".includes(c));
    if (classes.length > 0) {
      const placeholders = classes.map(() => "?").join(",");
      where.push(`d.etiquette_dpe IN (${placeholders})`);
      args.push(...classes);
    }
  }
  if (proprietaire) {
    where.push(`EXISTS (
      SELECT 1 FROM tertiary_occupants o
      WHERE o.building_id = b.id AND LOWER(o.denomination) LIKE ?
    )`);
    args.push(`%${proprietaire.toLowerCase()}%`);
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

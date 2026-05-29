/**
 * GET /api/directory
 *
 * Recherche cross-entité dans l'annuaire unifié :
 *   - filtres par bbox (carte), code postal, département, type d'entité
 *   - recherche full-text simple sur display_name + address
 *   - filtre "avec contact" (phone OU email OU website non NULL) pour
 *     la prospection ciblée
 *
 * Lecture seule. Pour rafraîchir : POST /api/directory/sync.
 */
import { NextRequest, NextResponse } from "next/server";
import type { InValue } from "@libsql/client";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureDirectory } from "@/lib/db/ensure-directory";

export const runtime = "nodejs";

type DirectoryRow = {
  id: number;
  entity_type: string;
  entity_ref: string;
  display_name: string;
  display_subtitle: string | null;
  address: string | null;
  postcode: string | null;
  city: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  coords_source: string | null;
  phone: string | null;
  phone_source: string | null;
  email: string | null;
  email_source: string | null;
  website: string | null;
  website_source: string | null;
  parent_copro_id: number | null;
  parent_building_id: number | null;
  synced_at: number;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDirectory();

  const sp = req.nextUrl.searchParams;
  const limit = Math.min(Math.max(num(sp.get("limit")) ?? 200, 1), 1000);
  const q = sp.get("q")?.trim();
  const cp = sp.get("cp")?.trim();
  const dept = sp.get("dept")?.trim();
  const typesParam = sp.get("types")?.trim(); // CSV : occupant,copro,syndic,prospect_custom
  const minLat = num(sp.get("minLat"));
  const maxLat = num(sp.get("maxLat"));
  const minLon = num(sp.get("minLon"));
  const maxLon = num(sp.get("maxLon"));
  const onlyWithContact = sp.get("onlyWithContact") === "1";
  const onlyWithCoords = sp.get("onlyWithCoords") === "1";

  const where: string[] = ["1=1"];
  const params: InValue[] = [];

  if (q) {
    where.push("(LOWER(display_name) LIKE ? OR LOWER(address) LIKE ?)");
    const s = `%${q.toLowerCase()}%`;
    params.push(s, s);
  }
  if (cp) {
    where.push("postcode LIKE ?");
    params.push(`${cp}%`);
  }
  if (dept) {
    where.push("departement = ?");
    params.push(dept);
  }
  if (typesParam) {
    const types = typesParam
      .split(",")
      .map((t) => t.trim())
      .filter((t) =>
        ["occupant", "copro", "syndic", "prospect_custom"].includes(t),
      );
    if (types.length) {
      const placeholders = types.map(() => "?").join(",");
      where.push(`entity_type IN (${placeholders})`);
      params.push(...types);
    }
  }
  if (minLat != null && maxLat != null && minLon != null && maxLon != null) {
    where.push("lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?");
    params.push(minLat, maxLat, minLon, maxLon);
  }
  if (onlyWithContact) {
    where.push(
      "(phone IS NOT NULL OR email IS NOT NULL OR website IS NOT NULL)",
    );
  }
  if (onlyWithCoords) {
    where.push("lat IS NOT NULL AND lon IS NOT NULL");
  }

  const rows = await db.all<DirectoryRow>(
    `SELECT id, entity_type, entity_ref, display_name, display_subtitle,
            address, postcode, city, departement,
            lat, lon, coords_source,
            phone, phone_source, email, email_source, website, website_source,
            parent_copro_id, parent_building_id, synced_at
     FROM directory
     WHERE ${where.join(" AND ")}
     ORDER BY entity_type, display_name
     LIMIT ?`,
    [...params, limit],
  );

  return NextResponse.json({ count: rows.length, limit, items: rows });
}

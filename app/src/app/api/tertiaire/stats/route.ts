/**
 * GET /api/tertiaire/stats — diagnostic minimal : combien de bâtiments
 * tertiaires sont visibles depuis CE déploiement (utile pour vérifier
 * que Vercel pointe bien vers la même DB Turso que celle peuplée en local).
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import { getTertiaireStats } from "@/lib/services/global-counts";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  // Agrégats lourds (COUNT/GROUP BY full-scan) mémoïsés 5 min.
  const stats = await getTertiaireStats();

  // Échantillon (LIMIT 5, négligeable) gardé frais pour le diagnostic live.
  const sample = await db.all<{ id: number; label: string | null; lat: number | null; lon: number | null; secteur: string | null }>(
    `SELECT id, label, lat, lon, secteur FROM tertiary_buildings LIMIT 5`,
  );

  return NextResponse.json({
    db: {
      tursoUrlSet: Boolean(process.env.TURSO_DATABASE_URL),
      tursoUrlPrefix: process.env.TURSO_DATABASE_URL?.slice(0, 20) ?? null,
      authTokenSet: Boolean(process.env.TURSO_AUTH_TOKEN),
    },
    totalBuildings: stats.totalBuildings,
    withCoords: stats.withCoords,
    parisBboxCount: stats.parisBboxCount,
    byDept: stats.byDept,
    bySource: stats.bySource,
    sample,
  });
}

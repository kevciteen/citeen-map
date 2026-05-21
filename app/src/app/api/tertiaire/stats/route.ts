/**
 * GET /api/tertiaire/stats — diagnostic minimal : combien de bâtiments
 * tertiaires sont visibles depuis CE déploiement (utile pour vérifier
 * que Vercel pointe bien vers la même DB Turso que celle peuplée en local).
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  const total = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM tertiary_buildings`,
  );
  const withCoords = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM tertiary_buildings WHERE lat IS NOT NULL AND lon IS NOT NULL`,
  );
  const byDept = await db.all<{ departement: string | null; n: number }>(
    `SELECT departement, COUNT(*) AS n FROM tertiary_buildings
     GROUP BY departement ORDER BY n DESC LIMIT 10`,
  );
  const bySource = await db.all<{ source: string | null; n: number }>(
    `SELECT source, COUNT(*) AS n FROM tertiary_buildings GROUP BY source`,
  );
  const sample = await db.all<{ id: number; label: string | null; lat: number | null; lon: number | null; secteur: string | null }>(
    `SELECT id, label, lat, lon, secteur FROM tertiary_buildings LIMIT 5`,
  );

  // Compte aussi pour la bbox Paris pour reproduire la requête carte
  const parisCount = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM tertiary_buildings
     WHERE lat BETWEEN 48.80 AND 48.95 AND lon BETWEEN 2.20 AND 2.50`,
  );

  return NextResponse.json({
    db: {
      tursoUrlSet: Boolean(process.env.TURSO_DATABASE_URL),
      tursoUrlPrefix: process.env.TURSO_DATABASE_URL?.slice(0, 20) ?? null,
      authTokenSet: Boolean(process.env.TURSO_AUTH_TOKEN),
    },
    totalBuildings: total?.n ?? 0,
    withCoords: withCoords?.n ?? 0,
    parisBboxCount: parisCount?.n ?? 0,
    byDept,
    bySource,
    sample,
  });
}

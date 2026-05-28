/**
 * GET /api/copros/coords-stats
 *
 * Diagnostique le géocodage des copros : combien ont des coords, par
 * source (registre/ban/cadastre/none), et combien il reste à backfiller.
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureCoprosCoords } from "@/lib/db/ensure-copros-coords";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureCoprosCoords();

  const total = await db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM copros`);
  const withCoords = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM copros WHERE lat IS NOT NULL AND lon IS NOT NULL`,
  );
  const bySource = await db.all<{ coords_source: string | null; n: number }>(
    `SELECT coords_source, COUNT(*) AS n FROM copros GROUP BY coords_source`,
  );
  const pendingBackfill = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM copros
     WHERE (lat IS NULL OR lon IS NULL)
       AND (coords_source IS NULL OR coords_source != 'none')`,
  );

  return NextResponse.json({
    total: total?.n ?? 0,
    withCoords: withCoords?.n ?? 0,
    withoutCoords: (total?.n ?? 0) - (withCoords?.n ?? 0),
    pendingBackfill: pendingBackfill?.n ?? 0,
    bySource: bySource.map((r) => ({ source: r.coords_source ?? "null", count: r.n })),
  });
}

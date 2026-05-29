/**
 * GET /api/directory/stats
 *
 * Compteurs pour le dashboard admin coords-health : total + répartition par
 * entity_type + dernier sync.
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureDirectory } from "@/lib/db/ensure-directory";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDirectory();

  const total = await db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM directory`);
  const byType = await db.all<{ entity_type: string; n: number }>(
    `SELECT entity_type, COUNT(*) AS n FROM directory GROUP BY entity_type ORDER BY entity_type`,
  );
  const lastSync = await db.get<{ ts: number | null }>(
    `SELECT MAX(synced_at) AS ts FROM directory`,
  );

  return NextResponse.json({
    total: total?.n ?? 0,
    byType: byType.map((r) => ({ entity_type: r.entity_type, count: r.n })),
    lastSyncedAt: lastSync?.ts ?? null,
  });
}

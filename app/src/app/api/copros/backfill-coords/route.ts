/**
 * POST /api/copros/backfill-coords  (admin only)
 *
 * Lance un batch de backfill BAN → cadastre sur les copros sans coords.
 * Body optionnel : `{ limit?: number }` (default 100, max 500).
 *
 * Idempotent : ne ré-traite pas les copros déjà marquées 'none' (sauf si
 * l'admin force un reset manuellement). Penser à appeler plusieurs fois
 * jusqu'à ce que pendingBackfill tombe à 0 (cf. /api/copros/coords-stats).
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAdmin } from "@/lib/auth/guards";
import { ensureCoprosCoords } from "@/lib/db/ensure-copros-coords";
import { backfillCoprosCoordsBatch } from "@/lib/services/copro-coords";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const maxDuration = 60;

export async function POST(req: NextRequest) {
  const guard = await ensureAdmin();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;
  await ensureCoprosCoords();

  const body = (await req.json().catch(() => ({}))) as { limit?: number };
  const rawLimit = typeof body.limit === "number" ? body.limit : 100;
  const limit = Math.min(Math.max(rawLimit, 1), 500);

  const result = await backfillCoprosCoordsBatch(limit);
  return NextResponse.json(result);
}

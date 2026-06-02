/**
 * GET /api/copros/:id/score
 *
 * Renvoie le score de priorité (0-100) + le breakdown détaillé
 * pour une copro donnée. Utilisé par le widget PrioriteScoreCard
 * sur la fiche copro.
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEntityOverrides } from "@/lib/db/ensure-entity-overrides";
import { computeScoreBreakdown } from "@/lib/services/priority-score";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEntityOverrides();

  const { id } = await ctx.params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId) || coproId <= 0) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }

  const breakdown = await computeScoreBreakdown(db, coproId);
  return NextResponse.json(breakdown, {
    headers: { "Cache-Control": "private, max-age=60" },
  });
}

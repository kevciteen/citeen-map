/**
 * GET /api/copros/priority
 *
 * Renvoie les top copros triées par score de priorité décroissant.
 * Pratique pour le widget Cockpit "Top priorités" et un éventuel
 * listing filtrable.
 *
 * Params : commune (filtre optionnel), limit (default 20, max 100),
 *          mineSyndic=1 (filtre sur copros de syndics du user — TODO)
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEntityOverrides } from "@/lib/db/ensure-entity-overrides";
import { SCORE_SQL_FRAGMENT } from "@/lib/services/priority-score";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEntityOverrides();

  const sp = req.nextUrl.searchParams;
  const commune = sp.get("commune")?.trim();
  const limit = Math.min(100, Math.max(1, Number(sp.get("limit") ?? 20)));
  const minScore = Number(sp.get("min") ?? 30);

  const where: string[] = [];
  const args: (string | number | null)[] = [];
  if (commune) {
    where.push("LOWER(c.commune) = LOWER(?)");
    args.push(commune);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const items = await db.all<{
    id: number;
    nom_copro: string | null;
    adresse: string | null;
    commune: string | null;
    syndic: string | null;
    nb_lots: number | null;
    classe_finale: string | null;
    score: number;
    has_prospect: number;
    prospect_stage: string | null;
  }>(
    `
    SELECT * FROM (
      SELECT
        c.id,
        c.nom_copro,
        c.adresse,
        c.commune,
        c.syndic,
        c.nb_lots,
        e.classe_finale,
        (${SCORE_SQL_FRAGMENT}) AS score,
        CASE WHEN p.id IS NOT NULL THEN 1 ELSE 0 END AS has_prospect,
        p.stage AS prospect_stage
      FROM copros c
      LEFT JOIN dpe_estimates e ON e.copro_id = c.id
      LEFT JOIN prospects p ON p.copro_id = c.id AND p.stage NOT IN ('won', 'lost')
      ${whereSql}
    ) sub
    WHERE score >= ?
    ORDER BY score DESC, COALESCE(nb_lots, 0) DESC
    LIMIT ?
    `,
    [...args, minScore, limit],
  );

  return NextResponse.json(
    { items, params: { commune, limit, minScore } },
    { headers: { "Cache-Control": "private, max-age=60" } },
  );
}

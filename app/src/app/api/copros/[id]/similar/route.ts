import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

/**
 * Recommandation de copros similaires pour faciliter la prospection
 * en cluster (même syndic = même décideur ; même période + classe = même
 * argumentaire de rénovation).
 *
 * Scoring de similarité (poids) :
 *   - même syndic (non null, non "non connu")   → +5
 *   - même classe DPE finale                     → +4
 *   - même période construction                  → +3
 *   - même CP                                    → +2
 *   - même commune (et CP différent)             → +1
 *
 * On ne retourne que les top‑N (par défaut 6) qui ne sont PAS déjà
 * dans le pipeline, pour orienter vers de nouvelles cibles.
 */
export async function GET(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId))
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });

  const limit = Math.min(
    Math.max(Number(req.nextUrl.searchParams.get("limit") ?? 6), 1),
    20,
  );

  const ref = await db.get<{
    id: number;
    syndic: string | null;
    periode_construction: string | null;
    code_postal: string | null;
    commune: string | null;
    classe_finale: string | null;
  }>(
    `SELECT c.id, c.syndic, c.periode_construction, c.code_postal, c.commune,
            e.classe_finale
     FROM copros c LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.id = ?`,
    [coproId],
  );

  if (!ref) return NextResponse.json({ error: "Not found" }, { status: 404 });

  const syndicValid =
    ref.syndic && ref.syndic.toLowerCase() !== "non connu" ? ref.syndic : null;

  const conditions: string[] = [];
  const params2: InValue[] = [];

  if (syndicValid) {
    conditions.push("c.syndic = ?");
    params2.push(syndicValid);
  }
  if (ref.periode_construction) {
    conditions.push("c.periode_construction = ?");
    params2.push(ref.periode_construction);
  }
  if (ref.classe_finale) {
    conditions.push("e.classe_finale = ?");
    params2.push(ref.classe_finale);
  }
  if (ref.commune) {
    conditions.push("c.commune = ?");
    params2.push(ref.commune);
  }

  // S'il n'y a aucun critère de regroupement → pas de suggestion utile
  if (conditions.length === 0) {
    return NextResponse.json({ items: [] });
  }

  const orClause = `(${conditions.join(" OR ")})`;

  const items = await db.all(
    `SELECT c.id, c.nom_copro, c.adresse, c.code_postal, c.commune,
            c.syndic, c.nb_lots_habitation,
            e.classe_finale, e.conso_moyenne,
            (SELECT p.id FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_id,
            (
              (CASE WHEN c.syndic = ? AND c.syndic IS NOT NULL THEN 5 ELSE 0 END) +
              (CASE WHEN e.classe_finale = ? AND e.classe_finale IS NOT NULL THEN 4 ELSE 0 END) +
              (CASE WHEN c.periode_construction = ? AND c.periode_construction IS NOT NULL THEN 3 ELSE 0 END) +
              (CASE WHEN c.code_postal = ? AND c.code_postal IS NOT NULL THEN 2 ELSE 0 END) +
              (CASE WHEN c.commune = ? AND c.code_postal != ? THEN 1 ELSE 0 END)
            ) AS similarity_score
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.id != ?
       AND ${orClause}
     ORDER BY similarity_score DESC, c.id
     LIMIT ?`,
    [
      syndicValid ?? "",
      ref.classe_finale ?? "",
      ref.periode_construction ?? "",
      ref.code_postal ?? "",
      ref.commune ?? "",
      ref.code_postal ?? "",
      coproId,
      ...params2,
      limit,
    ],
  );

  return NextResponse.json({ items });
}

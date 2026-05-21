/**
 * GET /api/tertiaire/[id]/dvf — mutations DVF récentes au point du bâtiment.
 *
 * Réutilise le service DVF (data.gouv via api.cquest.org). Filtre les
 * mutations dans un rayon (défaut 30m) autour du bâtiment.
 *
 * Données utiles pour le tertiaire :
 *   - Acquéreur **personne morale** visible (SCI, foncière, holding) — utile
 *     pour identifier le propriétaire foncier sans Cerema/DV3F
 *   - Personne physique : anonymisée RGPD 2019
 *   - Date / prix / surface → repérer mutations fraîches (cible chaude)
 *
 * Réponse :
 *   { transactions: DvfTransaction[], stats }
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { fetchDvfAround, sortByDateDesc } from "@/lib/services/dvf";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;
  await ensureTertiary();

  const { id } = await ctx.params;
  const buildingId = Number(id);
  if (!Number.isFinite(buildingId)) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }

  const building = await db.get<{ lat: number | null; lon: number | null; adresse: string | null }>(
    `SELECT lat, lon, adresse FROM tertiary_buildings WHERE id = ?`,
    [buildingId],
  );
  if (!building || building.lat == null || building.lon == null) {
    return NextResponse.json({ error: "Bâtiment introuvable ou sans coordonnées" }, { status: 404 });
  }

  const dist = Math.max(10, Math.min(Number(req.nextUrl.searchParams.get("dist") ?? 40), 200));

  try {
    const all = await fetchDvfAround({ lat: building.lat, lon: building.lon, distMeters: dist });
    const transactions = sortByDateDesc(all);
    const last = transactions[0] ?? null;
    return NextResponse.json({
      transactions,
      stats: {
        count: transactions.length,
        last_sale_date: last?.date_mutation ?? null,
        last_sale_price: last?.valeur_fonciere ?? null,
        last_prix_m2: last?.prix_m2 ?? null,
      },
    });
  } catch (err) {
    return NextResponse.json({ error: (err as Error).message }, { status: 500 });
  }
}

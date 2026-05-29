/**
 * GET /api/dpe/at-address?q=<adresse>
 *
 * Renvoie l'inventaire EXHAUSTIF des DPE ADEME à une adresse, sans filtrer
 * par type de bâtiment (contrairement à /api/maisons/lookup et /api/
 * appartements/lookup qui segmentent et font perdre des résultats).
 *
 * Sections retournées :
 *   - collectifsReels       (DPE immeuble collectif = méthode officielle)
 *   - appartementsIndividuels (DPE indiv réel diag)
 *   - appartementsDerivesImmeuble (DPE app dérivé du DPE immeuble parent)
 *   - maisonsIndividuelles
 *   - autres
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { lookupDpeAtAddress } from "@/lib/services/dpe-at-address";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const q = req.nextUrl.searchParams.get("q")?.trim();
  if (!q || q.length < 5) {
    return NextResponse.json(
      { error: "Paramètre `q` requis (adresse complète)" },
      { status: 400 },
    );
  }
  try {
    const result = await lookupDpeAtAddress(q);
    return NextResponse.json(result, {
      headers: {
        // 5 min cache : ADEME ne change pas dans la seconde
        "Cache-Control":
          "private, max-age=300, stale-while-revalidate=900",
      },
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}

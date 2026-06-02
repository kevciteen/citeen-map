/**
 * GET /api/georisques?codeInsee=93063
 *
 * Renvoie la liste des risques naturels et technologiques pour une commune.
 * Source : Géorisques (gouvernemental, gratuit).
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { fetchGeorisquesByInsee } from "@/lib/services/georisques";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const codeInsee = req.nextUrl.searchParams.get("codeInsee")?.trim();
  if (!codeInsee || codeInsee.length !== 5) {
    return NextResponse.json({ error: "codeInsee requis (5 chiffres)" }, { status: 400 });
  }
  const data = await fetchGeorisquesByInsee(codeInsee);
  return NextResponse.json(data ?? { risques: [], total: 0 }, {
    headers: {
      "Cache-Control": "private, max-age=86400, stale-while-revalidate=604800",
    },
  });
}

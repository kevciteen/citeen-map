/**
 * GET /api/cron/copros-refresh
 *
 * Cron mensuel (le 1er à 4h UTC) : déclenche un check du registre national
 * des copropriétés sur data.gouv.fr. Pour l'instant, on log juste l'état
 * et on prépare la structure pour un re-import futur.
 *
 * Re-import complet à venir : nécessite un job de longue durée (~10-15 min)
 * impossible sur Vercel Hobby (max 60s). À refactorer en :
 *  - Job côté CLI/local
 *  - OU GitHub Action mensuelle qui push les CSV mis à jour
 *  - OU service externe (Inngest, Trigger.dev)
 *
 * Pour l'instant ce cron sert de "tickler" : il indique dans les logs
 * Vercel quand le registre devrait être rafraîchi.
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";

export const runtime = "nodejs";
export const maxDuration = 60;

function assertCronAuth(req: NextRequest): NextResponse | null {
  const expected = process.env.CRON_SECRET;
  if (!expected) return null;
  const auth = req.headers.get("authorization");
  if (auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  return null;
}

export async function GET(req: NextRequest) {
  const unauth = assertCronAuth(req);
  if (unauth) return unauth;

  // État actuel du registre en DB
  const counts = await db.get<{
    total: number;
    avec_coords: number;
    sans_coords: number;
    dernier_import: number | null;
  }>(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 ELSE 0 END) AS avec_coords,
      SUM(CASE WHEN lat IS NULL OR lon IS NULL THEN 1 ELSE 0 END) AS sans_coords,
      MAX(imported_at) AS dernier_import
    FROM copros
  `);

  return NextResponse.json({
    ok: true,
    timestamp: new Date().toISOString(),
    snapshot: counts,
    note: "Re-import du registre data.gouv.fr non automatisé sur Vercel (timeout 60s). À déclencher manuellement via `npm run db:seed` ou GitHub Action.",
  });
}

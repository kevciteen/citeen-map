/**
 * GET /api/tertiaire/quota
 *
 * Renvoie l'état du quota Google Places (jour courant + mois courant) et
 * la taille du cache de contacts. Utile pour surveiller la consommation
 * avant épuisement silencieux du free tier (1000 req/mois).
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { getGoogleQuotaSnapshot } from "@/lib/services/google-quota";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  const quota = await getGoogleQuotaSnapshot();
  const cacheStats = await db.get<{ total: number; valid: number }>(
    `SELECT COUNT(*) AS total,
            SUM(CASE WHEN expires_at > unixepoch() THEN 1 ELSE 0 END) AS valid
     FROM contact_cache`,
  );

  return NextResponse.json({
    google: quota,
    cache: {
      total: cacheStats?.total ?? 0,
      valid: cacheStats?.valid ?? 0,
    },
  });
}

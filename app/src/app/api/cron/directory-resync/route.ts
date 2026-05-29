/**
 * GET /api/cron/directory-resync
 *
 * Cron quotidien Vercel : resynchronise l'annuaire `directory` depuis les
 * tables canoniques. Filet de sécurité au cas où le double-write best-effort
 * de quelques routes aurait raté un sync, ou si on a fait un import direct
 * en DB (drizzle-kit push, script seed, etc.).
 *
 * Auth via CRON_SECRET (header Authorization: Bearer <secret>) si configuré.
 */
import { NextRequest, NextResponse } from "next/server";
import { syncDirectoryAll } from "@/lib/services/directory-sync";

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
  const unauthorized = assertCronAuth(req);
  if (unauthorized) return unauthorized;

  const startedAt = Date.now();
  const result = await syncDirectoryAll();
  const elapsedMs = Date.now() - startedAt;

  return NextResponse.json({
    ok: true,
    timestamp: new Date().toISOString(),
    elapsedMs,
    ...result,
  });
}

import { NextRequest, NextResponse } from "next/server";
import { lookupMaisonByAddress } from "@/lib/services/maison";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const maxDuration = 60;

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
    const result = await lookupMaisonByAddress(q);
    return NextResponse.json(result);
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}

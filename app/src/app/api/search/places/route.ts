import { NextRequest, NextResponse } from "next/server";
import { geocodeAddress } from "@/lib/services/ban";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const q = req.nextUrl.searchParams.get("q")?.trim();
  if (!q || q.length < 3) {
    return NextResponse.json({ items: [] });
  }
  const results = await geocodeAddress(q, { limit: 8, autocomplete: true });
  return NextResponse.json({ items: results });
}

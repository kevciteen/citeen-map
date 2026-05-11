import { NextRequest, NextResponse } from "next/server";
import { geocodeAddress } from "@/lib/services/ban";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get("q")?.trim();
  if (!q || q.length < 3) {
    return NextResponse.json({ items: [] });
  }
  const results = await geocodeAddress(q, { limit: 8, autocomplete: true });
  return NextResponse.json({ items: results });
}

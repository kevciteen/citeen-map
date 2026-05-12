import { NextRequest, NextResponse } from "next/server";
import { lookupMaisonByAddress } from "@/lib/services/maison";

export const runtime = "nodejs";
export const maxDuration = 60;

export async function GET(req: NextRequest) {
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

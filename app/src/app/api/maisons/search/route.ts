import { NextRequest, NextResponse } from "next/server";
import { searchMaisonsByZone } from "@/lib/services/maison";

export const runtime = "nodejs";
export const maxDuration = 60;

function num(v: string | null): number | undefined {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const cp = sp.get("cp")?.trim();
  const commune = sp.get("commune")?.trim();
  if (!cp && !commune) {
    return NextResponse.json(
      { error: "Au moins `cp` ou `commune` requis" },
      { status: 400 },
    );
  }
  try {
    const dpeClassesRaw = sp.get("dpe");
    const result = await searchMaisonsByZone({
      cp,
      commune,
      dpeClasses: dpeClassesRaw
        ? dpeClassesRaw.split(",").map((c) => c.trim().toUpperCase()).filter(Boolean)
        : undefined,
      consoMin: num(sp.get("consoMin")),
      consoMax: num(sp.get("consoMax")),
      yearMin: num(sp.get("yearMin")),
      yearMax: num(sp.get("yearMax")),
      limit: num(sp.get("limit")) ?? 200,
      size: num(sp.get("size")) ?? 2000,
    });
    return NextResponse.json(result);
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}

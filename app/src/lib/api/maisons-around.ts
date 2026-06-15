/**
 * Handler partagé pour GET /api/{maisons,appartements}/around.
 * Recherche géographique (viewport) via ADEME geo_distance.
 */
import { NextRequest, NextResponse } from "next/server";
import { searchMaisonsAround, type BatimentType } from "@/lib/services/maison";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";

function num(v: string | null): number | undefined {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

export async function handleMaisonsAround(req: NextRequest, typeBatiment: BatimentType) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const sp = req.nextUrl.searchParams;
  const lat = num(sp.get("lat"));
  const lon = num(sp.get("lon"));
  const r = num(sp.get("r"));
  if (lat == null || lon == null || r == null) {
    return NextResponse.json({ error: "`lat`, `lon` et `r` requis" }, { status: 400 });
  }
  try {
    const dpeClassesRaw = sp.get("dpe");
    const gesClassesRaw = sp.get("ges");
    const energieRaw = sp.get("energie");
    const result = await searchMaisonsAround({
      typeBatiment,
      lat,
      lon,
      radiusM: r,
      dpeClasses: dpeClassesRaw
        ? dpeClassesRaw.split(",").map((c) => c.trim().toUpperCase()).filter(Boolean)
        : undefined,
      gesClasses: gesClassesRaw
        ? gesClassesRaw.split(",").map((c) => c.trim().toUpperCase()).filter(Boolean)
        : undefined,
      consoMin: num(sp.get("consoMin")),
      consoMax: num(sp.get("consoMax")),
      surfaceMin: num(sp.get("surfaceMin")),
      surfaceMax: num(sp.get("surfaceMax")),
      yearMin: num(sp.get("yearMin")),
      yearMax: num(sp.get("yearMax")),
      energie: energieRaw
        ? energieRaw.split(",").map((c) => c.trim()).filter(Boolean)
        : undefined,
      isolationMursMauvaise: sp.get("isolationMursMauvaise") === "1",
      dpeAncienAnnees: num(sp.get("dpeAncienAnnees")),
      limit: num(sp.get("limit")) ?? 800,
    });
    return NextResponse.json(result);
  } catch (err) {
    return NextResponse.json({ error: (err as Error).message }, { status: 500 });
  }
}

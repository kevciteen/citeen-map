/**
 * GET /api/maisons/find-dpe?lat=X&lon=Y&type=maison|appartement
 *
 * Cherche le numéro DPE le plus probable pour une adresse donnée
 * (utilisé par les prospects créés AVANT la migration numero_dpe :
 * on déduit le numéro à la volée pour pouvoir ouvrir la fiche détaillée).
 *
 * Stratégie : reverse-géocode lat/lon → adresse BAN → cherche dans l'ADEME
 * un DPE individuel matching avec un score raisonnable.
 */
import { NextRequest, NextResponse } from "next/server";
import { fetchAdemeDpeAround, type AdemeRecord } from "@/lib/services/ademe";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const maxDuration = 30;

function isMatchingType(rec: AdemeRecord, type: "maison" | "appartement"): boolean {
  const t = String(rec.type_batiment ?? "").toLowerCase();
  if (type === "maison") return t.includes("maison");
  return t.includes("appartement") || t.includes("appart");
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("search", `user:${guard.id}`);
  if (limited) return limited;

  const sp = req.nextUrl.searchParams;
  const lat = Number(sp.get("lat"));
  const lon = Number(sp.get("lon"));
  const typeRaw = sp.get("type");
  const type: "maison" | "appartement" =
    typeRaw === "appartement" ? "appartement" : "maison";
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return NextResponse.json({ error: "lat/lon requis" }, { status: 400 });
  }

  try {
    // Cherche les DPE dans un rayon de 30m autour des coords
    const raw = await fetchAdemeDpeAround({ lat, lon, r: 30, size: 50 });
    const candidates = raw.filter((r) => isMatchingType(r, type));
    if (candidates.length === 0) {
      return NextResponse.json(
        { error: "Aucun DPE trouvé à proximité" },
        { status: 404 },
      );
    }
    // Garde le plus récent
    candidates.sort((a, b) => {
      const da = Date.parse(String(a.date_etablissement_dpe ?? "")) || 0;
      const db = Date.parse(String(b.date_etablissement_dpe ?? "")) || 0;
      return db - da;
    });
    const best = candidates[0];
    const numero =
      (best.numero_dpe as string) ||
      (best.numero_dpe_immeuble as string) ||
      null;
    if (!numero) {
      return NextResponse.json({ error: "DPE sans numéro" }, { status: 404 });
    }
    return NextResponse.json({ numero_dpe: numero });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}

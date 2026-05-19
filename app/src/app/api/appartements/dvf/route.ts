/**
 * GET /api/appartements/dvf
 *
 * Même endpoint que /api/maisons/dvf mais filtré sur type_local="Appartement"
 * côté DVF (au lieu de "Maison").
 *
 * Paramètres :
 *   - lat, lon (requis)
 *   - housenumber, street (optionnels)
 *   - dist (défaut 30m)
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import {
  fetchDvfAround,
  filterDvfByAddress,
  sortByDateDesc,
} from "@/lib/services/dvf";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const sp = req.nextUrl.searchParams;
  const lat = Number(sp.get("lat"));
  const lon = Number(sp.get("lon"));
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return NextResponse.json(
      { error: "Paramètres `lat` et `lon` requis" },
      { status: 400 },
    );
  }
  const dist = Math.max(5, Math.min(Number(sp.get("dist") ?? 30) || 30, 200));
  const housenumber = sp.get("housenumber");
  const street = sp.get("street");

  try {
    const all = await fetchDvfAround({ lat, lon, distMeters: dist });
    const filtered = filterDvfByAddress(all, {
      housenumber,
      street,
      typeLocal: "appartement",
    });
    const transactions = sortByDateDesc(filtered);

    const last = transactions[0] ?? null;
    return NextResponse.json({
      transactions,
      stats: {
        count: transactions.length,
        last_sale_date: last?.date_mutation ?? null,
        last_sale_price: last?.valeur_fonciere ?? null,
        last_prix_m2: last?.prix_m2 ?? null,
        median_prix_m2: medianPrixM2(transactions),
      },
    });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}

function medianPrixM2(rows: { prix_m2: number | null; type_local: string }[]): number | null {
  const vals = rows
    .filter((r) => /appartement/i.test(r.type_local) && r.prix_m2 != null)
    .map((r) => r.prix_m2 as number)
    .sort((a, b) => a - b);
  if (vals.length === 0) return null;
  const mid = Math.floor(vals.length / 2);
  return vals.length % 2 === 1
    ? vals[mid]
    : Math.round((vals[mid - 1] + vals[mid]) / 2);
}

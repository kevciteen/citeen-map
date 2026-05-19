/**
 * GET /api/maisons/by-dpe/[numero]
 *
 * Retourne le MaisonDpe complet associé à un numéro de DPE ADEME. Utilisé
 * par la page dédiée /maisons/[numero_dpe] et /appartements/[numero_dpe].
 *
 * Renvoie aussi le prospect lié (par adresse / lat-lon) s'il existe, pour
 * permettre à la page d'accéder aux contacts, notes et tags du CRM.
 */
import { NextRequest, NextResponse } from "next/server";
import { fetchAdemeDpeByNumero } from "@/lib/services/ademe";
import { toMaisonDpe } from "@/lib/services/maison";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { db } from "@/lib/db/client";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ numero: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("search", `user:${guard.id}`);
  if (limited) return limited;

  const { numero } = await ctx.params;
  if (!numero || numero.length < 5) {
    return NextResponse.json(
      { error: "Numéro DPE invalide" },
      { status: 400 },
    );
  }

  try {
    const rec = await fetchAdemeDpeByNumero(numero);
    if (!rec) {
      return NextResponse.json(
        { error: "DPE introuvable dans l'ADEME" },
        { status: 404 },
      );
    }
    const maison = toMaisonDpe(rec);

    // Cherche un prospect existant pour cette adresse (lat/lon proche < 30 m)
    let prospect:
      | {
          id: number;
          stage: string;
          custom_label: string | null;
          tags: string | null;
        }
      | undefined;
    if (maison.lat != null && maison.lon != null) {
      // ~30m en lat ≈ 0.00027, en lon ≈ 0.00040 à 48°N
      const dLat = 0.0003;
      const dLon = 0.0004;
      prospect = await db.get<typeof prospect & object>(
        `SELECT id, stage, custom_label, tags
         FROM prospects
         WHERE custom_lat BETWEEN ? AND ?
           AND custom_lon BETWEEN ? AND ?
         ORDER BY id DESC LIMIT 1`,
        [
          maison.lat - dLat,
          maison.lat + dLat,
          maison.lon - dLon,
          maison.lon + dLon,
        ],
      );
    }

    return NextResponse.json({ maison, prospect: prospect ?? null });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message },
      { status: 500 },
    );
  }
}
